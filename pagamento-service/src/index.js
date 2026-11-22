const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const { randomUUID } = require('node:crypto');
const { validarPagamento } = require('./validation');
const app = express();
app.use(express.json({ limit: '16kb' }));
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const EXCHANGE = 'comprefacil.eventos';
const JOBS = 'comprefacil.processar_pagamento';
const delay = Number(process.env.PROCESS_DELAY_MS ?? 1500);
let channel, connection;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function initDb() {
  await pool.query(`CREATE TABLE IF NOT EXISTS pagamentos (
    id UUID PRIMARY KEY, cliente VARCHAR(120) NOT NULL, email VARCHAR(254) NOT NULL,
    valor_centavos INTEGER NOT NULL CHECK (valor_centavos > 0),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDENTE',
    criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(), atualizado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS pagamento_outbox (
    id BIGSERIAL PRIMARY KEY, evento_id UUID NOT NULL UNIQUE, tipo VARCHAR(60) NOT NULL,
    payload JSONB NOT NULL, enviado_em TIMESTAMPTZ, criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`);
}
function rowDto(row) {
  return { id: row.id, cliente: row.cliente, email: row.email,
    valor: row.valor_centavos / 100, status: row.status,
    criadoEm: row.criado_em, atualizadoEm: row.atualizado_em };
}
function event(type, pagamento) {
  return { eventoId: randomUUID(), tipo: type, pagamentoId: pagamento.id,
    cliente: pagamento.cliente, email: pagamento.email,
    valor: pagamento.valor_centavos / 100, data: new Date().toISOString() };
}
async function saveEvent(client, payload) {
  await client.query('INSERT INTO pagamento_outbox (evento_id, tipo, payload) VALUES ($1, $2, $3)',
    [payload.eventoId, payload.tipo, JSON.stringify(payload)]);
}
async function publishOutbox() {
  if (!channel) return;
  const result = await pool.query('SELECT id, tipo, payload FROM pagamento_outbox WHERE enviado_em IS NULL ORDER BY id LIMIT 50');
  for (const row of result.rows) {
    await new Promise((resolve, reject) => {
      channel.publish(EXCHANGE, row.tipo, Buffer.from(JSON.stringify(row.payload)),
        { persistent: true, contentType: 'application/json', messageId: row.payload.eventoId },
        err => err ? reject(err) : resolve());
    });
    await pool.query('UPDATE pagamento_outbox SET enviado_em = NOW() WHERE id = $1', [row.id]);
  }
}
async function processJob(msg) {
  let job;
  try { job = JSON.parse(msg.content.toString()); }
  catch { console.error('Mensagem de processamento inválida'); channel.ack(msg); return; }
  try {
    // Aguarda a publicação do evento inicial antes da confirmação.
    const initial = await pool.query(`SELECT enviado_em FROM pagamento_outbox
      WHERE payload->>'pagamentoId' = $1 AND tipo = 'pagamento.recebido' LIMIT 1`, [job.id]);
    if (!initial.rows[0]?.enviado_em) { channel.nack(msg, false, true); await sleep(500); return; }
    await sleep(Math.max(0, delay));
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const updated = await client.query(`UPDATE pagamentos SET status = 'SUCESSO', atualizado_em = NOW()
        WHERE id = $1 AND status = 'PENDENTE' RETURNING *`, [job.id]);
      if (updated.rowCount) await saveEvent(client, event('pagamento.confirmado', updated.rows[0]));
      await client.query('COMMIT');
    } catch (err) { await client.query('ROLLBACK'); throw err; }
    finally { client.release(); }
    channel.ack(msg);
    await publishOutbox();
  } catch (err) { console.error('Erro ao processar pagamento:', err); channel.nack(msg, false, true); await sleep(1000); }
}
app.get('/health', async (_req, res) => {
  try { await pool.query('SELECT 1'); res.json({ status: 'ok', servico: 'pagamento', rabbitmq: !!channel }); }
  catch { res.status(503).json({ status: 'indisponivel' }); }
});
app.post('/pagamentos', async (req, res) => {
  const invalid = validarPagamento(req.body);
  if (invalid) return res.status(400).json({ erro: invalid });
  if (!channel) return res.status(503).json({ erro: 'Mensageria indisponível.' });
  const id = randomUUID();
  const cents = Math.round(req.body.valor * 100);
  const client = await pool.connect().catch(() => null);
  if (!client) return res.status(503).json({ erro: 'Banco indisponível.' });
  try {
    await client.query('BEGIN');
    const result = await client.query(`INSERT INTO pagamentos (id, cliente, email, valor_centavos)
      VALUES ($1,$2,$3,$4) RETURNING *`, [id, req.body.cliente.trim(), req.body.email, cents]);
    await saveEvent(client, event('pagamento.recebido', result.rows[0]));
    await client.query('COMMIT');
    // A resposta é imediata; o worker confirma o pagamento de modo assíncrono.
    channel.sendToQueue(JOBS, Buffer.from(JSON.stringify({ id })), { persistent: true });
    res.status(202).json(rowDto(result.rows[0]));
    publishOutbox().catch(err => console.error('Outbox:', err));
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    console.error(err); res.status(500).json({ erro: 'Falha ao criar pagamento.' });
  } finally { client.release(); }
});
app.get('/pagamentos', async (_req, res) => {
  try { const r = await pool.query('SELECT * FROM pagamentos ORDER BY criado_em DESC LIMIT 100'); res.json(r.rows.map(rowDto)); }
  catch { res.status(500).json({ erro: 'Falha ao listar pagamentos.' }); }
});
app.get('/pagamentos/:id', async (req, res) => {
  if (!/^[0-9a-f]{8}-[0-9a-f-]{27,}$/i.test(req.params.id)) return res.status(400).json({ erro: 'ID inválido.' });
  try { const r = await pool.query('SELECT * FROM pagamentos WHERE id=$1', [req.params.id]);
    if (!r.rowCount) return res.status(404).json({ erro: 'Pagamento não encontrado.' });
    res.json(rowDto(r.rows[0]));
  } catch { res.status(500).json({ erro: 'Falha na consulta.' }); }
});
async function connectRabbit() {
  connection = await amqp.connect(process.env.AMQP_URL);
  connection.on('error', err => console.error('RabbitMQ:', err.message));
  connection.on('close', () => { console.error('Conexão RabbitMQ fechada; reiniciando processo.'); process.exit(1); });
  channel = await connection.createConfirmChannel();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  await channel.assertQueue(JOBS, { durable: true });
  await channel.assertQueue('comprefacil.notificacoes', { durable: true });
  await channel.bindQueue('comprefacil.notificacoes', EXCHANGE, 'pagamento.*');
  await channel.prefetch(1);
  await channel.consume(JOBS, processJob, { noAck: false });
}
async function main() {
  await initDb(); await connectRabbit();
  app.listen(Number(process.env.PORT || 3000), '0.0.0.0', () => console.log('Pagamento iniciado na porta', process.env.PORT || 3000));
  const timer = setInterval(() => publishOutbox().catch(err => console.error('Outbox:', err.message)), 1000);
  process.on('SIGTERM', () => { clearInterval(timer); process.exit(0); });
}
main().catch(err => { console.error('Inicialização falhou:', err); process.exit(1); });
