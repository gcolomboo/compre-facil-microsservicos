const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const app = express();
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const EXCHANGE = 'comprefacil.eventos';
const QUEUE = 'comprefacil.notificacoes';
let channel;
async function initDb() {
  await pool.query(`CREATE TABLE IF NOT EXISTS notificacoes (
    id BIGSERIAL PRIMARY KEY, evento_id UUID NOT NULL UNIQUE,
    pagamento_id UUID NOT NULL, tipo VARCHAR(60) NOT NULL,
    destinatario VARCHAR(254) NOT NULL, mensagem TEXT NOT NULL,
    criada_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`);
}
function mensagem(evento) {
  if (evento.tipo === 'pagamento.recebido') return `Olá, ${evento.cliente}! Recebemos sua solicitação de pagamento de R$ ${evento.valor.toFixed(2)}. Status: PENDENTE.`;
  if (evento.tipo === 'pagamento.confirmado') return `Olá, ${evento.cliente}! Seu pagamento de R$ ${evento.valor.toFixed(2)} foi confirmado. Status: SUCESSO.`;
  throw new Error('Tipo desconhecido');
}
async function consume(msg) {
  try {
    const evento = JSON.parse(msg.content.toString());
    const texto = mensagem(evento);
    const r = await pool.query(`INSERT INTO notificacoes (evento_id, pagamento_id, tipo, destinatario, mensagem)
      VALUES ($1,$2,$3,$4,$5) ON CONFLICT (evento_id) DO NOTHING RETURNING id`,
      [evento.eventoId, evento.pagamentoId, evento.tipo, evento.email, texto]);
    if (r.rowCount) console.log(`[NOTIFICAÇÃO SIMULADA] Para: ${evento.email} | ${texto}`);
    channel.ack(msg);
  } catch (err) {
    console.error('Falha na notificação:', err);
    // Mensagens inválidas são descartadas para evitar loop infinito.
    if (err instanceof SyntaxError || err.message === 'Tipo desconhecido') channel.nack(msg, false, false);
    else channel.nack(msg, false, true);
  }
}
app.get('/health', async (_req, res) => {
  try { await pool.query('SELECT 1'); res.json({ status: 'ok', servico: 'notificacao', rabbitmq: !!channel }); }
  catch { res.status(503).json({ status: 'indisponivel' }); }
});
app.get('/notificacoes', async (req, res) => {
  try {
    const id = req.query.pagamentoId;
    if (id && !/^[0-9a-f]{8}-[0-9a-f-]{27,}$/i.test(id)) return res.status(400).json({ erro: 'pagamentoId inválido.' });
    const r = id
      ? await pool.query('SELECT * FROM notificacoes WHERE pagamento_id=$1 ORDER BY id', [id])
      : await pool.query('SELECT * FROM notificacoes ORDER BY id DESC LIMIT 100');
    res.json(r.rows);
  } catch { res.status(500).json({ erro: 'Falha ao listar notificações.' }); }
});
async function main() {
  await initDb();
  const connection = await amqp.connect(process.env.AMQP_URL);
  connection.on('error', err => console.error('RabbitMQ:', err.message));
  connection.on('close', () => { console.error('RabbitMQ desconectado; reiniciando.'); process.exit(1); });
  channel = await connection.createChannel();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  await channel.assertQueue(QUEUE, { durable: true });
  await channel.bindQueue(QUEUE, EXCHANGE, 'pagamento.*');
  await channel.prefetch(5);
  await channel.consume(QUEUE, consume, { noAck: false });
  app.listen(Number(process.env.PORT || 3001), '0.0.0.0', () => console.log('Notificação iniciada na porta', process.env.PORT || 3001));
}
main().catch(err => { console.error('Inicialização falhou:', err); process.exit(1); });
