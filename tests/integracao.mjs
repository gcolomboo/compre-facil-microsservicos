import assert from 'node:assert/strict';
const PAG = process.env.PAG_URL || 'http://localhost:3000';
const NOT = process.env.NOT_URL || 'http://localhost:3001';
const wait = ms => new Promise(r => setTimeout(r, ms));
async function json(url, options) {
  const response = await fetch(url, options);
  const body = await response.json();
  return { status: response.status, body };
}
async function main() {
  console.log('Verificando serviços...');
  for (const url of [`${PAG}/health`, `${NOT}/health`]) {
    const r = await json(url); assert.equal(r.status, 200); assert.equal(r.body.rabbitmq, true);
  }
  const invalid = await json(`${PAG}/pagamentos`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ cliente: 'Teste', email: 'errado', valor: -1 }) });
  assert.equal(invalid.status, 400);
  const created = await json(`${PAG}/pagamentos`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ cliente: 'Cliente Teste', email: 'teste@example.com', valor: 49.9 }) });
  assert.equal(created.status, 202);
  assert.equal(created.body.status, 'PENDENTE');
  const id = created.body.id;
  console.log('Pagamento criado:', id);
  let payment, notes;
  for (let i = 0; i < 30; i++) {
    await wait(1000);
    payment = (await json(`${PAG}/pagamentos/${id}`)).body;
    notes = (await json(`${NOT}/notificacoes?pagamentoId=${id}`)).body;
    if (payment.status === 'SUCESSO' && notes.length === 2) break;
  }
  assert.equal(payment.status, 'SUCESSO', 'Pagamento deve ser confirmado');
  assert.deepEqual(notes.map(n => n.tipo).sort(), ['pagamento.confirmado', 'pagamento.recebido']);
  console.log('SUCESSO: pagamento confirmado e duas notificações persistidas.');
  console.table(notes.map(n => ({ tipo: n.tipo, mensagem: n.mensagem })));
}
main().catch(err => { console.error('TESTE FALHOU:', err); process.exitCode = 1; });
