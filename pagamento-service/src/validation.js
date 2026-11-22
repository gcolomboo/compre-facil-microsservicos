function validarPagamento(body) {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return 'Informe um objeto JSON.';
  if (typeof body.cliente !== 'string' || !body.cliente.trim() || body.cliente.length > 120) return 'cliente deve ter entre 1 e 120 caracteres.';
  if (typeof body.email !== 'string' || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(body.email) || body.email.length > 254) return 'email inválido.';
  if (typeof body.valor !== 'number' || !Number.isFinite(body.valor) || body.valor <= 0 || Math.abs(Math.round(body.valor * 100) - body.valor * 100) > 1e-7 || body.valor > 1000000) return 'valor deve ser positivo, com até 2 casas decimais e até 1000000.';
  return null;
}
module.exports = { validarPagamento };
