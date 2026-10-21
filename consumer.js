import { Kafka } from 'kafkajs';
import { initDb } from './database.js';

const kafka = new Kafka({
  clientId: 'fraud-consumer',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'fraud-detector-group' });

async function detectFraud(db, currentTx) {
  const flags = [];

  // Regra 1: ALTO_VALOR (amount >= 10000)
  if (currentTx.amount >= 10000) {
    flags.push('ALTO_VALOR');
  }

  // Regra 2: TEMPO_60s (4 transações em < 60s)
  // Verifica transações do mesmo cliente nos últimos 60 segundos (excluindo a atual)
  const window60s = currentTx.timestamp - 60 * 1000;
  const countRecent = await db.get(
    `SELECT COUNT(*) as total FROM transactions 
     WHERE clientId = ? AND timestamp >= ?`,
    [currentTx.clientId, window60s]
  );
  
  // Se já existiam 3 ou mais no histórico recente, a transação atual é a 4ª ou mais
  if (countRecent.total >= 3) {
    flags.push('TEMPO_60s');
  }

  // Regra 3: GEO_10m (2 transações para cidades diferentes no período de 10 minutos)
  const window10m = currentTx.timestamp - 10 * 60 * 1000;
  const recentDiffCity = await db.get(
    `SELECT city FROM transactions 
     WHERE clientId = ? AND timestamp >= ? AND city != ? 
     LIMIT 1`,
    [currentTx.clientId, window10m, currentTx.city]
  );

  if (recentDiffCity) {
    flags.push('GEO_10m');
  }

  return flags;
}

async function run() {
  const db = await initDb();
  await consumer.connect();
  await consumer.subscribe({ topic: 'transactions', fromBeginning: false });

  console.log('[CONSUMER] Conectado e aguardando transações...');

  await consumer.run({
    eachMessage: async ({ message }) => {
      const transaction = JSON.parse(message.value.toString());

      // 1. Avalia regras contra o histórico gravado
      const detectedFrauds = await detectFraud(db, transaction);
      const isFraud = detectedFrauds.length > 0;
      const fraudType = isFraud ? detectedFrauds.join(' | ') : null;

      // 2. Persiste a transação atual
      await db.run(
        `INSERT INTO transactions (id, clientId, amount, city, timestamp, fraudType, isFraud) 
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [
          transaction.id,
          transaction.clientId,
          transaction.amount,
          transaction.city,
          transaction.timestamp,
          fraudType,
          isFraud ? 1 : 0
        ]
      );

      // 3. Exibe alerta no console
      const hora = new Date(transaction.timestamp).toLocaleTimeString();
      if (isFraud) {
        console.warn(`\x1b[31m[ALERTA DE FRAUDE] [${fraudType}]\x1b[0m`);
        console.warn(` -> Cliente: ${transaction.clientId} | Valor: R$ ${transaction.amount} | Cidade: ${transaction.city} | Hora: ${hora}\n`);
      } else {
        console.log(`[TRANSAÇÃO OK] Cliente: ${transaction.clientId} | Valor: R$ ${transaction.amount} | Cidade: ${transaction.city} | Hora: ${hora}\n`);
      }
    }
  });
}

run().catch(console.error);
