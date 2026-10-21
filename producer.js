import { Kafka } from 'kafkajs';
import crypto from 'crypto';

const kafka = new Kafka({
  clientId: 'fraud-producer',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

const CLIENTS = ['cliente-1', 'cliente-2', 'cliente-3', 'cliente-4'];
const CITIES = ['São Paulo', 'Rio de Janeiro', 'Curitiba', 'Salvador', 'Fortaleza'];

function getRandomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function generateTransaction() {
  // Sorteia valores normais e valores altos (>= 10.000)
  const isHighValue = Math.random() < 0.25;
  const amount = isHighValue 
    ? Math.floor(Math.random() * 5000) + 10000 
    : Math.floor(Math.random() * 800) + 10;

  return {
    id: crypto.randomUUID(),
    clientId: getRandomItem(CLIENTS),
    amount: parseFloat(amount.toFixed(2)),
    city: getRandomItem(CITIES),
    timestamp: Date.now()
  };
}

async function run() {
  await producer.connect();
  console.log('[PRODUCER] Conectado ao Kafka com sucesso.');

  setInterval(async () => {
    const transaction = generateTransaction();

    await producer.send({
      topic: 'transactions',
      messages: [
        {
          key: transaction.clientId,
          value: JSON.stringify(transaction)
        }
      ]
    });

    console.log(`[PRODUCER] Enviado: [${transaction.clientId}] R$ ${transaction.amount} em ${transaction.city}`);
  }, 3000);
}

run().catch(console.error);
