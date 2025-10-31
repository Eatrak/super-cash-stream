const { Kafka } = require("kafkajs");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const kafka = new Kafka({
  clientId: "cash-stream-app",
  brokers: ["broker:9092"], // nome servizio docker
});

const consumer = kafka.consumer({ groupId: "stats-consumer" });
const producer = kafka.producer();

const totalRevenuePerShop = new Map();
const totalQuantityPerProduct = new Map();

async function publishStats() {
  const stats = {
    totalRevenuePerShop: Object.fromEntries(totalRevenuePerShop),
    totalQuantityPerProduct: Object.fromEntries(totalQuantityPerProduct),
    windowStart: new Date().toISOString(),
  };

  await producer.send({
    topic: "hourly-cash-stats",
    messages: [{ value: JSON.stringify(stats) }],
  });

  console.log("Statistiche inviate su hourly-cash-stats");
}

(async () => {
  console.log("Attendo che Kafka sia pronto...");
  await sleep(10000); // 10 secondi, puoi aumentare se necessario
  console.log("Avvio consumer...");

  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "super-cash-stream", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      const { shop_id, product_id, quantity, price } = event;

      // Revenue per shop
      totalRevenuePerShop.set(
        shop_id,
        (totalRevenuePerShop.get(shop_id) || 0) + price * quantity
      );

      // Quantity per product
      totalQuantityPerProduct.set(
        product_id,
        (totalQuantityPerProduct.get(product_id) || 0) + quantity
      );
    },
  });

  // Invio statistiche ogni minuto
  setInterval(publishStats, 60_000);

  // Reset finestra ogni 1 ora
  setInterval(() => {
    totalRevenuePerShop.clear();
    totalQuantityPerProduct.clear();
    console.log("=== Nuova finestra di 1 ora ===\n");
  }, 60 * 60 * 1000);
})();
