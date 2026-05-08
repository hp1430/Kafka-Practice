const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "manual-consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "manual-group",
});

async function run() {
  await consumer.connect();

  await consumer.subscribe({
    topic: "practice-topic",
    fromBeginning: true,
  });

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      // Ignore partition 2
      if (partition !== 0 && partition !== 1) {
        return;
      }

      console.log({
        partition,
        value: message.value.toString(),
      });
    },
  });
}

run().catch(console.error);
