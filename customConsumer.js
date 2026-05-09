const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "partition-consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "partition-group",
});

async function run() {
  await consumer.connect();

  await consumer.subscribe({
    topic: "practice-topic",
    fromBeginning: true,
  });

  console.log("Listening...\n");

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const key = message.key ? message.key.toString() : "NULL";

      const value = message.value ? message.value.toString() : "NULL";

      console.log(`Partition: ${partition} | Key: ${key} | Value: ${value}`);
    },
  });
}

run().catch(console.error);
