const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "dead-letter-consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "dead-letter-group",
});

async function run() {
  await consumer.connect();

  await consumer.subscribe({
    topic: "dead-letter-topic",
    fromBeginning: true,
  });

  console.log("Listening to dead-letter-topic...\n");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const data = JSON.parse(message.value.toString());

        console.log("====================================");
        console.log("Dead Letter Message Received");
        console.log("====================================");

        // Kafka metadata
        console.log("Kafka Metadata:");
        console.log(`Topic: ${topic}`);
        console.log(`DLQ Partition: ${partition}`);
        console.log(`Offset: ${message.offset}`);
        console.log(
          `Kafka Timestamp: ${new Date(
            Number(message.timestamp),
          ).toLocaleString()}`,
        );

        console.log("");

        // Original failed message metadata
        console.log("Original Message Metadata:");
        console.log(`Original Partition: ${data.partition}`);
        console.log(`Failure Timestamp: ${data.timestamp}`);
        console.log(`Error: ${data.error}`);

        console.log("");

        // Actual failed payload
        console.log("Original Message:");
        console.log(data.originalMessage);

        console.log("====================================\n");
      } catch (err) {
        console.error("Failed to parse DLQ message:", err.message);
      }
    },
  });
}

run().catch(console.error);
