const { Kafka, Partitioners } = require("kafkajs");

const kafka = new Kafka({
  clientId: "practice-consumer",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "practice-group",
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

async function run() {
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({
    topic: "practice-topic",
    fromBeginning: true,
  });

  console.log("Listening to practice-topic...");

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const value = message.value.toString();

      try {
        console.log(`Processing message from partition ${partition}: ${value}`);

        // Simulate failure
        if (value.includes("error")) {
          throw new Error("Processing failed");
        }

        console.log(`Successfully processed: ${value}`);
      } catch (err) {
        console.log(`Sending failed message to dead-letter-topic`);

        await producer.send({
          topic: "dead-letter-topic",
          messages: [
            {
              value: JSON.stringify({
                originalMessage: value,
                error: err.message,
                partition,
                timestamp: new Date().toISOString(),
              }),
            },
          ],
        });
      }
    },
  });
}

run().catch(console.error);
