const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "custom-partitioner-app",
  brokers: ["localhost:9092"],
});

// Custom partitioner
const customPartitioner = () => {
  return ({ message }) => {
    const key = message.key.toString();

    if (key.startsWith("A")) {
      return 0;
    }

    if (key.startsWith("B")) {
      return 1;
    }

    return 2;
  };
};

const producer = kafka.producer({
  createPartitioner: customPartitioner,
});

async function run() {
  await producer.connect();

  const messages = [
    { key: "Apple", value: "Message for partition 0" },
    { key: "Amazon", value: "Another message for partition 0" },

    { key: "BMW", value: "Message for partition 1" },
    { key: "Bank", value: "Another message for partition 1" },

    { key: "Cat", value: "Message for partition 2" },
    { key: "Dog", value: "Another message for partition 2" },
  ];

  await producer.send({
    topic: "practice-topic",
    messages,
  });

  console.log("Messages sent successfully");

  await producer.disconnect();
}

run().catch(console.error);
