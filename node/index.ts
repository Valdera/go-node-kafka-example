import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:29092"]
});

enum TOPICS {
  PRIME_TOPIC = "PRIME_TOPIC",
  PRIME_TOPIC_UPDATE = "PRIME_TOPIC_UPDATE"
}

async function main() {
  const consumer = kafka.consumer({ groupId: "node" });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.PRIME_TOPIC });
  const dispatcher = await dispatch();

  await consumer.run({
    eachMessage: async ({ message, partition, topic }) => {
      const { key, value } = message;
      console.log({
        partition,
        topic,
        key: key.toString(),
        value: value!.toString()
      });

      dispatcher(key.toString(), value!.toString());
    }
  });
}

const dispatch = async () => {
  const producer = kafka.producer();
  await producer.connect();

  return async (key: string, value: string) => {
    const randomWaitTime = Math.floor(Math.random() * 10000);
    await new Promise((resolve) => setTimeout(resolve, randomWaitTime));

    await producer.send({
      topic: TOPICS.PRIME_TOPIC_UPDATE,
      messages: [
        {
          key: Math.random().toString(),
          value: `Success from key ${key} and value was ${value}`
        }
      ]
    });
  };
};

main();
