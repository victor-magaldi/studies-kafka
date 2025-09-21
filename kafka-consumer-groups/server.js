import express from "express";
import { Kafka } from "kafkajs";

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: "my-app",
  brokers: ["localhost:9092"],
});

const admin = kafka.admin();
const producer = kafka.producer();

// Consumer Groups Configurations
const consumerGroup1 = kafka.consumer({ groupId: "my-group-1" });
const consumerGroup2 = kafka.consumer({ groupId: "my-group-2" });


let messagesGroup1 = [];
let messagesGroup2 = [];

// create topic
app.post("/create-topic", async (req, res) => {
  const { topic, numPartitions = 1, replicationFactor = 1 } = req.body;
  if (!topic) return res.status(400).json({ error: "Topic name is required" });

  try {
    await admin.connect();
    await admin.createTopics({
      topics: [
        {
          topic,
          numPartitions,
          replicationFactor,
        },
      ],
    });
    await admin.disconnect();
    return res
      .status(201)
      .json({ message: `Topic '${topic}' created!`, numPartitions, replicationFactor });
  } catch (error) {
    console.error("Erro ao criar tópico:", error);
    return res.status(500).json({ error: "Failed to create topic" });
  }
});

// Producer messages
app.post("/produce", async (req, res) => {
  const { topic, key, value, headers } = req.body;

  if (!topic || !value) {
    return res.status(400).json({ error: "Topic and value are required" });
  }

  try {
    await producer.connect();
    await producer.send({
      topic,
      messages: [
        {
          key: key ?? null,
          value: value.toString(),
          headers: headers ?? {},
        },
      ],
    });
    return res.status(201).json({ message: "Mensagem enviada com sucesso!" });
  } catch (error) {
    console.error("Erro ao enviar mensagem:", error);
    return res.status(500).json({ error: "Failed to send message" });
  }
});

// Consumer
const startConsumers = async () => {
  // Consumer 1
  await consumerGroup1.connect();
  await consumerGroup1.subscribe({ topic: "meu-topico", fromBeginning: true });

  await consumerGroup1.run({
    eachMessage: async ({ topic, partition, message }) => {
      const msg = {
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString() ?? null,
        value: message?.value?.toString() ?? null,
      };
      console.log("[GROUP-1] Nova mensagem:", msg);
      messagesGroup1.push(msg);
    },
  });

  // Consumer 2
  await consumerGroup2.connect();
  await consumerGroup2.subscribe({ topic: "meu-topico", fromBeginning: true });

  await consumerGroup2.run({
    eachMessage: async ({ topic, partition, message }) => {
      const msg = {
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString() ?? null,
        value: message?.value?.toString() ?? null,
      };
      console.log("[GROUP-2] Nova mensagem:", msg);
      messagesGroup2.push(msg);
    },
  });
};

app.get("/messages/group1", (req, res) => res.json(messagesGroup1));
app.get("/messages/group2", (req, res) => res.json(messagesGroup2));

app.listen(3000, async () => {
  console.log("API rodando em http://localhost:3000");
  await startConsumers();
});
