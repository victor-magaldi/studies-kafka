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
const consumer = kafka.consumer({ groupId: "my-group" });

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
          key: key ?? null, // opcional
          value: value.toString(),
          headers: headers ?? {}, // headers no formato { chave: valor }
        },
      ],
    });
    return res.status(201).json({ message: "Mensagem enviada com sucesso!" });
  } catch (error) {
    console.error("Erro ao enviar mensagem:", error);
    return res.status(500).json({ error: "Failed to send message" });
  }
});

// --- Consumer ---
let messages = [];
const startConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "meu-topico", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const msg = {
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString() ?? null,
        value: message.value.toString(),
        headers: Object.fromEntries(
          Object.entries(message.headers ?? {}).map(([k, v]) => [k, v.toString()])
        ),
      };
      console.log("Nova mensagem:", msg);
      messages.push(msg);
    },
  });
};

app.get("/messages", (req, res) => {
  return res.json(messages);
});

app.listen(3000, async () => {
  console.log("API rodando em http://localhost:3000");
  await startConsumer();
});
