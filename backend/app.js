const path = require('path');
const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
const PORT = 3000;

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  res.render('index');
});

app.get('/events', (req, res) => {
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Content-Type', 'text/event-stream');

  res.write(`data: ${"Connected!"}\n\n`);

  const kafka = new Kafka({
    clientId: 'temperature-app',
    brokers: ['localhost:9092']
  });

  const consumer = kafka.consumer({ groupId: 'temperature-group-id' + Math.random() });

  const consume = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'average_temperature_results', fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const data = `${message.value}`;
        console.log("data:", data)
        res.write(`data: ${data}\n\n`);
      },
    });
  };

  consume().catch(e => console.error(`[kafka-consumer] ${e.message}`, e));

  req.on('close', async () => {
    await consumer.disconnect();
    res.end();
  });
});

// Start the server
app.listen(PORT, () => {
  console.log(`Server started on http://localhost:${PORT}`);
});
