require("./tracing");

const express = require("express");
const { trace, SpanStatusCode } = require("@opentelemetry/api");

const app = express();
app.use(express.json());

const tracer = trace.getTracer("dummy-express-server");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function randomBetween(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function pickOne(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function errorSpan(span, message) {
  const err = new Error(message);
  span.recordException(err);
  span.setStatus({ code: SpanStatusCode.ERROR, message });
  span.setAttribute("error", true);
}

app.get("/orders", async (req, res) => {
  const errorSlot = pickOne(["fetch-orders-from-db"]);

  await tracer.startActiveSpan("fetch-orders-from-db", async (span) => {
    await sleep(randomBetween(20, 80));
    span.setAttribute("db.system", "postgresql");
    span.setAttribute("db.statement", "SELECT * FROM orders LIMIT 10");
    if (errorSlot === "fetch-orders-from-db") errorSpan(span, "Database connection timeout");
    span.end();
  });

  res.json({ orders: [{ id: 1 }, { id: 2 }, { id: 3 }] });
});

app.get("/orders/:id", async (req, res) => {
  const orderId = req.params.id;
  const errorSlot = pickOne(["fetch-order", "fetch-order-items"]);

  const order = await tracer.startActiveSpan("fetch-order", async (span) => {
    span.setAttribute("order.id", orderId);
    await sleep(randomBetween(10, 50));

    await tracer.startActiveSpan("fetch-order-items", async (childSpan) => {
      childSpan.setAttribute("db.system", "postgresql");
      childSpan.setAttribute("db.statement", `SELECT * FROM order_items WHERE order_id = ${orderId}`);
      await sleep(randomBetween(5, 30));
      if (errorSlot === "fetch-order-items") errorSpan(childSpan, "Order not found");
      childSpan.end();
    });

    if (errorSlot === "fetch-order") errorSpan(span, "Failed to fetch order");
    span.end();
    return { id: orderId, items: [{ sku: "ABC-1" }, { sku: "XYZ-2" }] };
  });

  res.json(order);
});

app.post("/orders", async (req, res) => {
  const errorSlot = pickOne(["validate-inventory", "charge-payment", "save-order-to-db", "create-order"]);

  const result = await tracer.startActiveSpan("create-order", async (rootSpan) => {
    rootSpan.setAttribute("order.source", "api");

    await tracer.startActiveSpan("validate-inventory", async (span) => {
      await sleep(randomBetween(15, 40));
      span.setAttribute("inventory.checked", true);
      if (errorSlot === "validate-inventory") errorSpan(span, "Item out of stock");
      span.end();
    });

    await tracer.startActiveSpan("charge-payment", async (span) => {
      await sleep(randomBetween(80, 200));
      span.setAttribute("payment.provider", "stripe");
      span.setAttribute("payment.amount", randomBetween(10, 500));
      if (errorSlot === "charge-payment") errorSpan(span, "Card declined");
      span.end();
    });

    await tracer.startActiveSpan("save-order-to-db", async (span) => {
      span.setAttribute("db.system", "postgresql");
      span.setAttribute("db.statement", "INSERT INTO orders ...");
      await sleep(randomBetween(10, 30));
      if (errorSlot === "save-order-to-db") errorSpan(span, "Deadlock detected");
      span.end();
    });

    if (errorSlot === "create-order") errorSpan(rootSpan, "Order creation failed");
    rootSpan.end();
    return { id: randomBetween(100, 999), status: "created" };
  });

  res.status(201).json(result);
});

app.get("/users/:id", async (req, res) => {
  const userId = req.params.id;
  const errorSlot = pickOne(["cache-lookup", "fetch-user"]);

  await tracer.startActiveSpan("fetch-user", async (span) => {
    span.setAttribute("user.id", userId);

    await tracer.startActiveSpan("cache-lookup", async (cacheSpan) => {
      cacheSpan.setAttribute("cache.backend", "redis");
      await sleep(randomBetween(1, 10));
      cacheSpan.setAttribute("cache.hit", Math.random() > 0.5);

      await tracer.startActiveSpan("redis-get", async (redisSpan) => {
        redisSpan.setAttribute("redis.command", "GET");
        redisSpan.setAttribute("redis.key", `user:${userId}`);
        await sleep(randomBetween(1, 5));
        redisSpan.end();
      });

      if (errorSlot === "cache-lookup") errorSpan(cacheSpan, "Redis connection refused");
      cacheSpan.end();
    });

    await sleep(randomBetween(5, 25));
    if (errorSlot === "fetch-user") errorSpan(span, "User not found");
    span.end();
  });

  res.json({ id: userId, name: "Alice", email: "alice@example.com" });
});

app.get("/health", (_req, res) => res.json({ status: "ok" }));

const PORT = 8080;
app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
  console.log(`Sending traces to Nabatshy at http://localhost:4318`);
});
