require("./tracing");

const express = require("express");
const { trace, SpanStatusCode } = require("@opentelemetry/api");
const { logs, SeverityNumber } = require("@opentelemetry/api-logs");

const app = express();
app.use(express.json());

const tracer = trace.getTracer("dummy-express-server");
const logger = logs.getLogger("dummy-express-server");

function log(severity, body, attributes = {}) {
  logger.emit({ severityNumber: SeverityNumber[severity], severityText: severity, body, attributes });
}

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
    log("INFO", "Fetching orders list");
    await sleep(randomBetween(20, 80));
    span.setAttribute("db.system", "postgresql");
    span.setAttribute("db.statement", "SELECT * FROM orders LIMIT 10");
    if (errorSlot === "fetch-orders-from-db") {
      errorSpan(span, "Database connection timeout");
      log("ERROR", "Database connection timeout while fetching orders", { "db.system": "postgresql" });
    } else {
      log("DEBUG", "Orders query completed", { "db.system": "postgresql", "result.count": "3" });
    }
    span.end();
  });

  res.json({ orders: [{ id: 1 }, { id: 2 }, { id: 3 }] });
});

app.get("/orders/:id", async (req, res) => {
  const orderId = req.params.id;
  const errorSlot = pickOne(["fetch-order", "fetch-order-items"]);

  const order = await tracer.startActiveSpan("fetch-order", async (span) => {
    log("INFO", `Fetching order ${orderId}`, { "order.id": orderId });
    span.setAttribute("order.id", orderId);
    await sleep(randomBetween(10, 50));

    await tracer.startActiveSpan("fetch-order-items", async (childSpan) => {
      childSpan.setAttribute("db.system", "postgresql");
      childSpan.setAttribute("db.statement", `SELECT * FROM order_items WHERE order_id = ${orderId}`);
      await sleep(randomBetween(5, 30));
      if (errorSlot === "fetch-order-items") {
        errorSpan(childSpan, "Order not found");
        log("WARN", `Order items not found for order ${orderId}`, { "order.id": orderId });
      }
      childSpan.end();
    });

    if (errorSlot === "fetch-order") {
      errorSpan(span, "Failed to fetch order");
      log("ERROR", `Failed to fetch order ${orderId}`, { "order.id": orderId });
    } else {
      log("DEBUG", `Order ${orderId} fetched successfully`, { "order.id": orderId });
    }
    span.end();
    return { id: orderId, items: [{ sku: "ABC-1" }, { sku: "XYZ-2" }] };
  });

  res.json(order);
});

app.post("/orders", async (req, res) => {
  const errorSlot = pickOne(["validate-inventory", "charge-payment", "save-order-to-db", "create-order"]);

  const result = await tracer.startActiveSpan("create-order", async (rootSpan) => {
    rootSpan.setAttribute("order.source", "api");
    log("INFO", "Creating new order", { "order.source": "api" });

    await tracer.startActiveSpan("validate-inventory", async (span) => {
      await sleep(randomBetween(15, 40));
      span.setAttribute("inventory.checked", true);
      if (errorSlot === "validate-inventory") {
        errorSpan(span, "Item out of stock");
        log("WARN", "Inventory validation failed: item out of stock");
      } else {
        log("DEBUG", "Inventory validation passed");
      }
      span.end();
    });

    await tracer.startActiveSpan("charge-payment", async (span) => {
      const amount = randomBetween(10, 500);
      await sleep(randomBetween(80, 200));
      span.setAttribute("payment.provider", "stripe");
      span.setAttribute("payment.amount", amount);
      if (errorSlot === "charge-payment") {
        errorSpan(span, "Card declined");
        log("ERROR", "Payment charge failed: card declined", { "payment.provider": "stripe", "payment.amount": String(amount) });
      } else {
        log("INFO", "Payment charged successfully", { "payment.provider": "stripe", "payment.amount": String(amount) });
      }
      span.end();
    });

    await tracer.startActiveSpan("save-order-to-db", async (span) => {
      span.setAttribute("db.system", "postgresql");
      span.setAttribute("db.statement", "INSERT INTO orders ...");
      await sleep(randomBetween(10, 30));
      if (errorSlot === "save-order-to-db") {
        errorSpan(span, "Deadlock detected");
        log("ERROR", "Failed to save order: deadlock detected", { "db.system": "postgresql" });
      }
      span.end();
    });

    if (errorSlot === "create-order") {
      errorSpan(rootSpan, "Order creation failed");
      log("ERROR", "Order creation failed");
    }
    rootSpan.end();
    const orderId = randomBetween(100, 999);
    log("INFO", `Order ${orderId} created successfully`, { "order.id": String(orderId) });
    log("DEBUG", `Order audit trail: ${JSON.stringify({ orderId, source: "api", steps: ["validate-inventory", "charge-payment", "save-order-to-db"], timestamps: { validated: new Date().toISOString(), charged: new Date().toISOString(), saved: new Date().toISOString() }, metadata: { requestId: Math.random().toString(36).slice(2), region: "us-east-1", instanceId: "i-0abc123def456", traceFlags: 1, samplingRate: 0.1, userAgent: "Mozilla/5.0 (compatible; OrderService/2.1)", correlationId: Math.random().toString(36).slice(2), retryCount: 0, processingTimeMs: randomBetween(100, 400), paymentProvider: "stripe", inventoryNode: "node-" + randomBetween(1, 5) } })}`);
    return { id: orderId, status: "created" };
  });

  res.status(201).json(result);
});

app.get("/users/:id", async (req, res) => {
  const userId = req.params.id;
  const errorSlot = pickOne(["cache-lookup", "fetch-user"]);

  await tracer.startActiveSpan("fetch-user", async (span) => {
    log("INFO", `Fetching user ${userId}`, { "user.id": userId });
    span.setAttribute("user.id", userId);

    await tracer.startActiveSpan("cache-lookup", async (cacheSpan) => {
      cacheSpan.setAttribute("cache.backend", "redis");
      await sleep(randomBetween(1, 10));
      const hit = Math.random() > 0.5;
      cacheSpan.setAttribute("cache.hit", hit);
      log("DEBUG", `Cache ${hit ? "hit" : "miss"} for user ${userId}`, { "cache.backend": "redis", "cache.hit": String(hit) });

      await tracer.startActiveSpan("redis-get", async (redisSpan) => {
        redisSpan.setAttribute("redis.command", "GET");
        redisSpan.setAttribute("redis.key", `user:${userId}`);
        await sleep(randomBetween(1, 5));
        redisSpan.end();
      });

      if (errorSlot === "cache-lookup") {
        errorSpan(cacheSpan, "Redis connection refused");
        log("ERROR", "Redis connection refused during cache lookup", { "cache.backend": "redis" });
      }
      cacheSpan.end();
    });

    await sleep(randomBetween(5, 25));
    if (errorSlot === "fetch-user") {
      errorSpan(span, "User not found");
      log("WARN", `User ${userId} not found`, { "user.id": userId });
    }
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
