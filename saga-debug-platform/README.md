## Saga Demo Microservices (Order / Payment / Inventory)

This is a small, **clean** demo project showing three Spring Boot microservices communicating over **Kafka (KRaft)** with **PostgreSQL** and **Flyway**, using a simple order → payment → inventory flow.

### Services

- **order-service** (port 8081)
  - REST API to create and query orders.
  - Persists orders to `order_db`.
  - Publishes `OrderCreatedEvent` to Kafka.
  - Consumes final events from payment / inventory to update order status.

- **payment-service** (port 8082)
  - Consumes `OrderCreatedEvent`.
  - Persists payments to `payment_db`.
  - For demo, always marks payment as **COMPLETED**.
  - Publishes `PaymentCompletedEvent` (or `PaymentFailedEvent` if you change the rule).

- **inventory-service** (port 8083)
  - Consumes `PaymentCompletedEvent`.
  - Persists inventory and reservations to `inventory_db`.
  - Reserves stock or fails if not enough quantity.
  - Publishes `InventoryReservedEvent` or `InventoryFailedEvent`.

Each service uses:

- `controller` → REST endpoints
- `service` → business logic
- `domain` / `entity` → JPA entities and enums
- `repository` → Spring Data JPA
- `dto` → request/response types (with validation)
- `messaging` → Kafka events, producers, and consumers
- `config` → Kafka topic configuration
- `exception` → global exception handlers and simple custom exceptions

### Event flow

1. Client calls **order-service** `POST /api/orders`.
2. `order-service`:
   - Persists the order with status `CREATED`.
   - Publishes `OrderCreatedEvent` to topic `order-created`.
3. **payment-service**:
   - Consumes `order-created`.
   - Creates a `Payment` record.
   - Marks it `COMPLETED` (demo rule).
   - Publishes `PaymentCompletedEvent` to `payment-completed`.
4. **inventory-service**:
   - Consumes `payment-completed`.
   - Checks product inventory.
   - If enough stock:
     - Creates an `InventoryReservation` with status `RESERVED`.
     - Decrements `availableQuantity`.
     - Publishes `InventoryReservedEvent` to `inventory-reserved`.
   - If not enough stock:
     - Creates an `InventoryReservation` with status `FAILED`.
     - Publishes `InventoryFailedEvent` to `inventory-failed`.
5. **order-service**:
   - Consumes `payment-completed`, `payment-failed`, `inventory-reserved`, and `inventory-failed`.
   - Updates the `Order` status according to the rules:
     - Payment fails → order `FAILED`.
     - Payment succeeds and inventory fails → order `FAILED`.
     - Payment succeeds and inventory reserved → order `COMPLETED`.

### Kafka topics

- `order-created`
- `payment-completed`
- `payment-failed`
- `inventory-reserved`
- `inventory-failed`

### Infrastructure (Docker Compose)

From the project root:

```bash
docker-compose up -d
```

This starts:

- **PostgreSQL 15** (`demo-postgres`, port `5432`)
  - User: `demo`, Password: `demo`
  - Databases:
    - `order_db` (default created by container)
    - `payment_db`
    - `inventory_db`
- **Kafka (KRaft mode)** (`demo-kafka`, port `9092`)

Flyway migrations run automatically on service startup and create tables in each database. `inventory-service` seeds one demo product `PRODUCT-1` with quantity `100`.

### Running the services

1. Start infra:

```bash
docker-compose up -d
```

2. Build all services:

```bash
mvn clean package
```

3. Run each service in its own terminal:

```bash
cd order-service
mvn spring-boot:run

cd payment-service
mvn spring-boot:run

cd inventory-service
mvn spring-boot:run
```

4. Health checks (Actuator):

- `GET http://localhost:8081/actuator/health`
- `GET http://localhost:8082/actuator/health`
- `GET http://localhost:8083/actuator/health`

### Sample API flow (success case)

Assumes all three services are running and Kafka/Postgres are up via Docker Compose.

1. **Create an order**

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
        "customerId": "CUST-123",
        "productId": "PRODUCT-1",
        "quantity": 2,
        "amount": 50
      }'
```

Response (example):

```json
{
  "id": "d1b8c554-2a32-4d66-a6a0-8a9c7d7445f7",
  "customerId": "CUST-123",
  "productId": "PRODUCT-1",
  "quantity": 2,
  "amount": 50,
  "status": "CREATED",
  "createdAt": "...",
  "updatedAt": "..."
}
```

2. **Wait briefly** (Kafka processing)

Give it 1–2 seconds for events to propagate through payment and inventory services.

3. **Check order status**

```bash
curl http://localhost:8081/api/orders/d1b8c554-2a32-4d66-a6a0-8a9c7d7445f7
```

Expected final status (with seeded inventory and default payment success rule):

- `COMPLETED`

4. **Optional**: inspect payment and inventory

```bash
curl http://localhost:8082/api/payments/{paymentId}
curl http://localhost:8083/api/inventory/PRODUCT-1
```

### Important assumptions

- **No refunds / rollback**: if inventory fails after payment succeeds, the order is simply marked `FAILED`. There is no compensation or refund logic.
- **Idempotency is basic**:
  - Payment and inventory reservation look up by `orderId` and skip work if a record already exists.
  - Order updates ignore status changes once an order is `COMPLETED` or `FAILED`.
- **No extra infra**: no API gateway, discovery, config server, security/auth, or monitoring stack—this is intentionally minimal for teaching and demos.
- **Config via environment variables** for DB and Kafka hosts/ports, with sensible local defaults.

