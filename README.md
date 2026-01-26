# 🛒 Zia's Mart API 

An **event-driven, microservices-based Online Mart API** built using modern cloud-native technologies. This project is designed to be **scalable, maintainable, and production-ready**, following industry best practices such as **TDD, BDD, CI/CD, and cloud deployment**.

> 🎯 **Goal:** Build a real-world online mart backend capable of handling high traffic, asynchronous workflows, and distributed services — similar to large-scale e-commerce platforms.

---

## 📌 Project Overview

The **Online Mart API** is developed using an **Event-Driven Microservices Architecture** where each service is independently deployable and communicates asynchronously using **Kafka events** and **Protocol Buffers (Protobuf)**.

The system leverages:

* **FastAPI** for high-performance APIs
* **Kafka** for event streaming
* **Dapr** for service-to-service communication
* **Kong API Gateway** for request routing and security
* **Docker & Docker Compose** for containerization
* **Azure Container Apps (ACA)** for cloud deployment
* **GitHub Actions** for CI/CD automation

---

## 🧱 Architecture Overview

```
Client
  ↓
Kong API Gateway
  ↓
┌──────────────────────────────────────┐
│  Event-Driven Microservices System   │
│                                      │
│  User Service  ─┐                    │
│  Product Service ├─► Kafka (Events)  │
│  Order Service   ┤                   │
│  Inventory Service┘                  │
│  Notification Service                │
│  Payment Service                     │
└──────────────────────────────────────┘
```

Each microservice:

* Owns its **own database** (Database-per-Service pattern)
* Publishes and consumes **Kafka events**
* Uses **Protobuf** for message serialization
* Runs with a **Dapr sidecar**

---

## 📂 Repository Structure

```
.
├── init/kafka                # Kafka & topic initialization
├── kong                      # Kong API Gateway configuration
├── user-service              # User authentication & profiles
├── product-service           # Product catalog & CRUD
├── order-service             # Order creation & tracking
├── inventory-service         # Stock & inventory management
├── notification-service      # Email/SMS notifications
├── root                      # Shared configs & orchestration
└── docker-compose.yml        # Local development orchestration
```

---

## 🧩 Microservices Breakdown

### 👤 User Service

* User registration & authentication
* Profile management
* Emits user-related events

### 📦 Product Service

* Product CRUD operations
* Product availability events

### 🛒 Order Service

* Order creation & tracking
* Emits order placed / updated events

### 📊 Inventory Service

* Stock management
* Listens to order events
* Updates product availability

### 🔔 Notification Service

* Sends notifications (Email/SMS)
* Listens to order & payment events

### 💳 Payment Service

* Handles payments & transactions
* **Local:** PayFast (GoPayFast)
* **International:** Stripe

---

## 🔄 Event-Driven Communication

* **Kafka** acts as the central event bus
* Services communicate asynchronously
* **Protobuf** ensures fast & compact message serialization

Example events:

* `UserRegistered`
* `OrderPlaced`
* `InventoryUpdated`
* `PaymentCompleted`

---

## 🛠️ Tech Stack

### Backend & Infrastructure

* **FastAPI**
* **Python**
* **SQLModel + PostgreSQL**
* **Kafka**
* **Protocol Buffers (Protobuf)**
* **Dapr**
* **Kong API Gateway**

### DevOps & Cloud

* **Docker & Docker Compose**
* **DevContainers (VS Code)**
* **GitHub Actions (CI/CD)**
* **Azure Container Apps (ACA)**
* **Azure Container Registry (ACR)**

### Testing

* **Pytest** → Unit testing (TDD)
* **Behave** → Behavior-driven testing (BDD)

---

## 🧪 Development Methodology

### ✅ Test-Driven Development (TDD)

* Write tests **before** implementation
* Ensures correctness & prevents regressions
* Implemented using **Pytest**

### ✅ Behavior-Driven Development (BDD)

* Business-readable tests using **Gherkin syntax**
* Implemented using **Behave**
* Focuses on user behavior & acceptance criteria

---

## 🚀 Local Development Setup

### Prerequisites

* Docker & Docker Compose
* VS Code (recommended)
* DevContainers extension

### Run Locally

```bash
docker compose up --build
```

This will:

* Start all microservices
* Create Kafka, PostgreSQL instances
* Run Dapr sidecars
* Expose APIs through Kong

---

## ☁️ Cloud Deployment (Azure)

* **Azure Container Apps (ACA)**
* **Dapr + KEDA** for scaling
* **GitHub Actions** for CI/CD

### Deployment Flow

1. Push code to GitHub
2. GitHub Actions runs:

   * Tests (TDD + BDD)
   * Build Docker images
3. Deploy to Azure Container Apps **only if tests pass**

---

## 📈 Monitoring & Observability (Optional)

* Prometheus & Grafana for metrics
* Centralized logging
* Service health monitoring

---

## 🎯 Key Highlights

* ✔ Event-driven microservices
* ✔ Database-per-service
* ✔ Protobuf-based messaging
* ✔ Dapr-powered communication
* ✔ Kong API Gateway
* ✔ TDD + BDD
* ✔ Cloud-native & scalable
* ✔ CI/CD with GitHub Actions

---

## 🧠 Learning Outcomes

* Real-world microservices architecture
* Event-driven system design
* Distributed systems communication
* Cloud deployment & automation
* Testing-driven development

---

## 👨‍💻 Author

**Saad Kamran**
Full-Stack Developer
🌐 Portfolio: [https://saad-devportfolio.vercel.app/](https://saad-devportfolio.vercel.app/)
🐙 GitHub: [https://github.com/saad-kamran-2006](https://github.com/saad-kamran-2006)

---

⭐ *If you find this project useful, don’t forget to star the repository!*
