# Spark Bulk Data Load (SBDL) Service

## 📖 Project Overview
The Spark Bulk Data Load (SBDL) service is a production-grade Data Engineering pipeline designed for a multinational bank. The project addresses the challenge of synchronizing "Golden Copy" entity data from a central Master Data Management (MDM) platform to various downstream consumer systems (e.g., BI Dashboards, ML Prediction models) without overloading the source system.
By decoupling the MDM platform using a Kafka messaging layer, this batch pipeline ensures high scalability and reliable data distribution across the enterprise.
---

## ⭐ Business Context
* **The Domain:** A large financial institution using an MDM platform to synchronize master data (clients, employees, products, facilities) across the enterprise.
* **The "Gold Copy":** The MDM platform acts as the single source of truth, collecting data from various systems to maintain accurate entity records.
* **The Bottleneck:** Downstream systems currently rely on direct API calls to the MDM to fetch entity updates. As the number of consuming systems grows, this approach places an unsustainable load on the MDM platform and is not scalable for bulk data synchronization.

---

## 📐 Solution Architecture
To solve the scalability issue, this project implements a **Batch Data Pipeline** using PySpark on a Hadoop cluster.

![images/Architecture Diagram](Architecture_Diagram.png)

### ▶️ The workflow is as follows:
The project focuses specifically on the pipeline between the Hadoop/Hive cluster and Kafka.

1.  **Ingestion (Pre-existing):** MDM exports entity data to Hive tables daily (partitioned by `load_date`). (For local development used test_data) 
2.  **Processing (In Scope):** The Spark application reads specific partitions, handles complex joins (Accounts, Parties, Addresses), and prepares the final payload.
3.  **Publication (In Scope):** The application processes, prepares, and pushes the data to a Kafka Cluster.
4.  **Consumption (Out of Scope):** Downstream systems subscribe to the Kafka topics to synchronize their local databases.


---

## 📊 Data Model & Transformation Logic

### 1. Source Data (Hive Tables)
The **Account Entity** is reconstructed by joining data from three distinct Hive tables to create a unified view of a contract.



* **`accounts`**: Contains core contract details (Account ID, Start Date, Legal Titles, Active Indicator).
* **`parties`**: The junction table that links accounts to one or more individuals or entities (Party ID, Account ID).
* **`address`**: Contains the physical location details associated with each unique `Party ID`.

---

### 2. Target Output (Kafka JSON Schema)
The final output is a generic, nested JSON structure designed for high compatibility across all 500+ bank entities. This "envelope" ensures that downstream consumers can use a standardized parser.



#### A. Event Header
Contains metadata for auditing and tracking the record as it moves through the enterprise:
* **`eventIdentifier`**: A dynamically generated **UUID** for unique message tracing.
* **`eventType`**: A fixed string used for routing (e.g., `SBDL-Contract` for account-related events).
* **`eventDateTime`**: The current system timestamp captured at the moment the message is produced.

#### B. Keys
An array of structs containing the primary identifiers required for record lookup and Kafka partitioning.
* **Account Mapping**: To maintain a Common Data Model (CDM), the `account_id` from the source is renamed to **`contractIdentifier`** in the output.

#### C. Payload
The actual business data, formatted as a set of change-tracking triplets to support CDC (Change Data Capture) requirements.

> **The Triplet Rule:** Every business field is encapsulated in a structure containing an `operation`, `oldValue`, and `newValue`.

* **Current Phase Implementation:** For this bulk load, the `operation` is hardcoded as **`INSERT`**, and the `oldValue` is defaulted to **`NULL`** (and typically omitted from the final JSON string to save bandwidth).

---

### 3. Transformation Flow (The "One-to-Many" Join)
The Spark application handles the one-to-many relationship between **Accounts** and **Parties** by:
1.  Joining the three tables on `account_id` and `party_id`.
2.  Grouping the data by `account_id`.
3.  Collecting party and address details into a nested **Array of Structs** inside the final Payload.

---

## 📄 Sample Output Message

The final output sent to Kafka follows a strict JSON schema designed for enterprise interoperability. Each record represents a single **"Golden Copy"** of a bank account (contract).



### JSON Structure Breakdown
Below is a single event representing the account **6982391060**. This example demonstrates the nested "One-to-Many" relationship where a single account contains multiple legal title lines and associated party details.
> 📂 **View Full Output:** For the complete sample output, check `test_data/results/final_df.json`.
```json
{
  "eventHeader": {
    "eventIdentifier": "c361a145-d2fc-434e-a608-9688caa6d22e",
    "eventType": "SBDL-Contract",
    "majorSchemaVersion": 1,
    "minorSchemaVersion": 0,
    "eventDateTime": "2022-09-06T20:49:03+0530"
  },
  "keys": [
    {
      "keyField": "contractIdentifier",
      "keyValue": "6982391060"
    }
  ],
  "payload": {
    "contractIdentifier": { 
        "operation": "INSERT", 
        "newValue": "6982391060" 
    },
    "sourceSystemIdentifier": { 
        "operation": "INSERT", 
        "newValue": "COH" 
    },
    "contactStartDateTime": { 
        "operation": "INSERT", 
        "newValue": "2018-03-24T13:56:45.000+05:30" 
    },
    "contractTitle": {
      "operation": "INSERT",
      "newValue": [
        { "contractTitleLineType": "lgl_ttl_ln_1", "contractTitleLine": "Tiffany Riley" },
        { "contractTitleLineType": "lgl_ttl_ln_2", "contractTitleLine": "Matthew Davies" }
      ]
    },
    "partyRelations": [
      {
        "partyIdentifier": { "operation": "INSERT", "newValue": "9823462810" },
        "partyRelationshipType": { "operation": "INSERT", "newValue": "F-N" },
        "partyAddress": {
          "operation": "INSERT",
          "newValue": {
            "addressLine1": "45229 Drake Route",
            "addressCity": "Shanefort",
            "addressCountry": "Canada"
          }
        }
      }
    ]
  }
}
```
## 📂 Project Structure

A modular directory layout ensures that the business logic is separated from configuration and testing assets, making the application "production-ready" for enterprise environments.



```plaintext
Spark-Bulk-Data-Load-Application/
├── conf/               # Environment-specific configurations (sbdl.conf, spark.conf)
├── lib/                # Modular Python scripts (Utils, ConfigLoader, Transformations)
├── test_data/          # Sample datasets for local testing
├── .gitignore          # Prevents sensitive files and virtual envs from being committed
├── log4j.properties    # Professional logging configuration for Spark
├── Pipfile             # Deterministic dependency management (Pipenv)
├── README.md           # Project documentation
├── sbdl_main.py        # Entry point for execution
├── sbdl_submit.sh      # Shell script for automated spark-submit 
└── test_pytest_sbdl.py # Unit tests for the application logic using pytest
```
---


## 🚀 Key Technical Features

* **Modular Code Architecture:** Separated concerns between Data Loaders, Transformations, and Utility functions to ensure code reusability and maintainability.
* **Environment-Based Configuration:** Dynamically switches configurations for **LOCAL, QA, or PROD** environments by reading `sbdl.conf` and `spark.conf` to manage Hive connectivity, Kafka clusters, and Spark resource allocation.
* **Schema Management:** Professional handling of complex nested structures (Arrays and Structs) and multi-table joins to reconstruct the "Golden Copy" entity.
* **Production-Grade Logging:** Integrated `log4j` configuration for detailed audit trails and easier debugging in a cluster environment.
* **Unit Testing:** A robust test suite developed using `pytest` to ensure 100% transformation accuracy before deployment.
---

## 💻 Technology Stack
* **Language:** Python 3.11 (PySpark)
* **Compute Engine:** Apache Spark 3.5.0
* **Data Storage:** Apache Hive (Hadoop HDFS) (For local development used test_data)    
* **Messaging System:** Apache Kafka (Confluent Cloud)
* **Testing:** Pytest

## 📚 Acknowledgements & Learning Outcomes
This Capstone Project was developed as part of the **"PySpark - Apache Spark Programming for Beginners"** course on Udemy, instructed by **Prashant Kumar Pandey**.

Through the development of this application, I implemented and mastered the following core Data Engineering concepts:
* **Apache Spark Architecture:** Deep understanding of Drivers, Executors, Partitions, and the Spark Execution Plan.
* **PySpark Transformations & Actions:** writing efficient transformations to process large-scale datasets.
* **Schema Management:** Handling complex schemas and strictly typed data structures.
* **Advanced Data Processing:** Working with Complex Data Types (Array/Struct), Window Functions, Aggregations, and Joins.
* **Engineering Standards:** Implementing a **Modular Code Structure** and **Unit Testing (`pytest`)** to ensure production-grade reliability
---
