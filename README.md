# 🛍️ Retail Sales Analytics Agent with LangGraph and Snowflake

An **agentic workflow** that converts **natural-language questions to safe SQL** for a retail dataset in **Snowflake**.  
It uses **LangGraph** for orchestration, **OpenAI** for NL→SQL, and strict **guardrails** to ensure read-only access.

---

## 1. Features
- **Natural Language → SQL** (e.g., “Top categories by sales in 2025”)
- **Safety Guardrails** (blocks DDL/DML; table whitelist)
- **Auto-Repair** on SQL errors with schema + error context
- **Stepwise Workflow**: Schema → NL2SQL → Validate → Execute → Fix
- **Direct Snowflake Integration**; results as dicts/DataFrames

---

## 2. Architecture

```mermaid
flowchart TD
    A[User Query] --> B[LangGraph Agent]
    B --> C[Schema Node: Load Snowflake Schema]
    C --> D[NL2SQL Node: Generate SQL]
    D --> E[Validate Node: Guardrails]
    E -->|Safe| F[Execute Node: Run in Snowflake]
    E -->|Unsafe| G[Return Error]
    F --> H[Fix Node (if needed)]
    H --> D
    F --> I[Results Returned]
```

---

## 3. Project Structure
```
retail_agent/
│
├── retail_agent.py       # Main agent (LangGraph pipeline)
├── requirements.txt      # Python dependencies
├── .env                  # Environment variables (Snowflake + OpenAI)
└── README.md             # This file
```

---

## 4. Prerequisites
- Python 3.10+
- A Snowflake account (warehouse, database, schema)
- OpenAI API key

---

## 5. Setup

### 5.1 Create and activate a virtual environment
```bash
python -m venv venv
# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate
```

### 5.2 Install dependencies
```bash
pip install -r requirements.txt
```

**requirements.txt**
```txt
python-dotenv
snowflake-connector-python
pandas
langgraph
openai
```

### 5.3 Configure environment variables
Create a `.env` file in the project root:
```ini
OPENAI_API_KEY=sk-...
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=youraccount.region.cloud
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RETAIL_DB
SNOWFLAKE_SCHEMA=PUBLIC
```

### 5.4 Seed sample data (Snowflake)
Create minimal retail tables (`PRODUCTS`, `CUSTOMERS`, `ORDERS`, `ORDER_ITEMS`) and insert sample rows.  
(Use your existing seed script or ask the agent author for the SQL snippet.)

---

## 6. Usage

Run the agent with a sample question:
```bash
python retail_agent.py
```

Inside `retail_agent.py`, you can start with:
```python
if __name__ == "__main__":
    q = "Show monthly revenue by category for 2025."
    state = AgentState(user_query=q)
    out = app.invoke(state, config={"configurable":{"thread_id":"retail-demo-1"}})

    print("\n--- SQL ---\n", out.sql)
    print("\n--- RESULT (first rows) ---\n", out.result[:5] if out.result else out.error)
```

---

## 7. Example Queries
- “Top 5 products by sales revenue”
- “Average order value by month in 2025”
- “Monthly revenue trend for 2025”
- “Which category had the highest sales last quarter?”

---

## 8. Guardrails
- Only **`SELECT`** queries are allowed.
- DDL/DML blocked: `CREATE`, `INSERT`, `UPDATE`, `DELETE`, `ALTER`, `DROP`, `MERGE`, etc.
- Table whitelist (edit in code as needed):
  - `PRODUCTS`
  - `CUSTOMERS`
  - `ORDERS`
  - `ORDER_ITEMS`

---

## 9. Notes & Next Steps
- This is a **learning/portfolio** project showing an agentic analytics pipeline.
- Extensions:
  - Streamlit UI for non-technical users
  - Visualization/dashboarding
  - Caching & query history
  - Additional intents (KPI packs, diagnostics)
