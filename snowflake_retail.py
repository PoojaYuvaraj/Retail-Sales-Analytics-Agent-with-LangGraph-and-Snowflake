import os, re, json, snowflake.connector
from dotenv import load_dotenv
from typing import Dict, Any
import streamlit as st
from pydantic import BaseModel
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from openai import OpenAI

load_dotenv()
st.set_page_config(page_title="Snowflake Retail Agent", layout="wide")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

ALLOWED_TABLES = {"PRODUCTS","CUSTOMERS","ORDERS","ORDER_ITEMS"}
DDL_DML_PATTERN = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE)\b", re.I)

def sf_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE","COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE","RETAIL_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA","PUBLIC"),
    )


def get_schema_summary() -> str:
    q = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME IN ('PRODUCTS','CUSTOMERS','ORDERS','ORDER_ITEMS')
    ORDER BY TABLE_NAME, ORDINAL_POSITION
    """
    with sf_conn() as c, c.cursor() as cur:
        cur.execute(q)
        rows = cur.fetchall()
    schema = {}
    for t, col, typ in rows:
        schema.setdefault(t, []).append(f"{col}:{typ}")
    return "\n".join(f"{t}({', '.join(cols)})" for t, cols in schema.items())

def is_safe_sql(sql: str) -> bool:
    if DDL_DML_PATTERN.search(sql): return False
    mentioned = set(re.findall(r"\b([A-Z_][A-Z0-9_]*)\b", sql.upper()))
    if not mentioned.intersection(ALLOWED_TABLES):
        # must reference at least one allowed table when FROM/JOIN exists
        pass
    # simple table whitelist: forbid tables outside ALLOWED_TABLES if after FROM/JOIN
    for token in re.findall(r"(?:FROM|JOIN)\s+([A-Z_][A-Z0-9_\.]*)", sql.upper()):
        base = token.split(".")[-1]
        if base not in ALLOWED_TABLES:
            return False
    return True

class AgentState(BaseModel):
    user_query: str
    schema_summary: str = ""
    sql: str = ""
    result: Any = None
    error: str = ""



def node_schema(state: AgentState):
    state.schema_summary = get_schema_summary()
    return state

def node_nl2sql(state: AgentState):
    prompt = f"""
You are a Snowflake SQL expert. Generate a single SELECT query only (no comments) for the user question.
Use only these tables/columns:\n{state.schema_summary}\n
User question: {state.user_query}
Return ONLY the SQL.
"""
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0
    )
    state.sql = resp.choices[0].message.content.strip().strip("`")
    state.sql = state.sql.strip().strip("sql\n")
    print("SQL state:",state)
    return state



def node_validate(state: AgentState):
    sql = state.sql.strip().rstrip(";")
    if not sql.upper().startswith("SELECT") or not is_safe_sql(sql):
        state.error = "Unsafe or invalid SQL. Only read-only SELECT on whitelisted tables is allowed."
    else:
        state.sql = sql
    return state


def node_execute(state: AgentState):
    if state.error: return state
    try:
        with sf_conn() as c, c.cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(state.sql + " LIMIT 200")
            state.result = cur.fetchall()
    except Exception as e:
        state.error = f"Execution error: {e}"
    return state

def node_fix(state: AgentState):
    if not state.error: return state
    # attempt one self-repair via the model
    prompt = f"""
You produced SQL that failed with error: {state.error}
Schema:\n{state.schema_summary}
Original question: {state.user_query}
Return a corrected SELECT-only SQL using only allowed tables. Return ONLY SQL.
"""
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0
    )
    candidate = resp.choices[0].message.content.strip().strip("`")
    if is_safe_sql(candidate):
        state.sql = candidate
        state.error = ""
        return node_execute(state)
    return state


graph = StateGraph(AgentState)
graph.add_node("schema", node_schema)
graph.add_node("nl2sql", node_nl2sql)
graph.add_node("validate", node_validate)
graph.add_node("execute", node_execute)
graph.add_node("fix", node_fix)


graph.set_entry_point("schema")
graph.add_edge("schema","nl2sql")
graph.add_edge("nl2sql","validate")
graph.add_edge("validate","execute")
graph.add_edge("execute","fix")
graph.add_edge("fix", END)

checkpointer = MemorySaver()
app = graph.compile(checkpointer=checkpointer)
config = {"configurable": {"thread_id": "retail-demo-1"}}


if __name__ == "__main__":
    question = "list the customers who have made the maximum amount of purchases"
    state = AgentState(user_query=question)
    out = app.invoke(state, config=config) if config else app.invoke(state)

    print(out)
    print("\n--- SQL ---\n", out["sql"])
    print("\n--- RESULT (first rows) ---\n", out["result"][:5] if out["result"] else out["error"])