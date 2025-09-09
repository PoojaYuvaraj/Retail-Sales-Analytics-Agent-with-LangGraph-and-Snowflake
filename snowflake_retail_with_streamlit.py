import os, re, time
import pandas as pd
import streamlit as st
import snowflake.connector
from dotenv import load_dotenv
from openai import OpenAI

# ---------- Load env ----------
load_dotenv()
st.set_page_config(page_title="Snowflake Retail Agent", layout="wide")

# ---------- OpenAI client ----------
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ---------- Guardrails ----------
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
    if DDL_DML_PATTERN.search(sql): 
        return False
    # Forbid non-whitelisted tables in FROM/JOIN
    for token in re.findall(r"(?:FROM|JOIN)\s+([A-Z_][A-Z0-9_\.]*)", sql.upper()):
        base = token.split(".")[-1]
        if base not in ALLOWED_TABLES:
            return False
    # Must start with SELECT
    return sql.strip().upper().startswith("SELECT")

def nl_to_sql(user_query: str, schema_summary: str) -> str:
    prompt = f"""
You are a Snowflake SQL expert. Generate a single SELECT query only (no comments, no explanations).
Use only these tables/columns:
{schema_summary}

User question: {user_query}
Return ONLY the SQL.
"""
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role":"user","content":prompt}],
        temperature=0
    )
    inter_sql = resp.choices[0].message.content.strip().strip("`").rstrip(";")
    clean_sql = inter_sql.strip().strip("sql\n")
    return clean_sql

@st.cache_data(ttl=300)
def run_sql_df(sql: str, limit: int = 500) -> pd.DataFrame:
    with sf_conn() as c, c.cursor(snowflake.connector.DictCursor) as cur:
        
        if("limit" in sql.lower()):
            cur.execute(f"{sql}")
        else:
            sql = sql.replace(";","")
            cur.execute(f"{sql} LIMIT {limit}")
        rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

# ------------------- UI -------------------
st.title("Snowflake Retail Agent")
st.caption("Ask natural-language questions → get safe SELECT SQL + results")

with st.sidebar:
    st.subheader("Connection (from .env)")
    st.text(f"Account: {os.getenv('SNOWFLAKE_ACCOUNT','')}")
    st.text(f"DB / Schema: {os.getenv('SNOWFLAKE_DATABASE','')}/{os.getenv('SNOWFLAKE_SCHEMA','')}")
    st.text(f"Warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE','')}")
    limit = st.number_input("Row limit", min_value=10, max_value=5000, value=500, step=10)
    st.markdown("---")
    st.caption("Tables allowed: PRODUCTS, CUSTOMERS, ORDERS, ORDER_ITEMS")

# Schema cache on first load
if "schema_summary" not in st.session_state:
    with st.spinner("Loading schema…"):
        st.session_state.schema_summary = get_schema_summary()

default_q = "Show monthly revenue by category for 2025."
user_q = st.text_input("Your question", value=default_q, placeholder="e.g., Top 5 customers by total spend in 2025")
go = st.button("Run")

if go and user_q.strip():
    with st.spinner("Thinking…"):
        schema_summary = st.session_state.schema_summary
        sql = nl_to_sql(user_q.strip(), schema_summary)

        # safety
        safe = is_safe_sql(sql)
        st.subheader("Generated SQL")
        st.code(sql or "-- no sql", language="sql")
        if not safe:
            st.error("Unsafe or invalid SQL detected. Only read-only SELECTs on whitelisted tables are allowed.")
        else:
            t0 = time.time()
            try:
                df = run_sql_df(sql, limit=limit)
                dt = time.time() - t0
                st.success(f"Done in {dt:.2f}s — {len(df)} rows")
                st.dataframe(df, use_container_width=True)
                # Optional simple chart if columns exist
                numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
                non_numeric = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]
                if numeric_cols and non_numeric:
                    with st.expander("Quick chart"):
                        x = st.selectbox("X (categorical)", non_numeric, index=0)
                        y = st.selectbox("Y (numeric)", numeric_cols, index=0)
                        st.bar_chart(df.set_index(x)[y])
            except Exception as e:
                st.error(f"Execution error: {e}")
else:
    st.info("Enter a question and click Run. Try: *Top customers by spend*, *Monthly revenue trend 2025*, or *Sales by category*.")
