from ydata_profiling import ProfileReport
import pandas as pd
import psycopg2
import re

conn_string = "postgresql://neondb_owner:npg_2F1ktmTIfnCW@ep-lingering-sky-akbzgff8.c-3.us-west-2.aws.neon.tech/BDpel?sslmode=require&channel_binding=require"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()


df = pd.read_csv("bdpel.csv")
profile = ProfileReport(df, title="Data Profileing")
profile.to_file("report.html")


def expression(valor):
    valor = str(valor)
    word_pattern = r"[a-zA-Z\u00C0-\u017F\-'&#+, .“”\"]+"
    # 1. Map characters to temporary placeholders
    valor = re.sub(word_pattern, "A", valor)
    valor = re.sub(r"\d", "D", valor)

    # 2. Group repeating placeholders and replace with Regex tokens
    valor = re.sub(r"A+", r"[a-zA-Z]+", valor)
    valor = re.sub(r"D+", r"\\d+", valor)

    return "^" + valor + "$"


null_query = """SELECT 
    column_name, 
    data_type, 
    is_nullable
FROM information_schema.columns
WHERE table_name = 'bdpel';"""
conns_query = """SELECT
    tc.constraint_name,
    tc.constraint_type,
    kcu.column_name
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
WHERE tc.table_name = 'bdpel';"""
cursor.execute(null_query)
null_results = cursor.fetchall()
cursor.execute(conns_query)
conns_results = cursor.fetchall()

patrones_por_columna = {}

for col in df.columns:
    patrones = set()

    for val in df[col].dropna():
        regex = expression(val)
        patrones.add(regex)

    patrones_por_columna[col] = patrones


for col, patrones in patrones_por_columna.items():
    print(f" Columna: {col}")
    print(f"Cantidad de expresiones: {len(patrones)}")
    if len(patrones) <= 25:
        for patron in patrones:
            print(f"  - {patron}")


print("Nulls:")
print(null_results)

print("Constraints:")
print(conns_results)
cursor.close()
conn.close()
