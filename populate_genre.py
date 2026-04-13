import psycopg2
import csv

conn_string = "postgresql://neondb_owner:npg_2F1ktmTIfnCW@ep-lingering-sky-akbzgff8.c-3.us-west-2.aws.neon.tech/BDpel?sslmode=require&channel_binding=require"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()


def create_genres_table(cursor):
    query = """
        CREATE TABLE IF NOT EXISTS genres (
            id SERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            genre TEXT NOT NULL
        );
        """
    cursor.execute(query)


create_genres_table(cursor)

# j = 0
try:
    with open("genres.txt", "r") as f:
        reader = csv.reader(f)
        # next(reader)  # skip header
        for i, row in enumerate(reader):
            row = eval(f"row")
            print(row.__class__)
            if i == 0:
                continue
            if len(row) == 0 or row[0] == "":
                #               print(j)
                #               j += 1
                continue
            cursor.execute(
                "INSERT INTO genres (id, category, genre) VALUES (%s, %s, %s)",
                (int(row[0]), row[1], row[2]),
            )

    # Commit changes
    # print(j)
    conn.commit()

except Exception as e:
    print("Error:", e)

finally:
    if conn:
        cursor.close()
        conn.close()
