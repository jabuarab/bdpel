import psycopg2
import csv

conn_string = "postgresql://neondb_owner:npg_2F1ktmTIfnCW@ep-lingering-sky-akbzgff8.c-3.us-west-2.aws.neon.tech/BDpel?sslmode=require&channel_binding=require"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()


def create_bdpel_table():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bdpel (
            idPel TEXT PRIMARY KEY,
            titulo TEXT,
            descripcion TEXT,
            directores TEXT,
            actores TEXT,
            generos TEXT,
            duracion TEXT,
            anoEstreno TEXT
        );
        """)


def popdataset1():
    # show_id,type,title,director,cast,country,date_added,release_year,rating,duration,listed_in,description

    with open("dataset1//dataset1.csv", "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i == 0:
                continue
            if len(row) == 0:  # or row[0] == "":
                continue
            cursor.execute(
                "INSERT INTO bdpel (idPel, titulo, descripcion, directores, actores,generos,  duracion, anoEstreno) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (row[0], row[2], row[11], row[3], row[4], row[10], row[9], row[7]),
            )


def popdataset2():
    # id,title,type,description,release_year,age_certification,runtime,genres,production_countries,seasons,imdb_id,imdb_score,imdb_votes,tmdb_popularity,tmdb_score

    with open("dataset2//dataset2.csv", "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i == 0:
                continue
            if len(row) == 0:  # or row[0] == "":
                continue

            cursor.execute(
                "INSERT INTO bdpel (idPel, titulo, descripcion, generos, duracion, anoEstreno) VALUES (%s, %s, %s, %s, %s, %s)",
                (row[0], row[1], row[3], row[7], row[6], row[4]),
            )


if __name__ == "__main__":
    create_bdpel_table()
    popdataset1()
    # popdataset2()
    conn.commit()
    cursor.close()
    conn.close()
