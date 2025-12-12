import psycopg2

conn = psycopg2.connect(
    dbname="rlt_test",
    user="postgres",
    password="jalol",
    host="localhost",
    port=5432
)

cur = conn.cursor()

cur.execute("DROP TABLE videos")
cur.execute("DROP TABLE video_snapshots")
conn.commit()
cur.close()
conn.close()