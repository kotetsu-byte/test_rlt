import json
import psycopg2

with open("videos.json", "r", encoding="utf-8") as file:
    data = json.load(file)

conn = psycopg2.connect(
    dbname="rlt_test",
    user="postgres",
    password="jalol",
    host="localhost",
    port=5432
)
cur = conn.cursor()

cur.execute("CREATE TABLE videos(" \
"id VARCHAR(250) PRIMARY KEY," \
"creator_id VARCHAR(250)," \
"video_created_at TIMESTAMP," \
"views_count INT," \
"likes_count INT," \
"comments_count INT," \
"reports_count INT," \
"created_at TIMESTAMP," \
"updated_at TIMESTAMP)")

cur.execute("CREATE TABLE video_snapshots(" \
"id VARCHAR(250) PRIMARY KEY," \
"video_id VARCHAR(250) REFERENCES videos(id)," \
"views_count INT," \
"likes_count INT," \
"comments_count INT," \
"reports_count INT," \
"delta_views_count INT," \
"delta_likes_count INT," \
"delta_comments_count INT," \
"delta_reports_count INT," \
"created_at TIMESTAMP," \
"updated_at TIMESTAMP)")

for i in data["videos"]:
    cur.execute(f"INSERT INTO videos(id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at) \
    VALUES('{i["id"]}', '{i["creator_id"]}', '{i["video_created_at"]}', {i["views_count"]}, {i["likes_count"]}, {i["comments_count"]}, {i["reports_count"]}, '{i["created_at"]}', '{i["updated_at"]}')")
    for j in i["snapshots"]:
        cur.execute(f"INSERT INTO video_snapshots(id, video_id, views_count, likes_count, comments_count, reports_count, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at, updated_at) \
        VALUES('{j["id"]}', '{j["video_id"]}', {j["views_count"]}, {j["likes_count"]}, {j["comments_count"]}, {j["reports_count"]}, {j["delta_views_count"]}, {j["delta_likes_count"]}, {j["delta_comments_count"]}, {j["delta_reports_count"]}, '{j["created_at"]}', '{j["updated_at"]}')")

conn.commit()   

cur.close()
conn.close()