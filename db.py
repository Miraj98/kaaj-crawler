from dotenv import load_dotenv
load_dotenv()

import os
import psycopg2
from psycopg2.extras import RealDictCursor

class Database:
    def __init__(self):
        self.conn = psycopg2.connect(database = os.getenv("DB_NAME"), user = os.getenv("DB_USER"), password = os.getenv("DB_PASS"), host = os.getenv("DB_HOST"), port = os.getenv("DB_PORT"))
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)

    def insert_data(self, data):
        values_str = ','.join(self.cursor.mogrify("(%s, %s, %s, %s, %s)", item).decode('utf-8') for item in data)
        insert_query = f"INSERT INTO licenses (name, license_number, city, state, is_license_active) VALUES {values_str} ON CONFLICT DO NOTHING;"
        try:
            self.cursor.execute(insert_query)
            self.conn.commit()
        except Exception as e:
            print("Error inserting:", e)

    def close(self):
        self.cursor.close()
        self.conn.close()

    def search_query(self, search_str: str | None = None, cursor: int = 0):
        q = f"SELECT * FROM licenses WHERE name ILIKE '{search_str}%' AND id > {cursor} ORDER BY id LIMIT 50" if search_str else f"SELECT * FROM licenses WHERE id > {cursor}  ORDER BY id LIMIT 50"
        try:
            self.cursor.execute(q)
            res = self.cursor.fetchall()
            return res
        except Exception as e:
            print("Error inserting:", e)


if __name__ == "__main__":
    db = Database()
    res = db.search_query()
    if res:
        print(res)
        print("Total len:", len(res))
    db.close()
