import psycopg2
from psycopg2 import sql
import os
cwd = os.getcwd()
# ---------------------------------------
# Create a database schema
# Please change the database name, username, and password to your own
def create_schema():
    conn = psycopg2.connect(
        host="localhost",
        database="your_db_name",
        user="your_username",
        password="your_password"
    )
    
    try:
        with open('cleaned_schema.sql', 'r') as sql_file:
            sql_commands = sql_file.read()
        
        cursor = conn.cursor()
        cursor.execute(sql_commands)
        conn.commit()
        print("Database schema created successfully.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL or creating schema: {error}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

if __name__ == '__main__':
    create_schema()