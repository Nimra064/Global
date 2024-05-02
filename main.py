import psycopg2
import subprocess
import datetime
import os

def backup_postgresql_database(host, port, username, password, database, backup_dir):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    backup_file = f"{database}_{timestamp}.sql"
    backup_path = os.path.join(backup_dir, backup_file)

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )

        cursor = conn.cursor()

        os.makedirs(backup_dir, exist_ok=True)

        pg_dump_path = 'C:/Program Files/PostgreSQL/10/bin/pg_dump.exe'  

        subprocess.run([
            pg_dump_path,
            '-h', host,
            '-p', str(port),
            '-U', username,
            '-d', database,
            '-F', 'c',  
            '-f', backup_path
        ], check=True)

        print("Backup successful!")
    except (Exception, psycopg2.Error, subprocess.CalledProcessError) as e:
        print(f"Backup failed: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Example usage:
backup_postgresql_database(
    host='localhost',
    port=5432,
    username='postgres',
    password='1234',
    database='urlscrap',
    backup_dir='PostsqlBackup/Backup'
)


