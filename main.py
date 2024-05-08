import os
import subprocess
import shutil
import time
import psycopg2
from datetime import datetime
import threading

# PostgreSQL connection parameters
PGDATABASE = "postgres"
PGUSER = "postgres"
PGHOST = "RED-DBA-PGS-P01"

# Directory for backups
dest_directory = "PostsqlBackup/Backup"

# Create the destination directory if it doesn't exist
os.makedirs(dest_directory, exist_ok=True)

# Database connection parameters
db_params = {
    "dbname": "automation",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": "5432"
}

# SQL statement to create the backup_status table
create_backup_status_table_sql = """
CREATE TABLE IF NOT EXISTS backup_status (
    id SERIAL PRIMARY KEY,
    ticket_number VARCHAR(255),
    database_name VARCHAR(255),
    hostname VARCHAR(255),
    createdat TIMESTAMP,
    updatedat TIMESTAMP,
    status VARCHAR(255),
    backup_size BIGINT
);
"""

# Function to create the backup_status table
def create_backup_status_table():
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(create_backup_status_table_sql)
        conn.commit()
        cur.close()
        conn.close()
        print("backup_status table created successfully.")
    except psycopg2.Error as e:
        print(f"Error creating backup_status table: {e}")

def get_database_name():
    while True:
        pg_database = input("Enter the database name: ")
        if check_database_exists(pg_database):
            return pg_database
        else:
            print(f"Database '{pg_database}' does not exist. Please enter an existing database name.")

def check_database_exists(database_name):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = %s)", (database_name,))
        exists = cur.fetchone()[0]
        cur.close()
        conn.close()
        return exists
    except psycopg2.Error as e:
        print(f"Error checking database existence: {e}")
        return False

def validate_ticket(ticket):
    return ticket.isdigit()

def check_ticket_dir_in_backup(ticket):
    existdir = os.path.isdir(os.path.join(dest_directory, ticket))
    if existdir:
        return True
    return False

def backup_progress_monitor(ticket, backup_file):
    backup_file = os.path.join(dest_directory, ticket, f"backup_{ticket}.db")
    start_time = time.time()
    while os.path.exists(backup_file):
        backup_size = os.path.getsize(backup_file)
        elapsed_time = time.time() - start_time
        if elapsed_time != 0:  
            transfer_rate = backup_size / elapsed_time
        else:
            transfer_rate = 0
        remaining_size = os.path.getsize(backup_file)
        if transfer_rate != 0:  
            estimated_time = remaining_size / transfer_rate
        else:
            estimated_time = 0
        progress = (backup_size / backup_size) * 100
        print(f"Backup progress: {progress:.2f}%, estimated time remaining: {estimated_time:.2f} seconds")
        time.sleep(1)
        if progress >= 100:
            break
    print("Backup process has completed.")

def perform_backup(database_name, ticket):
    backup_file = take_psql_backup(database_name, ticket)
    if backup_file:
        print(f"Backup completed for ticket {ticket}.")
        backup_status_id = update_backup_status(ticket, datetime.now(), None, "Completed", os.path.getsize(backup_file))

def main():
    # Create the backup_status table if it doesn't exist
    create_backup_status_table()

    # Get the database name
    database_name = get_database_name()

    # Get the ticket number
    while True:
        if database_name:
            ticket = input("Enter the Ticket Number: ")
            if validate_ticket(ticket):
                if check_ticket_dir_in_backup(ticket):
                    print("Backup folder for this ticket already exists. Please enter a unique ticket number.")
                else:
                    break

    # Record the start time
    start_time = datetime.now()

    # Backup creation thread
    backup_thread = threading.Thread(target=perform_backup, args=(database_name, ticket))
    backup_thread.start()

    # Progress monitoring thread
    progress_thread = threading.Thread(target=backup_progress_monitor, args=(ticket, None))
    progress_thread.start()

    backup_thread.join()
    progress_thread.join()

def validate_backup_size(database_name, backup_file):
    try:
        # Check if the destination directory exists
        if not os.path.exists(dest_directory):
            print("Backup directory does not exist.")
            return False

        # Get available space in the backup directory
        avail_space = shutil.disk_usage(dest_directory).free

        # Get backup file size
        backup_size = os.path.getsize(backup_file)

        req_space = backup_size + (backup_size // 4)
        if avail_space < req_space:
            print("Insufficient storage in backup directory")
            return False
        return True
    except FileNotFoundError as e:
        print(f"Error validating backup size: {e}")
        return False

def take_psql_backup(database_name, ticket):
    backup_dir = os.path.join(dest_directory, ticket)
    os.makedirs(backup_dir, exist_ok=True)
    backup_file = os.path.join(backup_dir, f"backup_{ticket}.db")
    with open(backup_file, "wb") as f:
        try:
            subprocess.Popen(["pg_dump", "-d", database_name], stdout=f, stderr=subprocess.PIPE)
            return backup_file
        except subprocess.CalledProcessError as e:
            print(f"Error taking backup: {e}")
            return None

def update_backup_status(ticket, createdat, updatedat, status, backup_size):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("INSERT INTO backup_status(ticket_number, createdat, updatedat, database_name, hostname, status, backup_size) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                    (ticket, createdat, updatedat, PGDATABASE, PGHOST, status, backup_size))
        backup_status_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return backup_status_id
    except psycopg2.Error as e:
        print(f"Error updating backup status: {e}")
        return None

if __name__ == "__main__":
    main()

