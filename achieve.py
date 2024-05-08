import os
import shutil
from datetime import datetime, timedelta
import psycopg2

# Function to connect to PostgreSQL database
def connect_to_database():
    try:
        conn = psycopg2.connect(
            dbname="automation",
            user="postgres",
            password="1234",
            host="localhost",
            port="5432"
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

# Function to create second_backup table if not exists
def create_second_backup_table(conn):
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS second_backup (
                id SERIAL PRIMARY KEY,
                folder_name VARCHAR(255),
                backup_name VARCHAR(255),
                backup_size BIGINT,
                backup_time TIMESTAMP
            )
        """)
        conn.commit()
        cur.close()
        print("second_backup table created successfully.")
    except psycopg2.Error as e:
        print(f"Error creating second_backup table: {e}")

# Function to insert record into second_backup table
def insert_into_second_backup(conn, folder_name, backup_name, backup_size):
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO second_backup (folder_name, backup_name, backup_size, backup_time)
            VALUES (%s, %s, %s, %s)
        """, (folder_name, backup_name, backup_size, datetime.now()))
        conn.commit()
        cur.close()
        print(f"Record inserted into second_backup table: {folder_name}, {backup_name}, {backup_size}")
    except psycopg2.Error as e:
        print(f"Error inserting record into second_backup table: {e}")

def move_old_backups():
    # Directory for main backups
    main_backup_directory = "BackupRecord"
    # Directory for second backups (older than 4 days)
    second_backup_directory = "SecondBackup"

    # Create the second backup directory if it doesn't exist
    os.makedirs(second_backup_directory, exist_ok=True)

    # Get the current date
    current_date = datetime.now()

    # Define the threshold date (4 days ago)
    threshold_date = current_date - timedelta(days=4)
    print("Threshold Date:", threshold_date)

    # Counter to track moved folders
    moved_folders_count = 0

    # Connect to the database
    conn = connect_to_database()
    if not conn:
        return

    # Create second_backup table if not exists
    create_second_backup_table(conn)

    # Iterate over each folder in the main backup directory
    for folder_name in os.listdir(main_backup_directory):
        folder_path = os.path.join(main_backup_directory, folder_name)
        if os.path.isdir(folder_path):
            # Check if the folder contains a backup file
            backup_files = [f for f in os.listdir(folder_path) if f.endswith('.sql')]
            if backup_files:
                # Get the modification time of the backup file
                backup_file = os.path.join(folder_path, backup_files[0])
                modification_time = datetime.fromtimestamp(os.path.getmtime(backup_file))
                print(f"Folder: {folder_name}, Last Modified: {modification_time}")
                # Check if the backup file is older than 4 days
                if modification_time < threshold_date:
                    # Insert record into second_backup table
                    backup_size = os.path.getsize(backup_file)
                    insert_into_second_backup(conn, folder_name, backup_files[0], backup_size)
                    # Move the entire folder to the second backup directory
                    destination_folder = os.path.join(second_backup_directory, folder_name)
                    try:
                        shutil.move(folder_path, destination_folder)
                        print(f"Moved '{folder_name}' to second backup directory.")
                        moved_folders_count += 1
                    except Exception as e:
                        print(f"Error moving '{folder_name}': {e}")
                else:
                    print(f"'{folder_name}' is not older than 4 days.")

    print(f"Backup move process completed. {moved_folders_count} folders moved to SecondBackup.")

    # Close database connection
    conn.close()

if __name__ == "__main__":
    move_old_backups()



