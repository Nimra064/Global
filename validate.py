import os

def get_file_size(file_path):
    
    return os.path.getsize(file_path)

def compare_backup_sizes(current_backup_file, last_backup_file):

    current_size = get_file_size(current_backup_file)
    last_size = get_file_size(last_backup_file)
    difference = last_size - current_size
    if difference / last_size > 0.05:
        print("Warning: Current backup file is more than 5% smaller than the last backup file. Backup may not have been successful.")
    else:
        print("Backup successful.")

# Example usage
current_backup_file = "PostsqlBackup/Backup/urlscrap_2024-05-03_03-55-33.sql"
last_backup_file = "SecondBackup/3/urlscrap_2024-05-03_03-58-47.sql"
compare_backup_sizes(current_backup_file, last_backup_file)
