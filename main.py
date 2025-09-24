import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_valid_timestamp(timestamp):
    try:
        datetime.fromisoformat(timestamp)
        return True
    except ValueError:
        return False

def is_valid_sensor_name(device_name):
    return isinstance(device_name, str) and device_name.strip() != ''

def is_valid_value(value):
    return isinstance(value, float)

def upload_to_mysql(csv_file_path, db, table_name):
    try:
        df = pd.read_csv(csv_file_path)
        
        filtered_df = df[df['timestamp'].apply(lambda x: is_valid_timestamp(x))]
        filtered_df = df[df['sensorName'].apply(lambda x: is_valid_sensor_name(x))]
        filtered_df = df[df['value'].apply(lambda x: is_valid_value(x))]
      
        # Create database connection
        engine = create_engine(
            f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        # Upload to MySQL
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        logging.info(f"Successfully uploaded {len(df)} rows from {csv_file_path} to {table_name}")

        # Check if all rows are valid
        assert len(df) == len(filtered_df), "Not all rows are valid"

        return True
        
    except Exception as e:
        logging.error(f"Error processing {csv_file_path}: {e}")

        return False

def process_csv_files(local_dir, db, table_name):
    try:
        # Check if directory exists
        if not os.path.isdir(local_dir):
            raise Exception(f"Directory does not exist: {local_dir}")
        
        # Iterate through all files in the directory
        all_completed = True
        for filename in os.listdir(local_dir):
            if filename.lower().endswith('.csv'):
                file_path = os.path.join(local_dir, filename)
                logging.info(f"Processing {file_path}...")
                status = upload_to_mysql(file_path, db, table_name)
                if not status:
                    all_completed = False

        if not all_completed:
            raise Exception("The uploading process is not fully completed")

        logging.info("The uploading process is fully completed")
                
    except Exception as e:
        logging.error(f"Error accessing directory {local_dir}: {e}")
        raise Exception(e)

def main():
    parser = argparse.ArgumentParser(description='Upload CSV files to MySQL')
    parser.add_argument('--local_dir', required=True, help='Directory containing CSV files')
    parser.add_argument('--db', required=True, help='Target MySQL database')
    parser.add_argument('--table_name', required=True, help='Target MySQL table name')
    args = parser.parse_args()
    
    # Process CSV files
    process_csv_files(args.local_dir, args.db, args.table_name)

if __name__ == '__main__':
    main()
