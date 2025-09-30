import duckdb
import os
import logging

# Setup logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=os.path.join("logs", "clean.log"),
    filemode="a"
)
logger = logging.getLogger(__name__)

def get_connection(db_path: str = "emissions.duckdb"):
    """Establishes a connection to a DuckDB database, logs success."""
    try:
        con = duckdb.connect(db_path)
        logger.info(f"Connected to DuckDB: {db_path}")
        return con
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise


def get_datetime_columns(table: str):
    """Returns the correct pickup and dropoff datetime column names depending on whether the table is yellow or green taxi data."""
    if table == "yellow":
        return "tpep_pickup_datetime", "tpep_dropoff_datetime"
    else:
        return "lpep_pickup_datetime", "lpep_dropoff_datetime"

def execute_cleanup(con, table: str, sql: str, description: str):
    """Executes a SQL cleanup operation on a table, calculates how many rows were removed, and logs the result."""
    try:
        before_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        con.execute(sql)
        after_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        removed = before_count - after_count
        
        message = f"{description}: {removed:,} trips removed from {table}"
        print(message)
        logger.info(message)
        return removed
    except Exception as e:
        logger.error(f"Error in {description.lower()}: {e}")
        raise

def remove_duplicates(con, table: str):
    """Removes duplicate taxi trips based on key trip fields by rebuilding the table with only distinct records."""
    pickup_col, dropoff_col = get_datetime_columns(table)
    
    # Create a temporary table with unique trips
    temp_table = f"{table}_unique"
    
    sql = f"""
        CREATE OR REPLACE TABLE {temp_table} AS
        SELECT DISTINCT {pickup_col}, {dropoff_col}, PULocationID, DOLocationID, 
               trip_distance, passenger_count
        FROM {table}
    """
    
    try:
        con.execute(sql)
        logger.info(f"Created temporary unique table: {temp_table}")
    except Exception as e:
        logger.error(f"Error creating unique table: {e}")
        raise
    
    # Get counts before and after
    before_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    
    # Drop original table and rename temp table
    con.execute(f"DROP TABLE {table}")
    con.execute(f"ALTER TABLE {temp_table} RENAME TO {table}")
    
    after_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    removed = before_count - after_count
    
    message = f"TABLES UPDATED TO REMOVE DUPLICATE TRIPS: {removed:,} duplicates removed from {table}"
    print(message)
    logger.info(message)
    
    return removed



def remove_zero_passengers(con, table: str):
    """Deletes all trips with zero passengers from the given table."""
    sql = f"DELETE FROM {table} WHERE passenger_count = 0"
    return execute_cleanup(con, table, sql, "TABLES UPDATED TO REMOVE TRIPS WITH 0 PASSENGERS")

def remove_zero_distance(con, table: str):
    """Deletes trips that have a recorded distance of zero miles."""
    sql = f"DELETE FROM {table} WHERE trip_distance = 0"
    return execute_cleanup(con, table, sql, "TABLES UPDATED TO REMOVE TRIPS 0 MILES IN LENGTH")

def remove_long_distance(con, table: str):
    """Removes trips that are longer than 100 miles, assuming these are data errors or outliers."""
    sql = f"DELETE FROM {table} WHERE trip_distance > 100"
    return execute_cleanup(con, table, sql, "TABLES UPDATED TO REMOVE TRIPS GREATER THAN 100 MILES IN LENGTH")

def remove_long_duration(con, table: str):
    """Removes trips lasting longer than 24 hours based on pickup and dropoff timestamps."""
    pickup_col, dropoff_col = get_datetime_columns(table)
    
    sql = f"""
        DELETE FROM {table}
        WHERE EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col})) > 86400
    """
    return execute_cleanup(con, table, sql, "TABLES UPDATED TO REMOVE TRIPS GREATER THAN 24 HOURS IN LENGTH")

def clean_table(con, table: str):
    """Runs all cleaning steps (duplicates, zero passengers, zero distance, long trips, long durations) and reports before/after counts."""
    print(f"\n=== CLEANING TABLE: {table.upper()} ===")
    logger.info(f"Starting cleaning for: {table}")
    
    initial_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"Initial count: {initial_count:,} trips")
    
    # Execute all cleaning operations
    total_removed = (
        remove_duplicates(con, table) +
        remove_zero_passengers(con, table) +
        remove_zero_distance(con, table) +
        remove_long_distance(con, table) +
        remove_long_duration(con, table)
    )
    
    final_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    
    print(f"\n=== CLEANING COMPLETE FOR {table.upper()} ===")
    print(f"Total trips removed: {total_removed:,}")
    print(f"Final trip count: {final_count:,}")
    print(f"Data reduction: {(total_removed/initial_count)*100:.1f}%")
    
    logger.info(f"Cleaning complete for {table}: {total_removed:,} removed, {final_count:,} remaining")

def verify_cleaning(con, table: str):
    """Runs validation tests to confirm that all cleaning rules have been enforced (no duplicates, no zero passengers, etc.)."""
    print(f"\n=== VERIFICATION TESTS FOR {table.upper()} ===")
    logger.info(f"Running verification for: {table}")
    
    pickup_col, dropoff_col = get_datetime_columns(table)
    
    # Define test queries and descriptions
    tests = [
        {
            "name": "Duplicate trips",
            "sql": f"""
                SELECT COUNT(*) - COUNT(*) FROM (
                    SELECT DISTINCT {pickup_col}, {dropoff_col}, 
                    PULocationID, DOLocationID, trip_distance, passenger_count
                    FROM {table}
                )
            """,
            "expected": 0
        },
        {
            "name": "Zero passenger trips", 
            "sql": f"SELECT COUNT(*) FROM {table} WHERE passenger_count = 0",
            "expected": 0
        },
        {
            "name": "Zero mile trips",
            "sql": f"SELECT COUNT(*) FROM {table} WHERE trip_distance = 0",
            "expected": 0
        },
        {
            "name": "Trips > 100 miles",
            "sql": f"SELECT COUNT(*) FROM {table} WHERE trip_distance > 100",
            "expected": 0
        },
        {
            "name": "Trips > 24 hours",
            "sql": f"""
                SELECT COUNT(*) FROM {table} 
                WHERE EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col})) > 86400
            """,
            "expected": 0
        }
    ]
    
    # Run all tests
    all_passed = True
    for i, test in enumerate(tests, 1):
        try:
            count = con.execute(test["sql"]).fetchone()[0]
            result = "PASS" if count == test["expected"] else "FAIL"
            message = f"Test {i} - {test['name']}: {result} ({count} found)"
            print(message)
            logger.info(message)
            
            if count != test["expected"]:
                all_passed = False
        except Exception as e:
            print(f"Test {i} - {test['name']}: ERROR - {e}")
            logger.error(f"Test {i} failed: {e}")
            all_passed = False
    
    # Overall result
    overall_result = "ALL TESTS PASSED" if all_passed else "SOME TESTS FAILED"
    print(f"\n=== OVERALL RESULT: {overall_result} ===")
    logger.info(f"Verification result for {table}: {overall_result}")
    
    return all_passed

def table_exists(con, table: str):
    """Checks whether a given table exists in the DuckDB database."""
    try:
        count = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name = '{table}'
        """).fetchone()[0]
        return count > 0
    except:
        return False

def main():
    """Main entry point that runs the cleaning and verification process for yellow and green taxi tables, with logging and error handling."""
    try:
        print("=" * 60)
        print("STARTING DATA CLEANING PROCESS")
        print("=" * 60)
        logger.info("Starting data cleaning process")
        
        con = get_connection()
        tables = ["yellow", "green"]
        
        for table in tables:
            if not table_exists(con, table):
                print(f"Table '{table}' does not exist, skipping...")
                continue
            
            # Clean the table
            clean_table(con, table)
            
            # Verify cleaning
            verify_cleaning(con, table)
        
        print("\n" + "=" * 60)
        print("DATA CLEANING PROCESS COMPLETED")
        print("=" * 60)
        logger.info("Data cleaning process completed")
        
    except Exception as e:
        print(f"Fatal error: {e}")
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        if 'con' in locals():
            con.close()
            print("Database connection closed")

if __name__ == "__main__":
    main()
