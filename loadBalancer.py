import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError

# Database configurations for C1 and C2
DB_CONFIG_C1 = {
    'host': 'localhost',
    'database': 'partition data base',
    'user': 'asmaemouradi',
    'password': 'password'
}

DB_CONFIG_C2 = {
    'host': '10.126.16.74',
    'database': 'partition 2',
    'user': 'asmaemouradi',
    'password': 'password'
}

def get_partition_size(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name, pg_total_relation_size(table_name)
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name LIKE 'data_partition_%'
            """)
            sizes = cur.fetchall()
            return {table: size for table, size in sizes}
    except Exception as e:
        print(f"Error getting partition size: {e}")
        return {}

def get_least_loaded_partition(conn1, conn2):
    size_c1 = get_partition_size(conn1)
    size_c2 = get_partition_size(conn2)

    combined_sizes = {
        partition: size_c1.get(partition, 0) + size_c2.get(partition, 0)
        for partition in set(size_c1) | set(size_c2)
    }

    least_loaded_partition = min(combined_sizes, key=combined_sizes.get)
    return least_loaded_partition

def insert_data(conn, partition, data):
    try:
        with conn.cursor() as cur:
            insert_query = sql.SQL("INSERT INTO {} (info) VALUES (%s)").format(sql.Identifier(partition))
            cur.execute(insert_query, [data])
            conn.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")

def main(data_to_insert):
    try:
        conn1 = psycopg2.connect(**DB_CONFIG_C1)
        print("Connected to C1")
    except OperationalError as e:
        print(f"Error connecting to C1: {e}")
        return
    
    try:
        conn2 = psycopg2.connect(**DB_CONFIG_C2)
        print("Connected to C2")
    except OperationalError as e:
        print(f"Error connecting to C2: {e}")
        conn1.close()
        return

    least_loaded_partition = get_least_loaded_partition(conn1, conn2)
    print(f"Least loaded partition: {least_loaded_partition}")

    if sum(get_partition_size(conn1).get(least_loaded_partition, 0)) <= sum(get_partition_size(conn2).get(least_loaded_partition, 0)):
        print(f"Inserting data into {least_loaded_partition} on C1")
        insert_data(conn1, least_loaded_partition, data_to_insert)
    else:
        print(f"Inserting data into {least_loaded_partition} on C2")
        insert_data(conn2, least_loaded_partition, data_to_insert)

    conn1.close()
    conn2.close()

if __name__ == "__main__":
    # Example data to insert
    data_to_insert = "example data"
    main(data_to_insert)
