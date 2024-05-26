import socket
import psycopg2
from psycopg2 import sql

# Database configurations for C1 and C2
# Database configurations for C1 asmae
DB_CONFIG_C1 = {
    'host': '10.126.14.44',
    'database': 'partition data base',
    'user': 'asmaemouradi',
    'password': 'password'
}
# Database configurations for C2 oumaima
DB_CONFIG_C2 = {
    'host': '10.126.16.74',
    'database': 'example_db',
    'user': 'partition data base',
    'password': 'your_password'
}

def get_partition_size(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name, pg_total_relation_size(table_name)
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        sizes = cur.fetchall()
        return sizes

def get_least_loaded_partition(conn1, conn2):
    size_c1 = get_partition_size(conn1)
    size_c2 = get_partition_size(conn2)

    # Assuming both partitions are the same on C1 and C2
    combined_sizes = {
        partition: size_c1.get(partition, 0) + size_c2.get(partition, 0)
        for partition in set(size_c1) | set(size_c2)
    }

    least_loaded_partition = min(combined_sizes, key=combined_sizes.get)
    return least_loaded_partition

def insert_data(conn, partition, data):
    with conn.cursor() as cur:
        insert_query = sql.SQL("INSERT INTO {} (data) VALUES (%s)").format(sql.Identifier(partition))
        cur.execute(insert_query, [data])
        conn.commit()

def main():
    data_to_insert = "example data"
    conn1 = psycopg2.connect(**DB_CONFIG_C1)
    conn2 = psycopg2.connect(**DB_CONFIG_C2)

    least_loaded_partition = get_least_loaded_partition(conn1, conn2)

    if sum(get_partition_size(conn1)[least_loaded_partition]) <= sum(get_partition_size(conn2)[least_loaded_partition]):
        insert_data(conn1, least_loaded_partition, data_to_insert)
    else:
        insert_data(conn2, least_loaded_partition, data_to_insert)

    conn1.close()
    conn2.close()

if __name__ == "__main__":
    main()
