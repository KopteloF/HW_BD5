import psycopg2
import json

def create_db(conn):
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            phones VARCHAR(255)[]
        )
        """)
        conn.commit()

def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cursor:
        cursor.execute("""
        INSERT INTO clients (first_name, last_name, email, phones)
        VALUES (%s, %s, %s, %s)
        """, (first_name, last_name, email, phones))
        conn.commit()

def add_phone(conn, client_id, phone):
    with conn.cursor() as cursor:
        cursor.execute("""
        UPDATE clients
        SET phones = array_append(phones, %s)
        WHERE id = %s
        """, (phone, client_id))
        conn.commit()

def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    updates = []
    if first_name:
        updates.append(f"first_name = %s")
    if last_name:
        updates.append(f"last_name = %s")
    if email:
        updates.append(f"email = %s")
    if phones:
        updates.append(f"phones = %s")

    if updates:
        query = f"""
        UPDATE clients
        SET {', '.join(updates)}
        WHERE id = %s
        """
        params = []
        if first_name:
            params.append(first_name)
        if last_name:
            params.append(last_name)
        if email:
            params.append(email)
        if phones:
            params.append(phones)
        params.append(client_id)

        with conn.cursor() as cursor:
            cursor.execute(query, tuple(params))
            conn.commit()

def delete_phone(conn, client_id, phone):
    with conn.cursor() as cursor:
        cursor.execute("""
        UPDATE clients
        SET phones = array_remove(phones, %s)
        WHERE id = %s
        """, (phone, client_id))
        conn.commit()

def delete_client(conn, client_id):
    with conn.cursor() as cursor:
        cursor.execute("""
        DELETE FROM clients
        WHERE id = %s
        """, (client_id,))
        conn.commit()

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    query = """
    SELECT * FROM clients
    WHERE 1=1
    """
    params = []

    if first_name:
        query += "AND first_name = %s "
        params.append(first_name)
    if last_name:
        query += "AND last_name = %s "
        params.append(last_name)
    if email:
        query += "AND email = %s "
        params.append(email)
    if phone:
        query += "AND %s = ANY(phones) "
        params.append(phone)

    with conn.cursor() as cursor:
        cursor.execute(query, tuple(params))
        return cursor.fetchall()

def add_clients_from_json(conn, filename):
    with open(filename, 'r') as file:
        clients_data = json.load(file)
        for client_data in clients_data:
            email = client_data.get('email')
            first_name = client_data.get('name')
            last_name = client_data.get('surname')
            phones = client_data.get('phone', [])
            add_client(conn, first_name, last_name, email, phones)

# Соединение с базой данных и вызов функций
with psycopg2.connect(database="adressbook", user="postgres", password="Oskar0908And") as conn:
    create_db(conn)
    add_clients_from_json(conn, 'data.json')
    found_clients = find_client(conn, last_name="Izmailov")

    print("Найденные клиенты:")
    for client in found_clients:
        print(client)

conn.close()