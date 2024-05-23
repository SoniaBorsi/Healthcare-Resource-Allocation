import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine


def create_db(db_name, db_user, db_password):
    # Database connection parameters
    admin_db = "postgres"
    admin_user = "postgres"  # Assuming the superuser is 'postgres'
    admin_password = "postgres"  # Replace with your superuser password
    db_host = "localhost"
    db_port = "5432"

    # Connect to the PostgreSQL server (not to any specific database)
    conn = psycopg2.connect(
        dbname=admin_db,
        user=admin_user,
        password=admin_password,
        host=db_host,
        port=db_port
    )

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Create the new user
    try:
        cur.execute(f"CREATE USER {db_user} WITH PASSWORD '{db_password}'")
        print(f"User '{db_user}' created successfully.")
    except psycopg2.errors.DuplicateObject:
        print(f"User '{db_user}' already exists.")

    # Create the new database
    try:
        cur.execute(f"CREATE DATABASE {db_name} OWNER {db_user}")
        print(f"Database '{db_name}' created successfully.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database '{db_name}' already exists.")

    # Grant privileges
    cur.execute(f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {db_user}")
    print(f"Granted all privileges on database '{db_name}' to user '{db_user}'.")

    # Connection string for SQLAlchemy
    conn_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

    # Create an engine using SQLAlchemy
    engine = create_engine(conn_string)

    # Test the connection
    try:
        with engine.connect() as connection:
            print("Successfully connected to the database using SQLAlchemy.")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
    
    return(engine)