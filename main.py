import pandas as pd
from numpy import nan
from requests import get
from psycopg2 import connect, DatabaseError
from psycopg2.extras import execute_batch
from config import SQL_PARAMS, API_ENDPOINT
from math import ceil
from time import sleep, localtime, strftime
from os import mkdir
from json import dumps
from shutil import rmtree


def drop_view(view_name, cur):
    """Deletes a view when specifying its name"""
    if view_name == 'leading_country':
        cur.execute("""DROP VIEW IF EXISTS leading_country_per_year_vw""")
    elif view_name == 'rocket_family_stats':
        cur.execute("""DROP VIEW IF EXISTS rocket_family_stats_vw""")
    print(f"View {view_name} deleted successfully")


def drop_index(index_name, cur):
    """Deletes an index when specifying its name"""
    if index_name == 'rocket':
        cur.execute("DROP INDEX IF EXISTS rocket_id_idx")
    elif index_name == 'mission':
        cur.execute("DROP INDEX IF EXISTS mission_id_idx")
    elif index_name == 'location':
        cur.execute("DROP INDEX IF EXISTS location_id_idx")
    elif index_name == 'pad':
        cur.execute("DROP INDEX IF EXISTS pad_id_idx")
    elif index_name == 'status':
        cur.execute("DROP INDEX IF EXISTS status_id_idx")
    elif index_name == 'launch':
        cur.execute("DROP INDEX IF EXISTS launch_id_idx")
    print(f"Index {index_name} deleted successfully")


def drop_table(table_name, cur):
    """Drops a table when specifying its name"""
    if table_name == 'rocket':
        cur.execute("DROP TABLE IF EXISTS rocket CASCADE")

    elif table_name == 'mission':
        cur.execute("DROP TABLE IF EXISTS mission CASCADE")

    elif table_name == 'location':
        cur.execute("DROP TABLE IF EXISTS location CASCADE")

    elif table_name == 'pad':
        cur.execute("DROP TABLE IF EXISTS pad CASCADE")

    elif table_name == 'launch':
        cur.execute("DROP TABLE IF EXISTS launch CASCADE")

    elif table_name == 'status':
        cur.execute("DROP TABLE IF EXISTS status CASCADE")
    print(f"Table {table_name} deleted successfully")


def create_table(table_name, cur):
    """Creates a table when specifying its name"""
    if table_name == 'rocket':
        cur.execute("""
        CREATE TABLE rocket
        (
        rocket_id INT PRIMARY KEY,
        rocket_config_id INT,
        rocket_name VARCHAR(255),
        rocket_family VARCHAR(255),
        rocket_variant VARCHAR(255)
        );
        """)

    elif table_name == 'mission':
        cur.execute("""
        CREATE TABLE mission
        (
        mission_id INT PRIMARY KEY,
        mission_name VARCHAR(255),
        type VARCHAR(255),
        orbit_name VARCHAR(255),
        mission_description TEXT
        );
        """)

    elif table_name == 'location':
        cur.execute("""
        CREATE TABLE location
        (
        location_id INT PRIMARY KEY,
        location_name VARCHAR (255),
        country_code VARCHAR (5),
        total_launch_count INT,
        total_landing_count INT
        )
        """)

    elif table_name == 'pad':
        cur.execute("""
        CREATE TABLE pad
        (
        pad_id INT PRIMARY KEY,
        location_id INT,
        pad_name VARCHAR (255),
        latitude REAL,
        longitude REAL
        );
        """)

    elif table_name == 'status':
        cur.execute("""
        CREATE TABLE status
        (
        status_id INT PRIMARY KEY,
        status_name VARCHAR(255),
        status_description TEXT
        );
        """)

    elif table_name == 'launch':
        cur.execute("""
        CREATE TABLE launch
        (
        launch_id serial PRIMARY KEY,
        launch_name VARCHAR(255),
        rocket_id INT REFERENCES rocket (rocket_id),
        mission_id INT NULL REFERENCES mission (mission_id) ON DELETE SET NULL,
        location_id INT REFERENCES location (location_id),
        pad_id INT REFERENCES pad (pad_id),
        status_id INT REFERENCES status (status_id),
        launch_time TIMESTAMP,
        last_updated_api TIMESTAMP,
        last_updated_db TIMESTAMP
        );
        """)
    print(f"Table {table_name} created successfully")


def create_df(df_name, original_dataframe):
    """Creates a dataframe when we specify its name using the original dataframe built upon a JSON file"""
    if df_name == 'rocket_df':
        df = original_dataframe[
            ['rocket.id', 'rocket.configuration.id', 'rocket.configuration.full_name',
             'rocket.configuration.family', 'rocket.configuration.variant']]

    elif df_name == 'mission_df':
        df = original_dataframe[
            ['mission.id', 'mission.name', 'mission.type', 'mission.orbit.name', 'mission.description']]

    elif df_name == 'location_df':
        df = original_dataframe[
            ['pad.location.id', 'pad.location.name', 'pad.location.country_code',
             'pad.location.total_launch_count', 'pad.location.total_landing_count']]

    elif df_name == 'pad_df':
        df = original_dataframe[['pad.id', 'pad.location.id', 'pad.name', 'pad.latitude', 'pad.longitude']]

    elif df_name == 'status_df':
        df = original_dataframe[['status.id', 'status.name', 'status.description']]

    elif df_name == 'launch_df':
        df = original_dataframe[['name', 'rocket.id', 'mission.id', 'pad.location.id',
                                 'pad.id', 'status.id', 'net', 'last_updated']]

    print(f"Dataframe {df_name} created successfully")
    return df


def transform_df(df_name, df):
    """
        1. Replaces empty strings and NaNs with None so postgresql reads them as NULL;
        2. Renames columns of dataframes;
        3. Drops None values when needed;
        4. Adds 'last_updated_db' column to launch table
    """
    df = df.replace({nan: None})
    df = df.replace({'': None})

    if df_name == 'rocket_df':
        df = df.rename(columns={
            'rocket.id': 'rocket_id',
            'rocket.configuration.id': 'rocket_config_id',
            'rocket.configuration.full_name': 'rocket_name',
            'rocket.configuration.family': 'rocket_family',
            'rocket.configuration.variant': 'rocket_variant'
        })

    elif df_name == 'mission_df':
        df = df.rename(
            columns={
                'mission.id': 'mission_id',
                'mission.name': 'mission_name',
                'mission.type': 'type',
                'mission.orbit.name': 'orbit_name',
                'mission.description': 'mission_description'
            }
        )
        df = df.dropna(subset=['mission_id'])

    elif df_name == 'location_df':
        df = df.rename(columns={
            'pad.location.id': 'location_id',
            'pad.location.name': 'location_name',
            'pad.location.country_code': 'country_code',
            'pad.location.total_launch_count': 'total_launch_count',
            'pad.location.total_landing_count': 'total_landing_count'
        })

    elif df_name == 'pad_df':
        df = df.rename(columns={
            'pad.id': 'pad_id',
            'pad.name': 'pad_name',
            'pad.location.id': 'location_id',
            'pad.latitude': 'latitude',
            'pad.longitude': 'longitude'
        })

    elif df_name == 'status_df':
        df = df.rename(columns={
            'status.id': 'status_id',
            'status.name': 'status_name',
            'status.description': 'status_description'
        })

    elif df_name == 'launch_df':
        df = df.rename(columns={
            'name': 'launch_name',
            'rocket.id': 'rocket_id',
            'mission.id': 'mission_id',
            'pad.location.id': 'location_id',
            'pad.id': 'pad_id',
            'status.id': 'status_id',
            'net': 'time',
            'last_updated': 'last_updated_api'
        })
        df.insert(8, 'last_updated_db', pd.Timestamp('now').ceil(freq='s'))

    print(f"Dataframe {df_name} transformed successfully")
    return df


def inserting_data(table_name, dataframe, conn, cur):
    """Inserts data using tuples (rows) """

    try:
        tuples = [tuple(x) for x in dataframe.to_numpy()]

        if table_name == 'rocket':
            statement = """
            INSERT INTO rocket
            (rocket_id, rocket_config_id, rocket_name,rocket_family,rocket_variant)
            VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
            """
            execute_batch(cur, statement, tuples, page_size=100)

        elif table_name == 'mission':
            statement = """
            INSERT INTO mission
            (mission_id, mission_name, type, orbit_name, mission_description)
            VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
            """
            execute_batch(cur, statement, tuples, page_size=100)

        elif table_name == 'location':
            statement = """
            INSERT INTO location
            (location_id, location_name, country_code, total_launch_count, total_landing_count)
            VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
            """
            execute_batch(cur, statement, tuples, page_size=100)

        elif table_name == 'pad':
            statement = """
            INSERT INTO pad
            (pad_id, location_id, pad_name, latitude, longitude)
            VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
            """
            execute_batch(cur, statement, tuples, page_size=100)

        elif table_name == 'status':
            statement = """
            INSERT INTO status
            (status_id, status_name, status_description)
            VALUES (%s,%s,%s) ON CONFLICT DO NOTHING;
            """
            execute_batch(cur, statement, tuples, page_size=100)

        elif table_name == 'launch':
            statement = """
            INSERT INTO launch
            (launch_name, rocket_id, mission_id, location_id, pad_id, status_id, 
            launch_time, last_updated_api, last_updated_db)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;
            """
            execute_batch(cur, statement, tuples, page_size=100)

        print(f"Data was inserted to {table_name} table successfully\n")
    except (Exception, DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()


def rocket_function(conn, cur, original_dataframe, drop_sql_table=False, create_sql_table=False, create_dataframe=False,
                    transform_dataframe=False, insert_data=False):
    """Executes all steps needed for rocket table"""
    if drop_sql_table:
        drop_table('rocket', cur)
    if create_sql_table:
        create_table('rocket', cur)
    if create_dataframe:
        rocket_df = create_df('rocket_df', original_dataframe)
    if transform_dataframe:
        transformed_df = transform_df('rocket_df', rocket_df)
    if insert_data:
        inserting_data('rocket', transformed_df, conn, cur)


def mission_function(conn, cur, original_dataframe, drop_sql_table=False, create_sql_table=False,
                     create_dataframe=False,
                     transform_dataframe=False, insert_data=False):
    """Executes all steps needed for mission table"""
    if drop_sql_table:
        drop_table('mission', cur)
    if create_sql_table:
        create_table('mission', cur)
    if create_dataframe:
        mission_df = create_df('mission_df', original_dataframe)
    if transform_dataframe:
        transformed_df = transform_df('mission_df', mission_df)
    if insert_data:
        inserting_data('mission', transformed_df, conn, cur)


def location_function(conn, cur, original_dataframe, drop_sql_table=False, create_sql_table=False,
                      create_dataframe=False,
                      transform_dataframe=False, insert_data=False):
    """Executes all steps needed for location table"""
    if drop_sql_table:
        drop_table('location', cur)
    if create_sql_table:
        create_table('location', cur)
    if create_dataframe:
        location_df = create_df('location_df', original_dataframe)
    if transform_dataframe:
        transformed_df = transform_df('location_df', location_df)
    if insert_data:
        inserting_data('location', transformed_df, conn, cur)


def pad_function(conn, cur, original_dataframe, drop_sql_table=False, create_sql_table=False, create_dataframe=False,
                 transform_dataframe=False, insert_data=False):
    """Executes all steps needed for pad table"""
    if drop_sql_table:
        drop_table('pad', cur)
    if create_sql_table:
        create_table('pad', cur)
    if create_dataframe:
        pad_df = create_df('pad_df', original_dataframe)
    if transform_dataframe:
        transformed_df = transform_df('pad_df', pad_df)
    if insert_data:
        inserting_data('pad', transformed_df, conn, cur)


def status_function(conn, cur, original_dataframe, drop_sql_table=False, create_sql_table=False, create_dataframe=False,
                    transform_dataframe=False, insert_data=False):
    """Executes all steps needed for status table"""
    if drop_sql_table:
        drop_table('status', cur)
    if create_sql_table:
        create_table('status', cur)
    if create_dataframe:
        status_df = create_df('status_df', original_dataframe)
    if transform_dataframe:
        transformed_df = transform_df('status_df', status_df)
    if insert_data:
        inserting_data('status', transformed_df, conn, cur)


def launch_function(conn, cur, original_dataframe, drop_sql_table=False, create_sql_table=False, create_dataframe=False,
                    transform_dataframe=False, insert_data=False):
    """Executes all steps needed for launch table"""
    if drop_sql_table:
        drop_table('launch', cur)
    if create_sql_table:
        create_table('launch', cur)
    if create_dataframe:
        launch_df = create_df('launch_df', original_dataframe)
    if transform_dataframe:
        transformed_df = transform_df('launch_df', launch_df)
    if insert_data:
        inserting_data('launch', transformed_df, conn, cur)


def create_index(index_name, cur):
    """Creates indexes for each Primary Key when specifying a table's name"""
    if index_name == 'rocket':
        cur.execute("CREATE INDEX rocket_id_idx ON rocket (rocket_id)")
    elif index_name == 'mission':
        cur.execute("CREATE INDEX mission_id_idx ON mission (mission_id)")
    elif index_name == 'location':
        cur.execute("CREATE INDEX location_id_idx ON location (location_id)")
    elif index_name == 'pad':
        cur.execute("CREATE INDEX pad_id_idx ON pad (pad_id)")
    elif index_name == 'launch':
        cur.execute("CREATE INDEX launch_id_idx ON launch (launch_id)")
    elif index_name == 'status':
        cur.execute("CREATE INDEX status_id_idx ON status (status_id)")
    print(f"Index {index_name} created successfully")


def create_view(view_name, cur):
    """Creates a view when specifying its name"""
    if view_name == 'leading_country':
        cur.execute("""
        CREATE VIEW leading_country_per_year_vw
        (year, total_launches, leading_country)
        AS
        SELECT 
            EXTRACT(YEAR from la.launch_time) AS year, 
            count(*) AS total_launches, 
            lo.country_code AS leading_country
        FROM launch la
        INNER JOIN location lo ON lo.location_id = la.location_id
        INNER JOIN (
            SELECT 
            EXTRACT(YEAR from launch_time) AS year, 
            location_id, 
            ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR from launch_time) ORDER BY count(*) DESC) AS row_num
            FROM launch
            WHERE status_id IN (3, 4, 7)
            GROUP BY 1, 2
        ) loc_count ON loc_count.year = EXTRACT(YEAR from la.launch_time) 
        AND loc_count.location_id = la.location_id AND loc_count.row_num = 1
        WHERE status_id IN (3, 4, 7)
        GROUP BY 1, 3
        ORDER BY 1 ASC
        """)
    elif view_name == 'rocket_family_stats':
        cur.execute("""
        CREATE VIEW rocket_family_stats_vw 
        (rocket_family, total_launches, successful_launches, failed_launches, success_rate) 
        AS
        SELECT r.rocket_family,
        count(la.*) FILTER (WHERE la.status_id IN (3, 4, 7)) AS total_launches,

        count(la.*) FILTER (WHERE la.status_id = 3) AS successful_launches,

        count(la.*) FILTER (WHERE la.status_id IN (4, 7)) AS failed_launches,

        count(la.*) FILTER (WHERE la.status_id = 3) * 100 / NULLIF(count(la.*) FILTER 
        (WHERE la.status_id IN (3, 4, 7)), 0) AS success_rate

        FROM rocket r
        JOIN launch la ON r.rocket_id = la.rocket_id
        GROUP BY r.rocket_family
        ORDER BY 2 DESC
        """)
        print(f"View {view_name} created successfully")


def get_db_conn():
    """Establishes connection to the postgres database"""
    conn = connect(
        user=SQL_PARAMS['user'],
        password=SQL_PARAMS['password'],
        host=SQL_PARAMS['host'],
        port=SQL_PARAMS['port'],
        dbname=SQL_PARAMS['database']
    )
    return conn


def create_json_file(iterations_done, data):
    """Creates a JSON file for each iteration"""
    folder_path = './json_files'
    if iterations_done == 0:
        rmtree(folder_path, ignore_errors=True)
        mkdir(folder_path)
        print(f'Folder {folder_path} created successfully')
    # Creates a name for a file
    first_id = (iterations_done + 1) * 100 - 99
    last_id = (iterations_done + 1) * 100

    file_path = f'{folder_path}/ids_{first_id}-{last_id}.json'
    with open(file_path, 'w') as f:
        f.write(dumps(data))


def main():
    """Handles the whole process"""
    api_endpoint = API_ENDPOINT
    r = get(api_endpoint)
    data = r.json()
    it_planned = ceil(data['count'] / 100)  # Number of iterations needed to fully populate the tables
    it_done = 0
    while it_done < it_planned:

        r = get(api_endpoint)
        print(api_endpoint)
        print(r.raise_for_status())
        data = r.json()
        original_df = pd.json_normalize(data['results'])  # Creates a normalized ('flat') JSON

        create_json_file(it_done, data)
        try:
            conn = get_db_conn()
            cur = conn.cursor()

            if it_done == 0:  # On the first iteration creates tables populates them, deletes indexes and views
                for index in ['rocket', 'mission', 'location', 'pad', 'status', 'launch']:
                    drop_index(index, cur)
                drop_view('leading_country', cur)
                drop_view('rocket_family_stats', cur)
                rocket_function(conn, cur, original_df, True, True, True, True, True)
                mission_function(conn, cur, original_df, True, True, True, True, True)
                location_function(conn, cur, original_df, True, True, True, True, True)
                pad_function(conn, cur, original_df, True, True, True, True, True)
                status_function(conn, cur, original_df, True, True, True, True, True)
                launch_function(conn, cur, original_df, True, True, True, True, True)
            else:  # The same as above without dropping tables, indexes, views and creating SQL tables
                rocket_function(conn, cur, original_df, False, False, True, True, True)
                mission_function(conn, cur, original_df, False, False, True, True, True)
                location_function(conn, cur, original_df, False, False, True, True, True)
                pad_function(conn, cur, original_df, False, False, True, True, True)
                status_function(conn, cur, original_df, False, False, True, True, True)
                launch_function(conn, cur, original_df, False, False, True, True, True)

            if it_done == it_planned - 1:  # After DB population it creates indexes and views.
                create_index('rocket', cur)
                create_index('mission', cur)
                create_index('location', cur)
                create_index('pad', cur)
                create_index('launch', cur)
                create_index('status', cur)

                create_view('leading_country', cur)
                create_view('rocket_family_stats', cur)

        except Exception as e:
            print(e)
        finally:
            if conn is not None:
                conn.commit()
                api_endpoint = data['next']
                it_done += 1
                print(f"ITERATIONS DONE: {it_done}")
                conn.close()
                print("Connection closed.")
                sleep(250)  # Skips time because of API requests limitation (15 requests / hour)


main()
current_time = strftime("%H:%M:%S", localtime())
print(f"DATABASE POPULATION HAS BEEN COMPLETED\nTIME: {current_time}")
