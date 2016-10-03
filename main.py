import csv
import psycopg2

connection = psycopg2.connect("dbname=movie_db user=movie_db")
cursor = connection.cursor()

cursor.execute("DROP TABLE IF EXISTS public.item_info;")
cursor.execute("DROP TABLE IF EXISTS public.user_info;")
cursor.execute("DROP TABLE IF EXISTS public.data_info;")

create_item_table_command = """
    CREATE TABLE public.item_info (
        movie_id SERIAL PRIMARY KEY NOT NULL,
        title VARCHAR(100),
        release_date VARCHAR(20),
        video_release_date VARCHAR(20),
        imdb_url VARCHAR(200),
        unknown INT,
        action INT,
        adventure INT,
        animation INT,
        childrens INT,
        comedy INT,
        crime INT,
        documentary INT,
        drama INT,
        fantasy INT,
        film_noir INT,
        horror INT,
        musical INT,
        mystery INT,
        romance INT,
        sci_fi INT,
        thriller INT,
        war INT,
        western INT
);
CREATE UNIQUE INDEX item_info_movie_id_uindex ON public.item_info (movie_id);
"""

create_user_table_command = """
    CREATE TABLE public.user_info (
        user_id SERIAL PRIMARY KEY NOT NULL,
        age INT,
        gender VARCHAR(20),
        occupation VARCHAR(50),
        zip_code VARCHAR(10)
    );
    CREATE UNIQUE INDEX user_info_user_id_uindex ON public.user_info (user_id);
"""

cursor.execute(create_item_table_command)
cursor.execute(create_user_table_command)


# with open('data.csv') as data_file:
#     data = csv.reader(data_file, delimiter=' ')

with open('item.csv') as item_file:
    item = csv.reader(item_file, delimiter='|')
    for row in item:
        cursor.execute("INSERT INTO public.item_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (row[:]))
connection.commit()

with open('user.csv') as user_file:
    user = csv.reader(user_file, delimiter='|')
    for row in user:
        cursor.execute("INSERT INTO public.user_info VALUES (%s,%s,%s,%s,%s)", (row[:]))
connection.commit()






cursor.close()
connection.close()
