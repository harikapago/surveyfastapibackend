import psycopg2
import traceback
from settings import *
import os

#Class for the  DATABASE
class pg_database:

    def __init__(self):

        try:
            self.conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
            print('connection')
        except Exception as e:
            print(f'Error in making connection to postgresql Database.:{repr(e)}')
            traceback.print_exc()
        try:
            self.curr = self.conn.cursor()
            print('database')
        except Exception as e:
            print(f'Error in initializing an postgres database instance.:{repr(e)}')
            traceback.print_exc()

    def create_tables(self, create_command):

        #create_command = CREATE TABLE audio_files (id SERIAL PRIMARY KEY,audio_bytes BYTEA NOT NULL);
        try:
            self.curr.execute(create_command)
            self.conn.commit()
            print('The given table is created')

        except Exception as e:
            print(f'Error in creating a table:{repr(e)}')
            traceback.print_exc()
        
        finally:
            if self.curr is not None:
                self.curr.close()
            if self.conn is not None:
                self.conn.close()

    def upload_questions(self, audio_files_folder_path):

        try:
            for i, audio_file in enumerate(os.listdir(audio_files_folder_path)):
                file = os.path.join(audio_files_folder_path, audio_file)
                f = open(file, 'rb')
                self.curr.execute(f"INSERT INTO audio_files (audio_bytes) VALUES (%s)", (f.read(),))
                self.conn.commit()

        except Exception as e:
            print(f'Error in inserting questions:{repr(e)}')
            traceback.print_exc()
        
        finally:
            if self.curr is not None:
                self.curr.close()
            if self.conn is not None:
                self.conn.close()

    def get_data(self, table_name):

        self.curr.execute(f'SELECT * FROM  {table_name}')
        print(self.curr.fetchall())

        self.curr.close()
        self.conn.close()








if __name__ == "__main__":

    db = pg_database()

    #### Commands for creating the two tables

    #first table for questions
    # cmd = 'CREATE TABLE IF NOT EXISTS Questions (id SERIAL PRIMARY KEY,audio_bytes BYTEA NOT NULL);'
    # db.create_tables(cmd)
    #second table for recordings
    # cmd = 'CREATE TABLE IF NOT EXISTS recordings (id SERIAL PRIMARY KEY, recording_audio_bytes BYTEA NOT NULL);'
    # db.create_tables(cmd)


    #### Commands to upload the questions. please give the audio files folder.

    pa = r'F:\Pago analytics\Dparliment\theaudiocodepythonfastapi\answers'
    db.upload_questions(pa)


    #### Commnads to get the table from the tables. give the table name.
    # db.get_data('audio_files')
    # db.get_data('recordings')