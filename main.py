from fastapi import FastAPI, UploadFile, File, Response
from fastapi.responses import JSONResponse
import uvicorn
import traceback
from helper import pg_database


app = FastAPI(title='Audio API', description='API to get and save audio from Database.')

#Welcome message
@app.get('/')
def welcome():
    return JSONResponse(content='Hello! welcome to the audio database API')

#Route for getting all the audio files (questions)
@app.get('/get_audio_files')
async def get_all_audio_files():

    try:
        db = pg_database()
        db.curr.execute("SELECT * FROM audio_files")
        audio_files_list = db.curr.fetchall()

        audio_bytes_tuples = [{'id':id, 'audio_bytes':audio_bytes.tobytes().hex()} for id, audio_bytes in audio_files_list]
        return audio_bytes_tuples

    except Exception as e:
        print(f"Error in connection to database or fetching the data: {repr(e)}")
        traceback.print_exc()
        return f"Error in Fetching the Data from the Database."
    
    finally:
        if db.curr is not None:
            db.curr.close()
        if db.conn is not None:
            db.conn.close()

    # return {'audio_files_list': audio_files_list}


@app.post('/save_recording')
async def store_audio_file(file: UploadFile = File(...)):

    try:
        db = pg_database()
        db.curr.execute(f"INSERT INTO recordings (recording_audio_bytes) VALUES (%s)", (file.file.read(),))
        db.conn.commit()
        return JSONResponse(content="The recording is successfully saved in the database", status_code=200)
    
    except Exception as e:
        print(f"Error in connection to database or fetching the data: {repr(e)}")
        traceback.print_exc()
        return f"Error in Fetching the Data from the Database."
    
    finally:
        if db.curr is not None:
            db.curr.close()
        if db.conn is not None:
            db.conn.close()

    
if __name__ == "__main__":

    uvicorn.run(app, host="0.0.0.0", port=8000)
