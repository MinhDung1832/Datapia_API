from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse,JSONResponse
from database import get_db_connection, get_db_gw_connection
from models import OrderData
from google.oauth2.service_account import Credentials
import gspread
import datetime
from decimal import Decimal
from pandas import DataFrame
import pandas as pd
import os
import logging

app = FastAPI()

@app.get("/google_sheet")
async def google_sheet(id: int):
    try:
        # Get a database connection
        conn = get_db_connection()
        sql_cursor = conn.cursor()

        # Fetch sheet configuration from the database
        sql_sheet_config = f'''
            SELECT * FROM dbo.googlesheet_config WHERE id = {id}
        '''
        sql_cursor.execute(sql_sheet_config)
        sheet_config_rows = sql_cursor.fetchall()

        # Loop through each config row and update corresponding Google Sheets
        for row in sheet_config_rows:
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]

            # Access elements in 'row' by index
            column_names = [desc[0] for desc in sql_cursor.description]

            # Find the index of the 'data_json' column
            data_json_index = column_names.index("data_json")
            spreadsheet_id_index = column_names.index("spreadsheet_id")
            credentials_path = os.path.join(str(row[data_json_index]))

            if not os.path.exists(credentials_path):
                raise HTTPException(status_code=500, detail="Credential file not found.")

            credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
            client = gspread.authorize(credentials)

            print(str(row[spreadsheet_id_index]))
            # Open the Google Sheet
            sheet = client.open_by_key(str(row[spreadsheet_id_index])).sheet1
            sheet.clear()

            # Fetch data to populate the Google Sheet
            sql = '''
                SELECT * FROM dbo.googlesheet_config
            '''
            sql_cursor.execute(sql)
            rows = sql_cursor.fetchall()

            # Ensure there are rows before proceeding
            if not rows:
                raise HTTPException(status_code=500, detail="No data returned from the query.")

            # Convert Decimal objects to float
            data = []
            for row in rows:
                row_data = []
                for value in row:
                    try: 
                        if isinstance(value, Decimal):
                            row_data.append(float(value))
                        elif isinstance(value, datetime.date):  # Convert date objects to strings
                            row_data.append(value.strftime('%Y-%m-%d'))
                        else:
                            row_data.append(value)
                    except Exception as ex:
                        logging.warning(f"Skipping value due to error: {value}, error: {ex}")
                        row_data.append(None)  # Append None in case of failure
                data.append(row_data)

            columns = [column[0] for column in sql_cursor.description]

            # Check if the number of columns matches the number of data fields
            if len(columns) != len(data[0]):
                raise HTTPException(status_code=500, detail="Mismatch between data and columns count.")

            # Create DataFrame
            df = pd.DataFrame(data, columns=columns)

            # Check if DataFrame is empty
            if df.empty:
                raise HTTPException(status_code=500, detail="DataFrame is empty, cannot update Google Sheet.")

            # Update to Google Sheet
            sheet.update([df.columns.values.tolist()] + df.values.tolist())

            return JSONResponse(status_code=200, content={"Status": 200, "Message": {"SpreadsheetId": str(row[spreadsheet_id_index])}})
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

    finally:
        # Ensure all resources are closed
        if sql_cursor:
            sql_cursor.close()
        if conn:
            conn.close()

