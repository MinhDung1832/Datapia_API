from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse,JSONResponse
from database import get_db_connection, get_db_gw_connection
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from models import OrderData
from google.oauth2.service_account import Credentials
import gspread
import datetime
from decimal import Decimal
from pandas import DataFrame
import pandas as pd
import os
import logging
import pyodbc

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
            sheet_id = str(row[spreadsheet_id_index])
            
            credentials_path = os.path.join(str(row[data_json_index]))

            if not os.path.exists(credentials_path):
                return JSONResponse(status_code=200,content={"Status": 400, "Message": "Credential file not found."})

            credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
            client = gspread.authorize(credentials)

            print(str(row[spreadsheet_id_index]))
            # Open the Google Sheet
            try:
                sheet = client.open_by_key(str(row[spreadsheet_id_index])).sheet1
            except Exception as e:
                return JSONResponse(status_code=200,content={"Status": 400, "Message": "Không kết nối được sheet."})
            sheet.clear()
            
            # Add a new sheet to the Google Sheet
            # try:
            #     data_insight = sheet.spreadsheet.add_worksheet(title="Data insights",rows="1000",cols="50")
            #     data_action = sheet.spreadsheet.add_worksheet(title="Data actions",rows="1000",cols="50")
            #     data_cost_per_action_type = sheet.spreadsheet.add_worksheet(title="Data cost per action type",rows="1000",cols="50")
            # except Exception as e:
            #     logging.error(f"Failed to add new sheet: {str(e)}")
            #     raise HTTPException(status_code=500, detail=f"Failed to add new sheet: {str(e)}")

            # Fetch data to populate the Google Sheet
            sql = '''
                SELECT * FROM dbo.fb_data_insight
            '''
            sql_cursor.execute(sql)
            rows = sql_cursor.fetchall()

            # Ensure there are rows before proceeding
            if not rows:
                return JSONResponse(status_code=200,content={"Status": 400, "Message": "No data returned from the query."})

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
                return JSONResponse(status_code=200,content={"Status": 400, "Message": "Mismatch between data and columns count."})

            # Create DataFrame
            df = pd.DataFrame(data, columns=columns)

            # Check if DataFrame is empty
            if df.empty:
                return JSONResponse(status_code=200,content={"Status": 400, "Message": "DataFrame is empty, cannot update Google Sheet."})

            # Update to Google Sheet
            sheet.update([df.columns.values.tolist()] + df.values.tolist())

            return JSONResponse(status_code=200, content={"Status": 200, "Message": sheet_id})
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return JSONResponse(status_code=200, content={"Status": 400, "Message": str(e)})

    finally:
        # Ensure all resources are closed
        if sql_cursor:
            sql_cursor.close()
        if conn:
            conn.close()
            
@app.get("/import_data")
async def import_data(id: int):
    try:
        # Get a database connection
        conn = get_db_connection()
        sql_cursor = conn.cursor()

        # Fetch sheet configuration from the database
        sql_connection_config = f'''
            SELECT * FROM dbo.connection_config WHERE id = {id}
        '''
        sql_cursor.execute(sql_connection_config)
        config_rows = sql_cursor.fetchone()
        # print(config_rows)
        column_names = [desc[0] for desc in sql_cursor.description]
        # print(column_names)
        access_token = str(config_rows[column_names.index("access_token")])
        ad_account_id = str(config_rows[column_names.index("acc_id_val")])
        app_secret = str(config_rows[column_names.index("app_secret")])
        app_id = str(config_rows[column_names.index("app_id")])
        
        # Initialize Facebook API
        FacebookAdsApi.init(access_token=access_token)

        # Define fields and parameters for the request
        fields = [
            'account_currency', 'account_id', 'account_name', 'campaign_name',
            'actions', 'ad_id', 'ad_name', 'adset_id', 'adset_name', 'campaign_id',
            'clicks', 'cost_per_action_type', 'cost_per_unique_click', 'cpc', 'cpm',
            'ctr', 'cost_per_conversion', 'date_start', 'frequency', 'impressions',
            'objective', 'optimization_goal', 'outbound_clicks', 'outbound_clicks_ctr',
            'reach', 'spend', 'conversions', 'cost_per_conversion', 'converted_product_quantity'
        ]

        params = {
            'time_range': {'since': '2022-01-01', 'until': '2024-12-31'},
            'time_increment': 1
        }

        # Fetch campaigns and insights
        campaigns = AdAccount(ad_account_id).get_campaigns(params=params)
        insights = [pd.DataFrame(campaign.get_insights(
            params=params, fields=fields)) for campaign in campaigns]

        df = pd.concat(insights, ignore_index=True) if insights else pd.DataFrame()

        # Database connection
        cursor = conn.cursor()

        # Ensure all required columns are present
        columns_to_insert = ['account_currency', 'account_id', 'account_name', 'campaign_name', 'actions',
                             'ad_id', 'ad_name', 'adset_id', 'adset_name', 'campaign_id', 'clicks',
                             'cost_per_action_type', 'cost_per_unique_click', 'cpc', 'cpm', 'ctr',
                             'cost_per_conversion', 'date_start', 'frequency', 'impressions',
                             'objective', 'optimization_goal', 'outbound_clicks', 'outbound_clicks_ctr',
                             'reach', 'spend', 'conversions', 'cost_per_conversion', 'converted_product_quantity', 'frequency_value']

        # Add missing columns
        for column in columns_to_insert:
            if column not in df.columns:
                df[column] = None

        # Tối ưu hóa việc thực thi các câu lệnh SQL bằng cách sử dụng batch insert thay vì insert từng dòng
        insert_data = []
        action_insert_data = []
        cost_per_insert_data = []

        df_list = df[columns_to_insert].to_dict(orient='records')
        # print(df_list)


        for row in df_list:
            # SQL table and columns table fb_action
            table_action = 'fb_action'
            columns_actions = ['campaign_id', 'campaign_name', 'action_type', 'value']

            # Prepare action data for insertion
            action_insert_data = [
                (row['campaign_id'], row['campaign_name'],
                 action['action_type'], action['value'])
                for action in row['actions']
            ]

            # If there is action data to insert
            if action_insert_data:
                placeholders_actions = ','.join(
                    ['?' for _ in range(len(columns_actions))])
                query_action = f"INSERT INTO {table_action} ({', '.join(columns_actions)}) VALUES ({placeholders_actions})"

                # Execute the SQL command
                cursor.executemany(query_action, action_insert_data)
                conn.commit()

            # ----------------------------------------------------------------
            # SQL table and columns table fb_cost_per_action_type
            table_cost_per_action_type = 'fb_cost_per_action_type'
            columns_cost_per_action_type = [
                'campaign_id', 'campaign_name', 'action_type', 'value']

            # Prepare action data for insertion
            cost_per_action_type_insert_data = [
                (row['campaign_id'], row['campaign_name'],
                 cost_per_action_type['action_type'], cost_per_action_type['value'])
                for cost_per_action_type in row['cost_per_action_type']
            ]

            # If there is action data to insert
            if cost_per_action_type_insert_data:
                placeholders_cost_per_action_type = ','.join(
                    ['?' for _ in range(len(columns_cost_per_action_type))])
                query_cost_per_action_type = f"INSERT INTO {table_cost_per_action_type} ({', '.join(columns_cost_per_action_type)}) VALUES ({placeholders_cost_per_action_type})"

                # Execute the SQL command
                cursor.executemany(query_cost_per_action_type,
                                   cost_per_action_type_insert_data)
                conn.commit()

            # ----------------------------------------------------------------
            # SQL table and columns table fb_data_insight
            table_insight = 'fb_data_insight'
            columns_insight = ['date_start', 'account_id', 'account_name', 'campaign_id', 'campaign_name', 'adset_id', 'adset_name', 'ad_id', 'ad_name',
                               'objective', 'optimization_goal', 'spend', 'impressions', 'reach', 'frequency_value', 'clicks', 'cost_per_unique_click', 'ctr', 'cpm']

            # Prepare action data for insertion
            insight_insert_data = [
                (
                    None if pd.isna(row['date_start']) else row['date_start'],
                    None if pd.isna(row['account_id']) else row['account_id'],
                    None if pd.isna(row['account_name']) else row['account_name'],
                    None if pd.isna(row['campaign_id']) else row['campaign_id'],
                    None if pd.isna(row['campaign_name']) else row['campaign_name'],
                    None if pd.isna(row['adset_id']) else row['adset_id'],
                    None if pd.isna(row['adset_name']) else row['adset_name'],
                    None if pd.isna(row['ad_id']) else row['ad_id'],
                    None if pd.isna(row['ad_name']) else row['ad_name'],
                    None if pd.isna(row['objective']) else row['objective'],
                    None if pd.isna(row['optimization_goal']
                                    ) else row['optimization_goal'],
                    None if pd.isna(row['spend']) else row['spend'],
                    None if pd.isna(row['impressions']) else row['impressions'],
                    None if pd.isna(row['reach']) else row['reach'],
                    None if pd.isna(row['frequency_value']
                                    ) else row['frequency_value'],
                    None if pd.isna(row['clicks']) else row['clicks'],
                    None if pd.isna(row['cost_per_unique_click']
                                    ) else row['cost_per_unique_click'],
                    None if pd.isna(row['ctr']) else row['ctr'],
                    None if pd.isna(row['cpm']) else row['cpm']
                )

                # for cost_per_action_type in row['cost_per_action_type']
            ]
            # print(insight_insert_data)

            # If there is action data to insert
            if insight_insert_data:
                placeholders_insight = ','.join(
                    ['?' for _ in range(len(columns_insight))])
                query_insight = f"INSERT INTO {table_insight} ({', '.join(columns_insight)}) VALUES ({placeholders_insight})"

                # Execute the SQL command
                cursor.executemany(query_insight, insight_insert_data)
                conn.commit()
        

        return JSONResponse(status_code=200, content={"Status": 200, "Message": "Success"})
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        # raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
        return JSONResponse(status_code=400, content={"Status": 200, "Message": str(e)})

    finally:
        # Ensure all resources are closed
        if sql_cursor:
            sql_cursor.close()
        if conn:
            conn.close()

