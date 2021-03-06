#Description:   Python job script used to actualize an SQL databases data with the newest avialable COVID data.
#               This script checks which date data was last added to the table.  All data between the last added date and the date the job runs will be added to the SQL database
# Author:       Cody Torgerson           


import os, requests, json, datetime 

## pip specific modules
import pandas as pd
import sqlite3

##project specific modules
import db_connector



class Actualizesqldatabase:
    """ Class which fills the SQlite database with data
    Args:
        Countries: list of strings which are country names
    """
    def df_to_sql(self,country_ISO):
        #Processes one countries data for a specific timerange
        try:
            #Collect List of States
            r = requests.get('https://covid-api.com/api/reports?date=2020-12-01&iso=DEU')
            jsonify = r.json()
            
            df = pd.DataFrame(jsonify['data'])

            #Because of nested json the regions column needs to be refactored
            regiondf = pd.DataFrame(df['region'])
            x = json.loads(regiondf.to_json())
            states = pd.DataFrame(x['region'])
            states = states.transpose()
            #remove column filled with nested JSON metadata
            Maindataframe = pd.DataFrame()
            for date in  self.daterange(datetime.date(2020,12,1),datetime.date(2020,12,5)):
                date = date.strftime("%Y-%m-%d")
                for rows in states['province']:
                    r = requests.get(f'https://covid-api.com/api/reports?date={date}&iso=DEU&region_province={rows}')
                    jsonify = r.json()
                    df = pd.DataFrame(jsonify['data'])
                    df = df.drop(columns=['region'])
                    df['State'] = rows
                    #TODO: Change the coutnry name so its updated by the ISO or read in
                    df['Country'] = "Germany"
                    #check if the dataframe has data in it or not
                    #If not then append the dataframe df to the Maindataframe
                    if Maindataframe.size == 0:
                        Maindataframe = df
                    else:
                        Maindataframe = Maindataframe.append(df, ignore_index=True)
                    
                    #TODO: Cleanup data so that the state and country are columns


            engine, meta = db_connector.db_engine()

            df.to_sql("covid19basic", engine, if_exists="replace")
        except Exception as e:
            print(e)



    def find_country_ISO(Country):
        #TODO: cache this array
        r = requests.get('https://covid-api.com/api/regions')
        jsonify = r.json()

        df = pd.DataFrame(jsonify['data'])
        try:
            index = df.loc[df['name']==Country]     
            iso_code = index['iso'].values[0] 
            return iso_code 
        except Exception as e:
            print('Country not found')
            return e

    
    def select_data_by_country(self, country_name: str):
        """ Queries the Covid19 SQLite database by country name.

            :returns Dataframe:
        """
        engine, meta = db_connector.db_engine()
        query_string = f"""SELECT * FROM 'covid19basic' WHERE [Country/Region] = '{country_name}' ORDER BY [Province/State] ASC, [ObservationDate] ASC;"""
        df = pd.read_sql_query(query_string, engine)
        return df

    def generate_new_cases_per_day(self, dataframe):
        """Takes the derivative of confirmed cases per day and saves it in a dataframe

        Args:
            dataframe: Dataframe containing the columns New_Cases, ObservationDate.

        Returns:
            A dataframe containing Columns Change_in_cases_added, ObservationDate.


        """
        length = len(dataframe)
        df = pd.DataFrame(columns=["SNo","New_Cases"])
        counter = 0
        try:
            dataframe = dataframe.rename(columns={'Province/State': 'Province_State'})
            for row in dataframe.itertuples():
                index = row.SNo
                if counter == 0:
                    x1 =row.Confirmed
                    state_1 = row.Province_State
                    counter += 1
                else:
                    state_2 = row.Province_State
                    if state_2 == state_1:
                        x2 = row.Confirmed
                        date = row.ObservationDate
                        derivative = (x2 - x1)/1
                        print(derivative)
                        df2 = pd.DataFrame({"SNo":[index],'New_Cases': [derivative]})
                        df = pd.concat([df,df2])
                        x1 = row.Confirmed
                        counter += 1
                    else:
                        counter = 1
                        state_1 = state_2
                        x1 = row.Confirmed        
            print(df)

            return df
        except Exception as e:
            print("Generate New Cases Data Failed")
            print(e)
                       


    def daterange(start_date, end_date):
        """Returns a list of dates in YYYY-MM-DD format
        From: https://stackoverflow.com/questions/1060279/iterating-through-a-range-of-dates-in-python

        Args:
           start_date: Datetime date string in YYYY-MM-DD format. Represents first day of list
           end_date: Datetime date string in YYYY-MM-DD format. Represents last day of list
          
        Returns:
           Returns a list of dates in YYYY-MM-DD format that represent the desired range of dates from start to end.
        
        Example:
        for single_date in daterange(start_date, end_date):
             print(single_date.strftime("%Y-%m-%d"))
        """
        for n in range(int((end_date - start_date).days)):
            yield start_date + datetime.timedelta(n)

    def generate_change_in_cases_added(self, dataframe):
        """Takes the derivative of New cases per day and saves it in a dataframe

        Args:
            dataframe: Dataframe containing the columns New_Cases, ObservationDate.

        Returns:
            A dataframe containing Columns Change_in_cases_added, ObservationDate.

        """
        length = len(dataframe)
        df = pd.DataFrame(columns=["SNo","Change_In_Cases_Added"])
        counter = 0
        try:
            dataframe = dataframe.rename(columns={'Province/State': 'Province_State'})
            for row in dataframe.itertuples():
                index = row.SNo
                if counter == 0:
                    x1 =row.New_Cases
                    state_1 = row.Province_State
                    counter += 1
                else:
                    state_2 = row.Province_State
                    if state_2 == state_1:
                        x2 = row.New_Cases
                        date = row.ObservationDate
                        derivative = (x2 - x1)/1
                        df2 = pd.DataFrame({"SNo":[index],'Change_In_Cases_Added': [derivative]})
                        df = pd.concat([df,df2])
                        x1 = row.New_Cases
                        counter += 1
                    else:
                        counter = 1
                        state_1 = state_2
                        x1 = row.New_Cases        
            print(df)
            
            return df
        except Exception as e:
            print("Generate Change in new cases Data Failed")
            print(e)





    def merge_newcases_change_in_cases_dataframes(self, dataframe_confirmed):
        """ Combines dataframes containing new cases, and the change in new cases, and inserts the dataframe to the SQLite database



        Args:
            dataframe_confirmed: The first parameter.
        Returns:
            df: A dataframe containing Columns Change_in_cases_added, ObservationDate.
        """
        try:
            df_newcases = self.generate_new_cases_per_day(dataframe_confirmed)
            df = pd.merge(dataframe_confirmed,df_newcases, how='outer', on='SNo')
            df_changeinnewcases = self.generate_change_in_cases_added(df)
            df = pd.merge(df_changeinnewcases,df, how='outer', on='SNo')    
        except Exception as e:
            print(e)

        try:
            engine, meta = db_connector.db_engine()
            df.to_sql("daily_change", engine, if_exists="replace")
        except Exception as e:
            print(e)

    def __init__(self, countries: list):
        """
        """
        # try:
        #     sql_string = "SELECT DISTINCT * FROM [daily_change] ORDER by ObservationDate Desc"   
        #     df = db_connector.df_sql_query(sql_string)
        # except sqlite3.OperationalError as e:
        #     print(e)
            

        


        if type(countries) is list:
            for country in countries:
                try:
                    df = self.select_data_by_country(country)
                    self.merge_newcases_change_in_cases_dataframes(df)
                    print(f"Succesfully generated and inserted data for {country}")
                except Exception as e:
                    print(e)
        else: 
            if type(countries) is str:

                try:
                    df = self.select_data_by_country(countries)
                    self.merge_newcases_change_in_cases_dataframes(df)
                    print(f"Succesfully generated and inserted data for {countries}")
                except Exception as e:
                    print(e)
            else:
                raise TypeError("Values given is not a String")

        sql_string = "SELECT DISTINCT * FROM [daily_change]"   
        df = db_connector.df_sql_query(sql_string)
        engine = db_connector.db_engine()
        df.to_sql("daily_change",engine,  if_exists="replace")
        
# response example      
#   "data": [
#     {
#       "date": "2020-12-01",
#       "confirmed": 152782,
#       "deaths": 2825,
#       "recovered": 107339,
#       "confirmed_diff": 2063,
#       "deaths_diff": 49,
#       "recovered_diff": 2701,
#       "last_update": "2020-12-02 05:27:41",
#       "active": 42618,
#       "active_diff": -687,
#       "fatality_rate": 0.0185,
#       "region": {
#         "iso": "DEU",
#         "name": "Germany",
#         "province": "Baden-Wurttemberg",
#         "lat": "48.6616",
#         "long": "9.3501",
#         "cities": []
#       }