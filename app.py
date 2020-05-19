import pandas as pd
import os
import db_connector
import plotly.express as px
import sqlite3



def total_confirmed_cases_figure_object(dataframe):
    """ Function creates a plotly Express timeseries figure object

    :returns fig:
    """
    
    country_name = dataframe["Country/Region"].values[1]
    
    fig = px.line(dataframe, x='ObservationDate', y='Confirmed', title=f"""Confirmed Cases in {country_name}""")
    return fig


def new_cases_figure_object(dataframe):
    """ Function creates a plotly Express timeseries figure object

    :returns fig:
    """
    country_name = dataframe["Country/Region"].values[1]

    fig = px.line(dataframe, x='ObservationDate', y='New_Cases',width=1000, height=800, title=f"""New Cases per day in {country_name}""", color='Province/State')
    return fig

def change_in_newcases_figure_object(dataframe):
    """ Function creates a plotly Express timeseries figure object

    :returns fig:
    """
    country_name = dataframe["Country/Region"].values[1]
    fig = px.line(dataframe, x='ObservationDate', y='Change_In_Cases_Added',width=1000, height=800, title=f"""Change in new cases per day {country_name}""")
    return fig


def get_states_in_data(dataframe):
    """Function returns a list of all states present in a country
    """
    try:
        country_name = dataframe["Country/Region"].values[1]
        sql_string = f"""SELECT DISTINCT [Province/State],[Country/Region] FROM [daily_change] WHERE [Country/Region] = '{country_name}' ORDER BY [Province/State] ASC; """
        engine,meta = db_connector.db_engine()
        conn = engine.connect()
        df = pd.read_sql_query(sql_string,conn)
        return df
    except Exception as e:
        print(e)

def query_state_data(Country,state):
    """Function Queries data by a state or province within a country
    """
    sql_string = f"""SELECT * FROM [daily_change] WHERE [Country/Region] = '{Country}' and [Province/State] = '{state}' ORDER BY [Province/State] ASC, [ObservationDate] ASC;"""
    engine,meta = db_connector.db_engine()
    conn = engine.connect()
    df = pd.read_sql_query(sql_string,conn)
    return df

def query_country_data(country):
    """ Function queries data by a countries name
    """
    try:
        engine, meta = db_connector.db_engine()
        conn = engine.connect()
        df_select = pd.read_sql_query(f"""SELECT * FROM [daily_change] WHERE [Country/Region] = '{country}' ORDER BY [Province/State] ASC, [ObservationDate] ASC; """,conn)
        if len(df_select) == 0:
            list_country = [country]
            initlize_db(list_country)
            try:
                engine, meta = db_connector.db_engine()
                conn = engine.connect()
                sql_string = f"""SELECT * FROM [daily_change] WHERE [Country/Region] = '{country}' ORDER BY [Province/State] ASC, [ObservationDate] ASC; """
                df_select = pd.read_sql_query(sql_string,conn)
                return df_select
            except Exception as e:
                print(e)  
            print("Country Data Present")
            return df_select     
        else:
            return df_select  
    except Exception as e:
        print(e)

class 
:
    """ Class which fills the SQlite database with data
    Args:
        Countries: list of strings which are country names
    """
    def df_to_sql(self):
        try:
            parent_dir = os.getcwd()
            data_folder_dir = os.path.join(parent_dir, "data")
            data_path = os.path.join(data_folder_dir, "covid_19_data.csv")
            df = pd.read_csv(data_path)

            engine, meta = db_connector.db_engine()

            df.to_sql("covid19basic", engine, if_exists="replace")
        except Exception as e:
            print(e)

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
        
        