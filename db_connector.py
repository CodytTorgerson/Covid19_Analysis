from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer, String, DateTime, Float
import pandas
import os

def db_engine():
    """Creates an Sqlalchemy engine object

    :returns: engine
    """
    current_dir = os.getcwd()
    database_dir = os.path.join(current_dir,"db","covid_19.db")
    connector_string = f"sqlite+pysqlite:////{database_dir}"
    engine = create_engine(connector_string)
    meta = MetaData(engine)
    return(engine,meta)


def execute_query(SQL_String: str):
    if type(SQL_String) == str:
        try:
            engine,meta = db_engine()
            conn = engine.connect()
            conn.execute(SQL_String)
        except Exception as e:
            print(e)

def df_sql_query(SQL_String: str):
    """ Function queries data into a data frame
    """
    if type(SQL_String) == str:
        try:
            engine,meta = db_engine()
            conn = engine.connect()
            df = pandas.read_sql_query(SQL_String,conn)
            return df
        except Exception as e:
            print(e)




def raw_insert_string(Sno,ObservationDate,Province_State,Country,Last_Update, Confirmed, Deaths,Recovered):
    """ Insert data string into 

    """

    string =f"""INSERT INTO `covid19basic` (SNo, ObservationDate, Province/State, Country/Region, Last Update, Confirmed, Deaths, Recovered)
    VALUES ({Sno},{ObservationDate},{Province_State},{Country}, {Last_Update}, {Confirmed}, {Deaths}, {Recovered});"""



def covid_19_basic_sql_insert_sqlalchemy(table, engine, dataframe):
    """Function inserts data into sqldatabase
        
    """
    ins = table.insert().values(
        ObservationDate = dataframe["ObservationDate"],
        Province_State = dataframe["DateTime"],
        Country_Region= dataframe["Country_Region"],
        Last_Update = dataframe["Last_Update"],
        Confirmed = dataframe["Confirmed"],
        Deaths = dataframe["Deaths"],
        Recovered = dataframe["Recovered"])

    try:
        conn = engine.connect()
        conn.execute(ins)
    except Exception as e:
        print(e)
        print("SQL INSERT FAILED")

def Daily_Change_sql_insert_sqlalchemy(table, engine, dataframe):
    """Function inserts data into sqldatabase
        
    """
    ins = table.insert().values(
        ObservationDate = dataframe["ObservationDate"],
        Province_State = dataframe["DateTime"],
        Country_Region= dataframe["Country_Region"],
        Confirmed = dataframe["Confirmed"],
        Deaths = dataframe["Deaths"],
        Recovered = dataframe["Recovered"])

    try:
        conn = engine.connect()
        conn.execute(ins)
    except Exception as e:
        print(e)
        print("SQL INSERT FAILED")

def covid19basic_table_object(meta):
    """ Creates an SQL Alchemy Table Object for the Covid19 Dataset
    
    :type SQLAlchemy_Table:
    :returns covid_table:

    """
    covid_table = Table('covid19basic', meta,
            Column('SNo', Integer, primary_key=True),
            Column('ObservationDate',String),
            Column('Province/State', DateTime),
            Column('Country/Region', String),
            Column('Last Update', DateTime),
            Column('Confirmed',Float),
            Column('Deaths',Float),
            Column('Recovered',Float))
    return covid_table

def daily_change_table(meta):
    """ Creates an SQL Alchemy Table Object for the Covid19 Dataset
    
    :type SQLAlchemy_Table:
    :returns covid_table:
    """


    daily_change_table = Table('daily_change', meta,
            Column('SNo', Integer, primary_key=True),
            Column('ObservationDate',String),
            Column('Province/State', DateTime),
            Column('Country/Region', String),
            Column('Last Update', DateTime),
            Column('Confirmed',Float),
            Column('New_Cases',Float),
            Column('Change_In_Cases_Added',Float),
            Column('Deaths',Float),
            Column('Recovered',Float))
    return daily_change_table

def create_covid_19_table(engine,meta):
    """ Creates a table used for the Covid 19 Dataset"

    This function only needs to be called once in order to generate the the SQL Table used in this data analysis.

 0   SNo               non-null   int64  
 1   ObservationDate   non-null   Datetime 
 2   Province/State    non-null   String 
 3   Country/Region    non-null   String 
 4   Last Update       non-null   Datetime 
 5   Confirmed         non-null   float64
 6   Deaths            non-null   float64
 7   Recovered         non-null   float64

    """
    covid_table = Table('covid19basic', meta,
            Column('SNo', Integer, primary_key=True),
            Column('ObservationDate',String),
            Column('Province/State', DateTime),
            Column('Country/Region', String),
            Column('Last Update', DateTime),
            Column('Confirmed',Float),
            Column('Deaths',Float),
            Column('Recovered',Float))


    covid_table.create()

def create_covid_19_derivative_Confirmed_table(engine,meta):
    """ Creates a table used for examining the rate of change of cases"

    This function only needs to be called once in order to generate the the SQL Table used in this data analysis.
    :Params
    0   SNo                        non-null   int64  
    1   ObservationDate            non-null   Datetime 
    2   Province/State             non-null   String 
    3   Country/Region             non-null   String 
    4   Last Update                non-null   Datetime 
    5   Increase in Cases          non-null   float64
    6   Rate_of_Increase_in_cases  non-null   float64
    :
    """
    
    covid_table = Table('daily_change', meta,
            Column('SNo', Integer, primary_key=True),
            Column('ObservationDate',String),
            Column('Province/State', DateTime),
            Column('Country/Region', String),
            Column('Last Update', DateTime),
            Column('Confirmed',Float),
            Column('New_Cases',Float),
            Column('Change_In_Cases_Added',Float),
            Column('Deaths',Float),
            Column('Recovered',Float))


    covid_table.create()
