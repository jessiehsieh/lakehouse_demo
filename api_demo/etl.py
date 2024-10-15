import os
import logging
import sqlite3

import pandas as pd
import requests
from requests.exceptions import HTTPError, ConnectionError, Timeout

retry_exc_types = (Timeout, ConnectionError)

# Set up logging
logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))

SQLITE_DB_NAME = 'my_database.db'
SQLITE_TABLE_NAME = 'persons'

API_DATA_KEYS_PII = ['lastname','firstname','email','phone','birthday','age']
API_DATA_KEYS_GEOLOC = ['longitude','latitude']
API_DATA_KEYS_ADDRESS = ['street','streetName','buildingNumber','zipcode']

def return_empty_list(retry_state):
    """
    Callback Function that is called upon failed retries. Returns an empty list.
    """
    logger.info(f"Maximum retry time reached.")
    logger.info(f"Stopping Execution after {round(retry_state.outcome_timestamp - retry_state.start_time, 2)} seconds and {retry_state.attempt_number} attempts")
    logger.exception(retry_state.outcome.exception())
    return []

@retry(
    retry=retry_if_exception_type(retry_exc_types),  # Retry on request exceptions
    retry_error_callback=return_empty_list,
    wait=wait_exponential(multiplier=2, min=10, max=60),  # Exponential backoff: 4, 8, 16... seconds
    stop=stop_after_delay(600)
)
def request_persons_data_from_api(url, params=None):
    """
    Function to get data from an API with retry mechanism using tenacity.
    
    input url: API endpoint URL
    input params: Dictionary of query parameters to send with the GET request
    output: Response JSON data or an empty list if failed after retry
    """
    logger.info(f"Sending GET request to {url} with params {params}")
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    logger.info(f"Received response: {response.status_code}")
    return response.json()['data']


def compute_age_from_birthday(tuple_today, tuple_birthday):
    """
    Function to compute the age today from a birthday, with both dates given in tuples.
    
    input tuple_today: a tuple displaying today's date as (year, month, day)
    input tuple_birthday: a tuple displaying the birthday as (year, month, day)
    output: an integer displaying the age 
    """
    age_gross = (tuple_today[0] - tuple_birthday[0])
    if tuple_today[1] < tuple_birthday[1]:
        return age_gross-1
    elif tuple_today[1] == tuple_birthday[1]:
        if tuple_today[2] < tuple_birthday[2]:
            return age_gross-1
    return age_gross


def create_age_col_from_birthday(df, birthday_col, age_col, str_today='today'):
    """
    Function to add an age column to a dataframe containing birthday info
    
    input df: a dataframe with the birthday column
    input birthday_col: the name of the birthday column
    input age_col: the name of age column to add
    input str_today: a string indicating today's date
    output: df
    """
    today_datetime = pd.to_datetime(str_today)
    df.loc[:,'today'] = today_datetime
    df.loc[:,'birthday_dt'] = pd.to_datetime(df[birthday_col])
    df.loc[:,age_col] = df['birthday_dt'].apply(lambda x: compute_age_from_birthday((today_datetime.year, 
                                                                                     today_datetime.month, 
                                                                                     today_datetime.day), 
                                                                                     (x.year, 
                                                                                      x.month, 
                                                                                      x.day)))
    df.drop('today', inplace=True, axis=1)
    df.drop('birthday_dt', inplace=True, axis=1)
    return df

def create_age_group_col_from_age(df, age_col, age_group_col):
    """
    Function to add an age group column to a dataframe containing age info
    
    input df: a dataframe with the birthday column
    input age_col: the name of age column
    input age_group_col: the name of the age group column to add
    output: df
    """
    bins = range(0, 200, 10)
    df.loc[:, age_group_col] = pd.cut(df[age_col], bins).astype(str)
    return df

def create_domain_col_from_email(df, email_col, domain_col):
    """
    Function to add an email domain column to a dataframe containing email info
    
    input df: a dataframe with the birthday column
    input email_col: the name of email column
    input domain_col: the name of the email domain column to add
    output: df
    """
    df.loc[:, domain_col] = df[email_col].str.split('@').str[1]
    return df

def mask_column(df, colname):
    df.loc[:, colname] = '****'
    return df

def flatten_df_column(df, colname):
    """
    Function to return the dataframe with a column unnested into many columns.
    Any nested field in the nested column that appears in the dataframe is dropped.
    
    input df: a dataframe with the nested column
    input colname: the name of neseted column
    output: df
    """
    
    df_inner = pd.json_normalize(df[colname])
    for col in df_inner.columns:
        if col in df.columns:
            df_inner.drop(col, axis=1, inplace=True)
    df_with_inner = pd.concat([df.drop(colname, axis=1), df_inner], axis=1)
    return df_with_inner

def execute_in_sqlite3(dbname, query, display=True):
    conn = sqlite3.connect(dbname)
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        if display:
            rows = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            cursor.close()
            return rows, colnames
        else:
            conn.commit()
            cursor.close()
    except Exception as e:
        print(e)


if __name__=="__main__":

    API_URL = "https://fakerapi.it/api/v1/persons"
    QUERY_PARAMS = {"_quantity": "1000", "_birthday_start": "1900-01-01"}

    dict_persons=[]
    while len(dict_persons) < 10000:
        try:
            api_output = request_persons_data_from_api(API_URL, QUERY_PARAMS)
            dict_persons += api_output
            logger.info(f"{len(api_output)} data successfully retrieved from API.")
        except:
            logger.error(f"Exceptions caught from HTTP or retry failed. Inspect the URL or authentication.")
            break

    df_persons = pd.DataFrame(dict_persons)

    df_persons_address = flatten_df_column(df_persons, 'address')
    df_persons_address_age = create_age_col_from_birthday(df_persons_address, 'birthday', 'age')
    df_persons_address_age_grouped = create_age_group_col_from_age(df_persons_address_age, 'age', 'age_group')
    df_persons_address_age_grouped_email = create_domain_col_from_email(df_persons_address_age_grouped, 'email', 'email_domain')

    for col in API_DATA_KEYS_PII+API_DATA_KEYS_GEOLOC+API_DATA_KEYS_ADDRESS:
        df_persons_address_age_grouped_email = mask_column(df_persons_address_age_grouped_email, col)

