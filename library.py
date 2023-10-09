"""
Library of functions for the dashboard.
"""

# Import libraries
import os
import sys
import numpy as np
import sqlalchemy as sa
import urllib
import pandas as pd
import seaborn as sns
from loguru import logger

# User defined functions
def check_datetime_format(datetime):
    """Check that datetime is in the correct format."""
    try:
        datetime = pd.to_datetime(datetime)
    except:
        logger.error(f'Datetime {datetime} is not in the correct format. Please enter a datetime in the format "mm/dd/yyyy hh:mm:ss".')
        raise
    return datetime

def check_datetime_order(datetime_start, datetime_end):
    """Check that datetime_start is before datetime_end."""
    if datetime_start > datetime_end:
        logger.error(f'Datetime {datetime_start} is after {datetime_end}. Please enter a datetime_start that is before datetime_end.')
        raise

def check_tags_table(df):
    """
    Check that tags table has been loaded correctly from the configuration file. 

    Args: 
    df (pandas.DataFrame): Dataframe of tags table.

    If tags table is not loaded correctly, log a warning.
    """

    # Check that df is a dataframe
    if not isinstance(df, pd.DataFrame):
        logger.warning('check_tags_table(): df is not a dataframe.')

    # Check that there are rows in the dataframe
    if df.shape[0] == 0:
        logger.warning('check_tags_table(): df is empty.')

    # Check that columns include TagID, Tag, Process, and Units
    if 'TagID' not in df.columns:
        logger.warning('check_tags_table(): TagID column not found in tags table.')
    if 'Tag' not in df.columns:
        logger.warning('check_tags_table(): Tag column not found in tags table.')
    if 'Process' not in df.columns:
        logger.warning('check_tags_table(): Process column not found in tags table.')
    if 'Units' not in df.columns:
        logger.warning('check_tags_table(): Units column not found in tags table.')

    # Check that TagID column is type int
    if df['TagID'].dtype != 'int64':
        logger.warning('check_tags_table(): TagID column is not type int64.')
    
    # Check that Tag column is type str
    if df['Tag'].dtype != 'object':
        logger.warning('check_tags_table(): Tag column is not type object.')

    # Check that Process column is type str
    if df['Process'].dtype != 'object':
        logger.warning('check_tags_table(): Process column is not type object.')

    # Check that Units column is type str
    if df['Units'].dtype != 'object':
        logger.warning('check_tags_table(): Units column is not type object.')

    # Check that TagID column is unique
    if not df['TagID'].is_unique:
        logger.warning('check_tags_table(): TagID column is not unique.')

    # Check that Tag column is unique
    if not df['Tag'].is_unique:
        logger.warning('check_tags_table(): Tag column is not unique.')

    return

def check_events_table(df): 
    """
    Check that events table has been loaded correctly from the configuration file.
    
    Args:
        df (pandas.DataFrame): Dataframe of events table.

    If events table is not loaded correctly, log a warning.
    """

    # Check that df is a dataframe
    if not isinstance(df, pd.DataFrame):
        logger.warning('check_events_table(): df is not a dataframe.')

    # Check that there are rows in the dataframe
    if df.shape[0] == 0:
        logger.warning('check_events_table(): df is empty.')

    # Check that columns include Empty, EventID, Event Text, Event Process, and Event Type
    if 'EventID' not in df.columns:
        logger.warning('check_events_table(): EventID column not found in events table.')
    if 'Event Text' not in df.columns:
        logger.warning('check_events_table(): Event Text column not found in events table.')
    if 'Event Process' not in df.columns:
        logger.warning('check_events_table(): Event Process column not found in events table.')
    if 'Event Type' not in df.columns:
        logger.warning('check_events_table(): Event Type column not found in events table.')

    # Check that EventID column is type int
    if df['EventID'].dtype != 'int64':
        logger.warning('check_events_table(): EventID column is not type int64.')

    # Check that Event Text column is type str
    if df['Event Text'].dtype != 'object':
        logger.warning('check_events_table(): Event Text column is not type object.')

    # Check that Event Process column is type str
    if df['Event Process'].dtype != 'object':
        logger.warning('check_events_table(): Event Process column is not type object.')
    
    # Check that Event Type column is type str
    if df['Event Type'].dtype != 'object':
        logger.warning('check_events_table(): Event Type column is not type object.')

    # Check that EventID column is unique
    if not df['EventID'].is_unique:
        logger.warning('check_events_table(): EventID column is not unique.')

    return

def convert_items_to_list_of_ints(event_dict):
    """
    Convert event dictionary items into a list of integers.
    If an integer already, convert to list of integers.
    If a string, convert to list of integers.
    If empty, convert to empty list.
    Otherwise, log an warning.
    
    Args: 
        event_dict (dictionary): dictionary of event items to convert to a list of integers.

    Returns:
        event_dict (dictionary): dictionary of event items converted to a list of integers.
    
    """
    # Check that parameter is a dictionary
    if type(event_dict) != dict:
        logger.error(f'Input argument is not a dictionary, it is {type(event_dict)}.')
    
    # Convert dictionary items into a list of integers
    for k, v in event_dict.items():
        if type(v) == int:
            event_dict[k] = [v]
        elif type(v) == str:
            event_dict[k] = [int(i) for i in v.split(',')]
        elif np.isnan(v):
            event_dict[k] = []
        elif v == '':
            event_dict[k] = []
        else:
            logger.warning(f'Issue converting TagIDs {k} in Events table to a list of integers.')

    return event_dict

def create_engine(driver, server, database, username, password):
    """
    Create engine with the project's SQL database.

    Args:
        driver (str): SQL driver name.
        server (str): SQL server name.
        database (str): SQL database name.
        username (str): SQL username.
        password (str): SQL password.

    Returns:
        sqlalchemy.engine: Engine object for the project's SQL database.
    """

    # Create SQL engine to read/write table data
    cnxn_url= urllib.parse.quote_plus(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')
    engine = sa.create_engine(f'mssql+pyodbc:///?odbc_connect={cnxn_url}')

    # Check the connection
    try:
        conn = engine.connect()
        logger.info('Database connection test successful.')
        conn.close()
    except Exception as e:
        logger.error(f'Database connection test failed: {e}')

    return engine

def check_tagids_missing_from_sql(engine, df, table, datetime_col, datetime_start, datetime_end):
    """
    Compare the TagIDs in the SQL database to the TagIDs in the configuration file. 
    If there are TagIDs in the configuration file that are not found in the SQL database, 
    log a warning with the TagIDs that are missing from the database.

    Args:
        engine (sqlalchemy.engine.base.Engine): SQLAlchemy connection engine.
        df (pandas.DataFrame): Dataframe of tags table.

    Returns: 
        list_missing_tagids (list): List of TagIDs that are missing from the SQL database.
    """

    ## Get a set of TagIDs from SQL database for datetime range
    logger.info('compare_sql_config_tags(): querying TagIDs from SQL database.')
    query1 = f'SELECT DISTINCT TagID FROM {table} WHERE {datetime_col} >= \'{datetime_start}\' AND {datetime_col} < \'{datetime_end}\''
    df_sql = pd.read_sql(query1, engine)

    # Convert TagID to int
    try:
        df_sql['TagID'] = df_sql['TagID'].astype(int)
    except:
        logger.warning(f'compare_sql_config_tags(): Warning! TagID cannot be converted into int.')

    sql_tagids = set(df_sql['TagID'])  # convert from dataframe to a set

    ## Get a set of TagIDs from config file
    config_tagids = set(df['TagID'])

    ## Compare the two sets of TagIDs. If there are TagIDs in the config file that aren't found in the database, log a warning with the TagIDs that are missing from the database
    if config_tagids.issubset(sql_tagids) == False:
        missing_tagids = config_tagids - sql_tagids
        logger.warning(f'The following TagIDs are missing from the database: {missing_tagids}')
        list_missing_tagids = df[df['TagID'].isin(missing_tagids)]
    else:
        logger.info('All TagIDs in the config file are found in the database.')
        list_missing_tagids = []

    return list_missing_tagids


def create_df_from_sql(engine, table, datetime_start, datetime_end, tagid_set, datetime_col="[DateTime]"):
    """
    Create a dataframe from a subset of the project's SQL database.

    Args:
        engine (sqlalchemy.engine): SQL engine to read/write table data.
        table (str): SQL table name.
        datetime_start (datetime): Start datetime for query.
        datetime_end (datetime): End datetime for query.
        tagid_set (set): Set of tagids to query.
        datetime_col (str): Datetime column name in SQL database.

    Returns:
        pandas.DataFrame: Dataframe of SQL query results.

    Warns:
        UserWarning: If tagid_set cannot be converted to type set.
    """

    # Convert tagid_set to type set, if cannot convert, log a warning
    try:
        tagid_set = set(tagid_set)
    except:
        logger.warning('create_df_from_sql(): tagid_set cannot be converted to a set.')
    
    # Convert set to a string to pass to SQL query
    tagid_str = '(' + ', '.join(str(e) for e in tagid_set) + ')'
    
    # Set up query
    query= f'SELECT * FROM {table} WHERE {datetime_col} >= \'{datetime_start}\' AND {datetime_col} < \'{datetime_end}\' AND TagID IN {tagid_str}'

    # Use pandas to query the database and return a pandas dataframe
    sql_df = pd.read_sql(query, engine)

    # Convert TagID to int
    try:
        sql_df['TagID'] = sql_df['TagID'].astype(int)
    except:
        logger.warning(f'create_df_from_sql(): Warning! TagID cannot be converted into int.')

    if datetime_col == '[DateTime]':
        sql_df = sql_df.rename(columns={'DateTime':'Datetime'})

    # Truncate datetimes with seconds on the minute containing the seconds
    sql_df['Datetime'] = pd.to_datetime(sql_df['Datetime'])
    sql_df['Datetime'] = sql_df['Datetime'].dt.floor('min')

    # Check if dataframe is empty. If so, log an error and exit the program.
    if sql_df.empty:
        logger.error(f'create_df_from_sql(): Error! No data found in {table} between {datetime_start} and {datetime_end}. Exiting program.')
        sys.exit()
        
    # Drop duplicates
    sql_df = sql_df.drop_duplicates(subset=['TagID','Datetime'], keep='first').reset_index(drop=True)
    
    return sql_df

def check_for_missing_status_tag(df_wide, tag):
    """
    Check for missing columns in dataframe (df_wide) for each production status tag (tagid). 
    If a column is missing, log a warning and create a new column with all values set to 1.0.

    Args:
        df_wide (pandas.DataFrame): Dataframe with wide format.
        tag (str): Tag name of production status tag.

    Returns:
        None
    """
    if tag not in df_wide.columns:
        logger.warning(f'The following column is missing from the dataframe: {tag}. Creating new column and setting all values to 1.0.')
        df_wide[tag] = 1.0  # 1.0 = in production (assume in production for entire datetime range if column is missing)

def check_for_missing_value_tag(df_wide, tag_dict, tagid):
    """
    Check for missing columns in dataframe (df_wide) for each value tag (tagid).
    If a column is missing, log a warning and create a new column with all values set to NaN.
    
    Args:  
        df_wide (pandas.DataFrame): Dataframe with wide format.
        tag_dict (dict): Dictionary of tagid:tagname.
        tagid (int): TagID of value tag.

    Returns:
        None
    """
    tag = tag_dict[tagid]
    if tag not in df_wide.columns:
        logger.warning(f'The following column is missing from the dataframe: {tag}. Creating new column and setting all values to NaN.')
        df_wide[tag] = np.nan  # create new column and set all values to NaN

def check_all_tags_missing_for_event(df_wide, tag_dict, tagids):
    """
    Check if all tags listed are missing. If so, then set event flag to True.

    Args:
        df_wide (pandas.DataFrame): Dataframe with wide format.
        tag_dict (dict): Dictionary of tagid:tagname.
        tagids (list): List of TagIDs.

    Returns:
        bool: True if all tags are missing, False if at least one tag is not missing.
    """
    tags = [tag_dict[tagid] for tagid in tagids]

    if not tags:  # if tags is empty return False
        return False
    elif all(tag not in df_wide.columns for tag in tags):  # if tags are not empty and all tags are missing, return True
        logger.warning(f'All of the following columns are missing from the dataframe: {tags}. Setting event flag to True.')
        return True
    else:
        return False
    
def create_primary_event_tags_dict(event_dict, tag_dict, name_ro_process, name_ro_monitoring, name_ro_wq1, name_ozone_wq1, name_ozone_monitoring):
    """
    Create a dictionary of eventid:primary event tag. The primary event tag is the tag that is used to determine if the event is occurring or not.

    Args:
        event_dict (dict): Dictionary of eventid:tagids.
        tag_dict (dict): Dictionary of tagid:tagname.
        name_ro_process (str): Tag name of RO Process.
        name_ro_monitoring (str): Tag name of RO Monitoring.
        name_ro_wq1 (str): Tag name of RO WQ1.
        name_ozone_wq1 (str): Tag name of Ozone WQ1.

    Returns:
        dict: Dictionary of eventid:primary event tag.
    """
    primary_event_tags = {}
    for eventid in event_dict.keys():
        # If an event only has one tag, then that tag is the primary event tag
        if len(event_dict[eventid]) == 1:
            primary_event_tags[eventid] = tag_dict[event_dict[eventid][0]]
        # If an event has multiple tags, set the primary event tag to the name of the calculated tag. 
        elif len(event_dict[eventid]) > 1:
            if eventid == 5:
                primary_event_tags[eventid] = name_ro_process
            elif eventid == 6:
                primary_event_tags[eventid] = name_ro_monitoring
            elif eventid == 7:
                primary_event_tags[eventid] = name_ro_wq1
            elif eventid == 14:
                primary_event_tags[eventid] = name_ozone_monitoring
            elif eventid == 15:
                primary_event_tags[eventid] = name_ozone_wq1
            else: 
                logger.warning(f'create_primary_event_tags_dict(): Event {eventid} has multiple tags but no primary event tag has been defined.')
        else:
            primary_event_tags[eventid] = None       
            # logger.warning(f'create_primary_event_tags_dict(): Event {eventid} has no tags.')
    
    return primary_event_tags
    
def pecos_check_missing(pm, event_tags):
    """
    Apply Pecos check for missing data to the event tags.

    Args:
        pm (pecos.monitoring.Monitor): Pecos monitoring object.
        event_tags (list): List of event tags.

    Returns:
        pm (pecos.monitoring.Monitor): Pecos monitoring object.
    """
    for tag in event_tags:
        pm.check_missing(tag)

    return pm
    
def facetplot_by_tag(df):
    """
    Create a multi-panel plot (facet plot) of the data.

    Args:
        df (pandas.DataFrame): Dataframe in tall format with columns: Datetime, Tag, Value.

    Returns:
        None.
    """

    # Create the FacetGrid
    g = sns.FacetGrid(data=df, col='Tag', hue='Tag', col_wrap=1, aspect=2.5, sharey=False)
    g.map_dataframe(sns.lineplot, x='Datetime', y='Value')
    g.set_axis_labels("", "")
    g.add_legend()

    # Rotate x-axis labels in FacetGrid
    for ax in g.axes.flat:
        for label in ax.get_xticklabels():
            label.set_rotation(90)

# Folder management functions
def reset_directory(folder_path):
    """
    Deletes all files and subfolders in the specified folder.
    If the folder does not exist, creates one.
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    else:
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    reset_directory(file_path)
                    os.rmdir(file_path)
            except Exception as e:
                logger.warning(f"Error deleting {file_path}: {e}")