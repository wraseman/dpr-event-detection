
"""
Create dashboard to identify events in direct potable reuse (DPR) systems.
Author(s): Billy Raseman and Nolan Townsend

Description: script to be deployed at the City of San Diego Pure Water North City DPR Demonstration Facility 
as part of Water Research Foundation (WRF) Project 4954. This script uses data from the demonstration facility and open 
source Python packages to create a dashboard that identifies events in the DPR system. The dashboard is intended to be
used by operators to identify events and to help inform the development of an automated event detection system.

Acknowledgements: this project is a collaboration between Hazen & Sawyer and Trussell Technologies with support from 
the City of San Diego and WRF.
"""

# Import functions from other scripts
from datetime import datetime, timedelta
from library import *

# Common Python packages
from loguru import logger
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import os
import pecos
import json
from plyer import notification

# Pecos is a python package for performing automated quality control of time series data.
# It is designed to work with data stored in pandas DataFrames and Series.
# Pecos contains tools for:
# 1. Automatically detecting and flagging anomalies in time series data
# 2. Calculating performance metrics for time series data
# 3. Generating summary statistics and plots for time series data
# 4. Generating HTML reports for time series data
# 5. Performing quality control on time series data

############################################# USER INPUTS #############################################
## SQL database connection information
# NOTE REMOVED DATABASE CONNECTION INFO
server = 
database = 
username = 
password = 
driver = 
table = 
datetime_col = 

## User defined run mode and outputs
save_plots = True  # save plots as png files
notify = True  # send notifications
# run_now = False  # run script in live mode (True) or historical mode (False)

## User defined paths
path_config = r"C:\Users\WRaseman\OneDrive - Hazen and Sawyer\Projects - Active\70047-000 WRF 4954 Reuse Data\Data Analysis\2023-06-05 Deployment\config.xlsx"
results_directory = 'example_1'  # name of results directory
path_working = os.getcwd()
path_events_json = os.path.join(path_working, 'events.json')

# ## User defined parameters
# ## Note: most data beings the week of September 12, 2022. 
# ##  Additional tags for ozone and RO were added the week of March 20, 2023 and in June 2023. 
# user_datetime_start = "5/24/2023 12:00:00"  # start datetime for historical mode
# user_datetime_end = "5/24/2023 14:00:00"  # end datetime for historical mode

############################################# END USER INPUTS #########################################

def main (): 

    # # Use current datetime if run_now == True, otherwise use user input
    # if run_now == True:
    #     datetime_start_init = datetime.now() - timedelta(days=0.5)  # start datetime for live mode
    #     datetime_start_round = datetime_start_init.replace(second=0, microsecond=0)
    #     datetime_start = datetime.strftime(datetime_start_round, '%Y-%m-%d %H:%M:%S')

    #     datetime_end_init = datetime.now()  # end datetime for live mode
    #     datetime_end_round = datetime_end_init.replace(second=0, microsecond=0)
    #     datetime_end = datetime.strftime(datetime_end_round, '%Y-%m-%d %H:%M:%S')

    # else:  # user input for historical mode
    datetime_start = user_datetime_start
    datetime_end = user_datetime_end

    # Check that datetime_start and datetime_end are in the correct format
    datetime_start = check_datetime_format(datetime_start)
    datetime_end = check_datetime_format(datetime_end)

    # Check that datetime_start is before datetime_end
    check_datetime_order(datetime_start, datetime_end)

    # Read in configuration file data
    logger.info('Reading data from configuration file.')

    ## Read in Tags and Events tables from config file
    df_tags = pd.read_excel(path_config, sheet_name='Tags')
    check_tags_table(df_tags)
    df_events = pd.read_excel(path_config, sheet_name='Events')
    check_events_table(df_events)

    ## Create engine to connect to project's SQL database 
    logger.info('Creating engine for SQL database.')
    engine = create_engine(driver, server, database, username, password)

    ## Check for TagIDs in config file that are not in the database
    check_tagids_missing_from_sql(engine, df_tags, table, datetime_col, datetime_start, datetime_end)

    ## Convert Tag and TagID columns to a dictionary
    tag_dict = dict(zip(df_tags['TagID'], df_tags['Tag (Units)']))

    ## Convert Event and EventID columns to a dictionary
    event_dict = dict(zip(df_events['EventID'], df_events['TagIDs']))
    event_dict = convert_items_to_list_of_ints(event_dict) # convert items to list of ints
    event_flags = dict.fromkeys(range(1, len(event_dict) + 1), False) # initialize event flags to False

    # Read in data from SQL database

    ## Only include datetime range that is specified
    tagid_set = set(tag_dict.keys())  # only include TagIDs that are in the config file
    logger.info(f'Querying data from SQL database between {datetime_start} and {datetime_end}.')
    df = create_df_from_sql(engine, table, datetime_start, datetime_end, tagid_set, datetime_col)

    # Add Tag to SQL data based on TagID using the tag_dict dictionary
    df['Tag'] = df['TagID'].map(tag_dict)
    df1 = df[['Datetime', 'Tag', 'Value']]  # only keep Datetime, Tag, and Value columns

    # Pivot dataframe to wide format (required for Pecos logic)
    ## Wide format: each Tag is a column (e.g., 'RO Feed TOC', 'UV Dose', etc.)
    df_wide = df1.pivot(index='Datetime', columns='Tag', values='Value').reset_index()
    df_wide = df_wide.set_index('Datetime')

    # Check for missing columns in dataframe

    ## Begin with production status tags
    logger.info('Checking for missing production status tags in dataframe.')

    tag_plant_status = tag_dict[43]
    tag_mf_status = tag_dict[64]
    tag_bac1_status = tag_dict[48]
    tag_bac2_status = tag_dict[49]

    status_tags = [tag_plant_status, tag_mf_status, tag_bac1_status, tag_bac2_status]

    # Check for missing columns for each production status tag
    check_for_missing_status_tag(df_wide, tag_plant_status)  # Plantwide status
    check_for_missing_status_tag(df_wide, tag_mf_status)  # MF status
    check_for_missing_status_tag(df_wide, tag_bac1_status)  # BAC1 status
    check_for_missing_status_tag(df_wide, tag_bac2_status)  # BAC2 status

    ## Next check value tags
    logger.info('Checking for missing value tags in dataframe.')

    # Check for missing columns for each value tag. If all tags are missing, then set event flag to True.
    for eventid in event_dict:
        tagids = event_dict[eventid]
        for tagid in tagids:
            check_for_missing_value_tag(df_wide, tag_dict, tagid)
        event_flags[eventid] = check_all_tags_missing_for_event(df_wide, tag_dict, event_dict[eventid])

    # For tags related to error or difference calculations, modify the data to be the absolute value of error/difference. 
    # This is to ensure that the data is always positive to avoid errors in the Pecos logic.
    tags_err_diff = [tag_dict[86], tag_dict[67], tag_dict[91], tag_dict[92]]
    logger.info(f'Modifying data for error/difference tags. Calculating absolute value for {tags_err_diff}.')

    for tag in tags_err_diff:
        df_wide[tag] = df_wide[tag].abs()

    # RO events: create calculated columns for RO events
    logger.info('Creating calculated columns for RO events.')
    name_ro_process = 'RO Process Event'  # column name for new tag
    name_ro_wq1 = 'RO Water Quality Event'
    name_ro_monitoring = 'RO Monitoring Event'

    # Create new column for RO Process event:
    # If RO Combined Permeate TOC > 50 ppb & RO Train A Permeate Conductivity > 125 ppb & RO Train B Permeate Conductivity > 125 ppb, 
    # then RO Process = 1, else 0. 
    cond1 = (df_wide[tag_dict[15]] > 50) | (df_wide[tag_dict[15]].isna())
    cond2 = (df_wide[tag_dict[29]] > 125) | (df_wide[tag_dict[29]].isna())
    cond3 = (df_wide[tag_dict[31]] > 125) | (df_wide[tag_dict[31]].isna())
    df_wide[name_ro_process] = np.where(cond1 & cond2 & cond3, 1, 0)
    del cond1, cond2, cond3

    # Create new column for RO Monitoring event:
    # If RO Feed TOC < 3780 ppb & RO LRV via TOC < 2.1, then RO Monitoring = 1, else 0. 
    cond1 = (df_wide[tag_dict[1]] < 3780) | (df_wide[tag_dict[1]].isna())
    cond2 = (df_wide[tag_dict[17]] < 2.1) | (df_wide[tag_dict[17]].isna())
    df_wide[name_ro_monitoring] = np.where(cond1 & cond2, 1, 0)
    del cond1, cond2

    # Create new column for RO Water Quality event:
    # If RO Combined Permeate TOC > 50 ppb & RO Train A Permeate Conductivity < 125 ppb & RO Train B Permeate Conductivity < 125 ppb,
    # then RO Water Quality = 1, else 0.
    cond1 = (df_wide[tag_dict[15]] > 50) | (df_wide[tag_dict[15]].isna())
    cond2 = (df_wide[tag_dict[29]] < 125) | (df_wide[tag_dict[29]].isna())
    cond3 = (df_wide[tag_dict[31]] < 125) | (df_wide[tag_dict[31]].isna())
    df_wide[name_ro_wq1] = np.where(cond1 & cond2 & cond3, 1, 0)
    del cond1, cond2, cond3

    # Ozone events: create calculated columns for Ozone events
    logger.info('Creating calculated columns for Ozone events.')
    name_ozone_wq1 = 'Ozone Water Quality Event'

    # Create new column for Ozone Water Quality event:

    # Write code to create a tictactoe game
    

    # If OSP 4 Hach Meter and rolling average difference is LESS than 10%, 
    # and OSP 7 Hach Meter and rolling average difference is LESS than 10%,
    # and ozone production error is LESS than 10%, 
    # and ozone demand is greater than 6.0 mg/L, 
    # then Ozone Water Quality = 1, else 0.
    # If any values are empty, assume they violate the condition.
    cond1 = (df_wide[tag_dict[91]] < 0.1)  # don't apply missing value logic here
    cond2 = (df_wide[tag_dict[92]] < 0.1)  # don't apply missing value logic here
    cond3 = (df_wide[tag_dict[86]] < 0.1)  # don't apply missing value logic here
    cond4 = (df_wide[tag_dict[59]] > 6.0) | (df_wide[tag_dict[59]].isna())
    df_wide[name_ozone_wq1] = np.where(cond1 & cond2 & cond3 & cond4, 1, 0)
    del cond1, cond2, cond3, cond4

    # Create dashboard based on Pecos results
    dashboard_content = {} # Initialize the dashboard content dictionary
    logger.info('Implementing Pecos tests.')

    # Create dictionary of primary event tags (the ones that are used to determine if an event is occurring)
    primary_event_tags = create_primary_event_tags_dict(event_dict, tag_dict, name_ro_process, name_ro_monitoring, name_ro_wq1, name_ozone_wq1)

    # Loop through the events in df_events
    list_detected_events = [] # Initialize list of detected events
    num_events = df_events.shape[0]
    for i in range(0, num_events):

        ## Get values for this EventID from df_events
        eventid = df_events['EventID'][i]
        event_text = df_events['Event Text'][i]
        event_process = df_events['Event Process'][i]
        event_type = df_events['Event Type'][i]

        ## Get tags for this event from dictionary
        event_tagids = event_dict[eventid]
        if len(event_tagids) == 0:
            is_empty = True
        else:
            is_empty = False
        event_tags = [tag_dict[tagid] for tagid in event_tagids]
        
        ## For events with calculated columns, add the calculated column name to the list of tags (in front)
        if eventid == 5:
            event_tags.insert(0, name_ro_process)
        elif eventid == 6:
            event_tags.insert(0, name_ro_monitoring)
        elif eventid == 7:
            event_tags.insert(0, name_ro_wq1)
        elif eventid == 15:
            event_tags.insert(0, name_ozone_wq1)

        ## Initialize event flag
        if is_empty == True:
            # If event is not applicable just store placeholder text
            content = { 'text': event_text }
            dashboard_content[(event_process, event_type)] = content
        else: 
            # Create new Pecos PerformanceMonitoring object
            pm = pecos.monitoring.PerformanceMonitoring()
            event_and_status_tags = event_tags + status_tags  # only keep value tags needed for the event plus all status tags
            df_wide_event = df_wide[event_and_status_tags]  # subset data to only include tags for this event
            pm.add_dataframe(df_wide_event)  # add data to Pecos object

            ## Timefilter
            time_filter_system = pm.data[tag_plant_status] == 1
            pm.add_time_filter(time_filter_system)  # add time filter when plantwide status is not 1 (normal)

            ## Check missing data
            pm = pecos_check_missing(pm, event_tags)

            # Apply process-specific time filters
            if event_process == 'MF':
                # Filter based on MF process status (if not in production)
                time_filter_process1 = pm.data[tag_mf_status] == 1
                pm.add_time_filter(time_filter_process1)
            elif event_process == 'Ozone':
                # Filter based on BAC process status (if not in production)
                time_filter_process1 = pm.data[tag_bac1_status] == 1
                pm.add_time_filter(time_filter_process1)
                time_filter_process2 = pm.data[tag_bac2_status] == 1
                pm.add_time_filter(time_filter_process2)

            # Apply Pecos tests for each EventID
            if eventid == 1: 
                # MF Process
                pm.check_range(key=tag_dict[10], bound=[None, 0.15], min_failures=15)

            elif eventid == 2: 
                # MF Monitoring
                pm.check_increment(key=tag_dict[10], bound=[0.0001, None], min_failures=30)

            elif eventid == 5: 
                # RO Process event
                pm.check_range(key=name_ro_process, bound=[None, 0], min_failures=30)
                pm.check_range(key=tag_dict[15], bound=[None, 50], min_failures=30) 
                pm.check_range(key=tag_dict[29], bound=[None, 125], min_failures=15) 
                pm.check_range(key=tag_dict[31], bound=[None, 125], min_failures=15)

            elif eventid == 6: 
                # RO Monitoring event
                pm.check_range(key=name_ro_monitoring, bound=[None, 0], min_failures=30) 
                pm.check_range(key=tag_dict[1], bound=[3780, None], min_failures=30)  
                pm.check_range(key=tag_dict[17], bound=[2.1, None], min_failures=30)  
        
            elif eventid == 7:
                # RO Water Quality 1 event
                pm.check_range(key=name_ro_wq1, bound=[None, 0], min_failures=30) 
                pm.check_range(key=tag_dict[15], bound=[None, 50], min_failures=30)
                pm.check_range(key=tag_dict[29], bound=[125, None], min_failures=5)
                pm.check_range(key=tag_dict[31], bound=[125, None], min_failures=5)  

            elif eventid == 9: 
                # UVAOP Process
                pm.check_range(key=tag_dict[40], bound=[300, None], min_failures=5) 

            elif eventid == 10:
                # UVAOP Monitoring
                # pm.check_increment(key=tag_dict[65], bound=[0.01, None], min_failures=15)  / in documentation
                pm.check_increment(key=tag_dict[65], bound=[0.01, None], min_failures=30) 

            elif eventid == 11:
                # UVAOP Water Quality 1
                pm.check_range(key=tag_dict[32], bound=[96, None], min_failures=15) 

            elif eventid == 12:
                # UVAOP Water Quality 2
                pm.check_range(key=tag_dict[83], bound=[None, 1], min_failures=15) 

            elif eventid == 13:
                # Ozone Process
                pm.check_range(key=tag_dict[86], bound=[None, 0.05], min_failures=15)

            elif eventid == 14:
                # Ozone Monitoring
                pm.check_range(key=tag_dict[67], bound=[None, 0.1], min_failures=15)

            elif eventid == 15:
                # Ozone Water Quality 1
                pm.check_range(key=name_ozone_wq1, bound=[None, 0], min_failures=15)
                pm.check_range(key=tag_dict[91], bound=[0.1, None], min_failures=2)
                pm.check_range(key=tag_dict[92], bound=[0.1, None], min_failures=2)
                pm.check_range(key=tag_dict[86], bound=[0.1, None], min_failures=2)
                pm.check_range(key=tag_dict[59], bound=[None, 6.0], min_failures=15)
            else:
                logger.warning(f'Incorrect logic for EventID {eventid}')

            # Compute metrics
            mask = pm.mask[event_tags]
            QCI = pecos.metrics.qci(mask, pm.tfilter)

            # If QCI is less than 1, then flag the event
            if QCI[primary_event_tags[eventid]] < 1:
                event_flags[eventid] = True

            # Define output files and subdirectories for this event
            results_subdirectory = os.path.join(results_directory, event_process+'_'+event_type)
            reset_directory(results_subdirectory)  # reset directory if it already exists. If it doesn't exist, create it.
            graphics_file_rootname = os.path.join(results_subdirectory, 'test_results')
            custom_graphics_file = os.path.abspath(os.path.join(results_subdirectory, 'custom.png'))
            test_results_file = os.path.join(results_subdirectory, 'test_results.csv')
            colorblock_graphics_file = os.path.abspath(os.path.join(results_subdirectory, 'colorblock.png'))
            report_file =  os.path.join(results_subdirectory, 'monitoring_report.html')

            # Create plots
            test_results_graphics = pecos.graphics.plot_test_results(pm.data, pm.test_results,
                                        pm.tfilter, filename_root=graphics_file_rootname)
            
            # Log event and modify colorblock if event is occurring
            if event_flags[eventid]:
                logger.critical(f'Alert: {event_text} event is occurring, take action!')
                list_detected_events.append(f'{event_text}')
                color = 0  # fill in colorblock if event is occurring
            else:
                color = 1  # otherwise, keep colorblock gray

            # Create colorblock plot
            pecos.graphics.plot_heatmap(pd.Series(color), vmin=0.9999, vmax=1, cmap=mpl.colors.ListedColormap(['magenta','lightgray']))  # colorblock (magenta or gray)
            plt.savefig(colorblock_graphics_file, dpi=90, bbox_inches='tight', pad_inches = 0.1)
            
            # Create timeseries plot each tag in the event to show the raw data
            if save_plots:
                df_plot = df1[df1['Tag'].isin(event_tags)]

                # If df_plot is empty, create a dummy plot
                if df_plot.empty:  # If no data, create an empty png
                    fig, ax = plt.subplots()
                    ax.axis('off')
                    ax.axis('tight')
                    ax.text(0.5, 0.5, 'No data to display', horizontalalignment='center', verticalalignment='center', transform=ax.transAxes)
                else:  # Create a facet plot of the data
                    facetplot_by_tag(df_plot)

                plt.savefig(custom_graphics_file, format='png', dpi=250, bbox_inches='tight')

            # Write test results and report files
            pecos.io.write_test_results(pm.test_results, test_results_file)
            pecos.io.write_monitoring_report(data=pm.data, 
                                             test_results=pm.test_results, 
                                             test_results_graphics=test_results_graphics,
                                             custom_graphics=[custom_graphics_file], 
                                             metrics=QCI, 
                                             title=event_text,
                                             filename=report_file)
            
            # Close plots
            plt.close('all') 

            ## Write CSS to report file
            with open(report_file, 'a') as f:
                css_file = os.path.join(path_working, 'style2.css')
                html_content = f'<link rel="stylesheet" type="text/css" href="{css_file}">\n'
                logo_file = os.path.join(path_working, 'logo_all.png')
                html_content += f'<div style="height: 50px; display: flex; flex-direction: row; align-items: center; justify-content: flex-start;"> <img style="height: 50px; width: 355px !important;" src="{logo_file}" alt="logo"></div>\n'
                f.write(html_content)

            # Store content to be displayed in the dashboard
            content = { 'text': event_text,
                        'graphics': [colorblock_graphics_file],
                    'link': {'Link to Report': os.path.abspath(report_file)}}
            dashboard_content[(event_process, event_type)] = content

    # Create/update dashboard 
    logger.info('Writing Pecos results to dashboard.')  
    events = ['Process', 'Monitoring', 'Water Quality 1', 'Water Quality 2']
    processes = ['MF', 'RO', 'UVAOP', 'Ozone']
    dashboard_file = f'dashboard_{results_directory}.html'

    pecos.io.write_dashboard(column_names=events, row_names=processes, 
                             content=dashboard_content, title='Direct Potable Reuse Monitoring Dashboard',
                            filename=dashboard_file)

    ## Write CSS to dashboard file
    path_dashboard_file = os.path.join(path_working, dashboard_file)
    with open(path_dashboard_file, 'a') as f:
        css_file = os.path.join(path_working, 'style1.css')
        html_content = f'<link rel="stylesheet" type="text/css" href="{css_file}">\n'
        logo_file = os.path.join(path_working, 'logo_all.png')
        html_content += f'<div style="height: 50px; display: flex; flex-direction: row; align-items: center; justify-content: flex-start;"> <img style="height: 50px; width: 355px !important;" src="{logo_file}" alt="logo"></div>\n'
        f.write(html_content)

    # Send notification if any new events have occurred since last time script was run
    logger.info('Checking event flags and sending notifications, if necessary.')
    if notify == True:  # only send notifications if script is running live
        # Read in events from previous run
        if os.path.exists(path_events_json):
            with open(path_events_json, 'r') as f:
                events_json = json.load(f)
        else:
            events_json = {'events': []}

        # Check if any new events have occurred
        list_new_events = []
        for event in list_detected_events:
            if event not in events_json['events']:
                list_new_events.append(event)

        # If new events have occurred, send notification
        if len(list_new_events) > 0:
            logger.info('New event(s) detected. Sending notification.')
            message = f"New event(s) detected. Refresh the dashboard to review the events: {list_new_events}"
            notification.notify(title="Alert!", message=message)
            events_json['events'] = list_detected_events  # update events_json
            with open(path_events_json, 'w') as f:
                json.dump(events_json, f)
        else:
            logger.info('No new events have occurred since last time script was run.')

if __name__ == '__main__':
    
    # Initialize logger in current working directory
    path_log = os.path.join(path_working, 'dashboard.log')
    logger.add(path_log, retention='365 days', level='INFO', rotation='30 days', compression='zip')

    # Run the main function for 24 hour time periods
    # This is to test the script for a full month of data.
    # After running the script each time, pause for user input to continue to the next time period.

    start_datetime = datetime(2023, 5, 18, 0, 0, 0)
    end_datetime = datetime(2023, 5, 18, 23, 59, 59)

    # For loop to run the main function for each 24 hour time period
    for i in range(31):

        # Convert datetime to "YYYY-MM-DD HH:MM:SS" format
        user_datetime_start = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
        user_datetime_end = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

        # Run main function
        main()

        # Pause for user input to continue to the next time period
        input("Press Enter to continue to next time period...")

        # Increment start and end datetimes by 24 hours
        start_datetime += timedelta(days=1)
        end_datetime += timedelta(days=1)