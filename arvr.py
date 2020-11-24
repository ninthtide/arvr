# arvr.py
# Development script for extracting data from OSI PI, send to TCS ARVR solution (eventually)
#   Queries OSI PI
#   Updates ARVR platform

DEBUG_OUTPUT = True

ASSETS_FILENAME = 'assets-arvr.json'

import pyodbc
import datetime
import time
import requests
import csv

# We have to use simplejson and not the default json library as the default one fails to encode decimal values
import simplejson as json

# OSI PI
from requests.auth import HTTPBasicAuth
# Fix to irritating message about unverified HTTPS request.  We have to do this because the firewall root certificate has not been loaded on to SVINOTEST03
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


# OSI PI credentials
OSI_URL_STEM = 'https://sviotprod01/piwebapi/'

#OSI_ASSET_SERVER = 'ODS-AF'     # is this now just 'ODS'?
OSI_ASSET_SERVER = 'ODS-AF'     # is this now just 'ODS'?
OSI_AF_DATABASE = 'ODS'
# OSI_SERVER_NAME = 'ODS'
OSI_DECIMALS = 3                    # How many decimal places to send to time series / graphed variables (RDS)
osi_header = {'content-type': 'application/json', 'X-Requested-With': 'XmlHttpRequest'}
OSI_USERNAME = 'prototype.service'
OSI_PASSWORD = 'rgKRa7csvT'


# For reference
#elements_url = '{}elements?path=\\\\{}\\{}\\{}'.format(OSI_URL_STEM, OSI_ASSET_SERVER, OSI_AF_DATABASE, asset['ositag'])

# Repeating options
run_forever = False                 # Set to True if you want the program to run itself on repeat every 2 minutes, rather than being triggered externally
OSI_INTERVAL_SECONDS = 120          # How many seconds between each poll - only applies if run_forever = True

# The script is currently scheduled in Windows Task Scheduler to run every 2 minutes.
# Windows credentials are:
# Local user: 'Ecodomus'
# Password:    3coDomu$robot

security_auth = HTTPBasicAuth(OSI_USERNAME, OSI_PASSWORD)

# IMPORTANT NOTE ABOUT PROXIES
#
# pip requires that we set two env variables to use the Melbourne Water proxy servers, namely:
#   http_proxy
#   https_proxy
#
# By default, requests will use these variables.  However we cannot connect to Ecodomus via these proxies
# Instead, we have to establish a requests session and set .trust_env to False, to bypass the proxies

session = requests.Session()
session.trust_env = False





# -----------------------------------------------------------------------------------------------

# SUPPORT FUNCTIONS

# Stub for posting to ARVR solution, when ready
def get_bearer():
    response = session.post(
        ECODOMUS_URL_STEM + 'token',
        headers = {
            'Content-Type' : 'application/x-www-form-urlencoded',
            'Accept' : 'application/json'
        },
        data = {
            'client_id' : ECODOMUS_CLIENT_ID,
            'client_secret' : ECODOMUS_CLIENT_SECRET,
            'username' : ECODOMUS_USERNAME,
            'password' : ECODOMUS_PASSWORD,
            'grant_type' : 'password'
        }
    )
    json_response = response.json()
    bearer_token = json_response['access_token']
    return bearer_token



# --------------------------------


# PERFORM INITIALISATION TASKS


# Define loadlist
with open(ASSETS_FILENAME, 'r') as f:
    assets_dict = json.load(f)

first_row = True




# --------------------------------


# MAIN ROUTINE

# RUNS INDEFINITELY

running = True

while running:

    # Capture start time.  The process takes about 23 seconds, however we want it to start every 2 mins, as defined in OSI_INTERVAL_SECONDS
    # This ensures we get consistent data points on the graphs in Ecodomus

    start_time = datetime.datetime.now()

    for asset in assets_dict:

        # If you wish to debug a particular asset, set its ID here
        if asset['arvr_id'] == 'FX00990' and DEBUG_OUTPUT:
            debug = True
        else:
            debug = False

            
        
        # Check if we have an OSI PI element tag defined; if so, we can extract their values
        # The process goes:
        # ... look up elements using the ositag
        # ... if not 'grouped', get the attributes of the tag
        # ... if 'grouped', look up the elements of the tag, then get its attributes
        # then for each attribute, use its WebId to get the actual final value
        # process the data type of the value e.g. text values are dict, others are floats etc
        

        if 'ositag' in asset:
            print('Asset: ', asset['arvr_id'] ,' OSI TAG:',asset['ositag'])


            # elements_url = '{}elements?path=\\\\{}\\{}\\{}'.format(OSI_URL_STEM, OSI_ASSET_SERVER, OSI_AF_DATABASE, asset['ositag'])
            elements_url = '{}elements?path={}'.format(OSI_URL_STEM, asset['ositag'])
            response = session.get(elements_url, auth=security_auth, verify=False)


            if response.status_code==200:
                data = json.loads(response.text)

            else:
                print('Error in retrieving elements from OSI asset', asset)
                print(response.text)



            # Get WebId of attributes

            response = session.get(OSI_URL_STEM + '/elements/'+data['WebId'] + '/attributes',auth = security_auth, verify=False)



            # Some OSI tags do not show the individual attributes, but instead groups of attributes.  If so, get the elements of the group
            if 'grouped' in asset:
                if asset['grouped']:
                    # Start off by getting the attributes of the (grouped) OSI tag itself
                    response = session.get(OSI_URL_STEM+'/elements/'+data['WebId'] + '/attributes', auth = security_auth, verify=False)    # Grouped
                    groups_attr = json.loads(response.content)
                    groups = groups_attr['Items']
                    # Look for the group that matches the requested name
                    group_id = [group for group in groups if group['Name'] == asset['osi_group_name']]
                    if debug:
                        print(OSI_URL_STEM+'/elements/'+group_id[0]['WebId'] + '/attributes')
                    # Resubmit the query for attributes, but this time the ID is group_id[0], i.e. we found the group that matches the name defined in assets.json (e.g. "PMP")
                    response = session.get(OSI_URL_STEM+'/attributes/'+group_id[0]['WebId'] + '/attributes', auth = security_auth, verify=False)      # Not grouped

                else:
                    # Not grouped; can query the tag directly for attributes
                    response = session.get(OSI_URL_STEM+'/elements/'+data['WebId'] + '/attributes', auth = security_auth, verify=False)      # Not grouped    
            else:
                response = session.get(OSI_URL_STEM+'/elements/'+data['WebId'] + '/attributes', auth = security_auth, verify=False)      # Not grouped


            attr = json.loads(response.content)
            items = attr['Items']



            # We check for valid OSI PI attributes and convert them to Ecodomus field names using various dictionaries
            # valid_items - specifies which attributes we will process, so if we find something we don't expect or recognise, it will be skipped
            # floats - which fields should be represented as floating point values
            # ints - which fields should be represented as integers
            # suffixes - what suffix to append to each field
            # rds_items - what tags should additionally be passed to the RDS archive; i.e. they are time series / graphed values

            valid_items = {'Value'} 
            
            # Iterate through all items
            for item in items:
                if item['Name'] in valid_items:
                    item_webid = item['WebId']
                    # Get the individual value from OSI PI
                    response = session.get(OSI_URL_STEM+'/streams/' + item_webid + '/value', auth = security_auth, verify=False)
                    vals = json.loads(response.text)

                    
                    if debug or True:
                        print(item['Name'], vals['Timestamp'], vals['Value'], type(vals['Value']))

                    

                    
                    arvr_payload = {
                        # Check the valid_items dictionary.  If item['Name'] is CUR, this will return 'Current', which we can then look up in field_id
                        "Id" : item['Name'],
                        "Value" : str(vals['Value'])
                    }


                    # Post the payload
                    if False:
                        response = session.post(
                            ARVR_URL_STEM + 'remote/rds/archive',
                            headers = {
                                'Content-Type' : 'application/json',
                                'clientname' : 'EcoDomus',
                                'Authorization' : 'bearer ' + bearer_token,

                            },
                            json = json.loads(rds_payload)      # Note converstion to JSON here
                        )

                        if response.status_code==200:
                            data = json.loads(response.text)

                            if debug:
                                print(json.dumps(data, indent = 3))
                        else:
                            print('ERROR: Error in posting to RDS archive', asset['max_location'])
                            print(response.text)




# --------------------------------------------------------------------



    end_time = datetime.datetime.now()
    duration_time = end_time - start_time
    print('Process complete, took:', str(int(duration_time.total_seconds())), 's.')

    

    if run_forever:
        print('Program is in "run forever" mode. Sleeping for next execution')
        time.sleep(OSI_INTERVAL_SECONDS - duration_time.total_seconds())
    else:
        # Program is in "run once" mode
        running = False

        

print('Process complete.')

