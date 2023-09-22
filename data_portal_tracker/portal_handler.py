# Importing necessary packages
import os
import json
import requests
import datetime
import pandas as pd
from urllib.parse import urlparse
from helpers import check_protocol
from IPython.display import display


def extract_search_results(search_results_folder: str, output_file: str):
    """Extracting the URLs of organic search results from the saved JSON files

    Code for looping through the search results was adapted from https://github.com/semantisch/crawley (© Daniil Dobriy)

    Args:
        ``search_results_folder (str):`` the path of the folder in the crawley-lite directory containing the search results 

        ``output_file (str):`` the path of the CSV file to be exported
    """

    # Setting the folder name
    folder = search_results_folder

    # Creating a dataframe for the portal URLs
    search_results = pd.DataFrame(columns = ["url"])

    # Looping through the result files
    for filename in os.listdir(folder):
        filepath = os.path.join(folder, filename)
        result_file = json.load(open(filepath, 'r', encoding='utf-8'))
        # Considering only organic results
        if "organic_results" in result_file:
            # Add the organic results to the dataframe
            for organic_result in result_file["organic_results"]:
                search_results.loc[len(search_results)] = organic_result["link"]

    # print("Number of result files:", len(os.listdir(folder)))
    # print("Number of organic search results:", len(search_results))

    # Exporting the list as a CSV file
    search_results.to_csv(output_file, index = None)


def create_list(search_results_file: str, output_file: str):
    """Creating an initial list of portal URLs based on multiple sources

    Args:
        ``search_results_file (str):`` the path of the CSV input file containing search result URLs in a column "url"

        ``output_file (str):`` the path of the CSV file to be exported
    """

    # Creating a dataframe for the portal URLs
    initial_portals = pd.DataFrame(columns = ["url"])

    # Source 1 - Downloading portal lists and deserializing JSON
    list_1_url = "https://data.opendatasoft.com/api/explore/v2.1/catalog/datasets/open-data-sources@public/exports/json"
    list_2_url = "https://dataportals.org/api/data.json"

    list_1_response = requests.get(list_1_url)
    list_2_response = requests.get(list_2_url)

    list_1 = json.loads(list_1_response.content)
    list_2 = json.loads(list_2_response.content)

    # Source 2 - Manual additions to the list
    additional_portals = [
        {"name": "OEBB", "url": "https://data.oebb.at"},
        {"name": "DB", "url": "https://data.deutschebahn.com"},
        {"name": "RENFE", "url": "https://data.renfe.com"},
        {"name": "SNCF", "url": "https://data.sncf.com"},
        {"name": "INFRABEL", "url": "https://opendata.infrabel.be"},
        {"name": "SBB", "url": "https://data.sbb.ch"},
        {"name": "PRORAIL", "url": "https://prorailnl.hub.arcgis.com"},
        {"name": "Stanford", "url": "https://stanfordopendata.org"},
        {"name": "Universidad de Alicante", "url": "https://transparencia.ua.es"},
        {"name": "University of Southampton", "url": "https://data.southampton.ac.uk"},
        {"name": "California State University", "url": "http://opendata.calstate.edu"},
        {"name": "Department of Education", "url": "https://data.ed.gov"},
        {"name": "University of Chicago", "url": "https://ucopendata.netlify.app"},
        {"name": "University of Oxford", "url": "https://data.ox.ac.uk"},
        {"name": "University of Edinburgh", "url": "https://datashare.ed.ac.uk"},
    ]

    # Source 3 - Old portals from Open Data Portal Watch
    portalwatch_portals = pd.read_csv("data/portalwatch_portals.csv")
    portalwatch_portals = portalwatch_portals["portal_url"].values.tolist()

    # Source 4 - Results from search engine queries
    search_results = pd.read_csv(search_results_file)
    search_results = search_results["url"].values.tolist()

    # Adding all portals to the dataframe
    for portal in list_1:
        initial_portals.loc[len(initial_portals)] = portal["url"]

    for portal in list_2:
        initial_portals.loc[len(initial_portals)] = list_2[portal]["url"]

    for portal in additional_portals:
        initial_portals.loc[len(initial_portals)] = portal["url"]

    for portal in portalwatch_portals:
        initial_portals.loc[len(initial_portals)] = portal

    for portal in search_results:
        initial_portals.loc[len(initial_portals)] = portal

    # Basic first deduplication
    initial_portals = initial_portals.drop_duplicates(ignore_index = True)

    # Sorting the list
    initial_portals = initial_portals.sort_values("url", ignore_index = True)

    # Exporting the list as a CSV file
    initial_portals.to_csv(output_file, index = None)


def remove_duplicates(initial_portals_file: str, output_file: str):
    """Removing duplicates from an input list of URLs

    Args:
        ``initial_portals_file (str):`` the path of the CSV input file containing initial portal URLs in a column "url"
        
        ``output_file (str):`` the path of the CSV file to be exported
    """

    # Opening the file that contains the initial portal URLs
    initial_portals = pd.read_csv(initial_portals_file)

    # Looping through the portal URLs
    for index, portal in initial_portals.iterrows():
        # Removing slashes and number signs at the end of the URL
        while (portal["url"].endswith("/") or portal["url"].endswith("#")):
            initial_portals.loc[index, "url"] = portal["url"].rstrip("/").rstrip("#")

        # Removing whitespace and quotes around the URL
        initial_portals.loc[index, "url"] = portal["url"].strip().strip('\"').strip("\'")

        # Shortening the URL to the base URL and removing the HTTP(S) protocol prefix
        initial_portals.loc[index, "url"] = urlparse(portal["url"]).netloc

    # Saving the duplicates to a dataframe and exporting it as a CSV file
    # duplicate_portals = initial_portals[initial_portals.duplicated()].sort_values(by=["url"])
    # duplicate_portals.to_csv("data/duplicate_portals.csv", index = None)

    # Saving the unique values to a dataframe and exporting it as a CSV file
    deduplicated_portals = initial_portals.drop_duplicates(ignore_index = True).sort_values(by=["url"])
    deduplicated_portals.to_csv(output_file, index = None)


def add_api_endpoints(manual_api_additions_file: str, deduplicated_portals_file: str, output_file: str):
    """Adding known portal API endpoints that are not reachable from the base URL ("/api") but via a different path (like "/catalog/api") or do not have any HTML markers

    Args:
        ``manual_api_additions_file (str):`` the path of the CSV input file containing API base URLs without "/api/..." in a column "url", e.g. "data.gv.at/katalog", and the API software name or "Unknown" in a column "manually_checked_api"
        
        ``deduplicated_portals_file (str):`` the path of the CSV input file containing deduplicated portal URLs in a column "url"

        ``output_file (str):`` the path of the CSV file to be exported
    """

    # Opening the file that contains the API endpoints
    manual_api_additions = pd.read_csv(manual_api_additions_file)

    # Opening the file that contains the unique / deduplicated portal URLs (without protocol prefixes)
    deduplicated_portals = pd.read_csv(deduplicated_portals_file)

    # Adding the API endpoints to the portal list
    extended_portals = pd.concat([deduplicated_portals, manual_api_additions], ignore_index = True).drop_duplicates().sort_values(by = "url")

    # Exporting the extended list to a CSV file
    extended_portals.to_csv(output_file, index = None)


def add_prefixes(extended_portals_file: str, output_file: str):
    """Iterating over a portal list, adding the protocol prefix (if it is working) and adding a portal activity status

    Args:
        ``extended_portals_file (str):`` the path of the CSV input file containing portal URLs in a column "url"
        
        ``output_file (str):`` the path of the CSV file to be exported
    """
    
    # Opening the file that contains the unique / deduplicated portal URLs (without protocol prefixes)
    extended_portals = pd.read_csv(extended_portals_file)

    # Adding a column that indicates whether or not the portal is active, i.e. responds to an HTTP request.
    extended_portals["active"] = pd.Series(dtype = "boolean")
    
    # Adding a column for error information
    extended_portals["error_type"] = None

    # Iterating over all portals in the list
    for index, portal in extended_portals.iterrows():
        # Print current portal and its position in the list
        print("Portal " + str(index + 1) + "/" + str(len(extended_portals)) + ": " + portal["url"])

        # Check protocol and update portal URL in the list
        result = check_protocol(portal["url"])
        extended_portals.loc[index, "url"] = result

        # HTTPS worked
        if (result.startswith("https://")):
            print("Added to list as active portal with HTTPS. \n")
            extended_portals.loc[index, "active"] = True
        # HTTP worked
        elif (result.startswith("http://")):
            print("Added to list as active portal with HTTP. \n")
            extended_portals.loc[index, "active"] = True
        # Neither worked
        else:
            print("Added to list as inactive portal. \n")
            extended_portals.loc[index, "active"] = False
            extended_portals.loc[index, "error_type"] = ["HTTPS and HTTP requests failed"]
        
    # Sorting the dataframe by URL and saving it to a CSV file
    prefixed_portals = extended_portals.sort_values(by=["url"])
    prefixed_portals.to_csv(output_file, index = None)


def validate_list(input_list: str, output_list: str, output_markers: str, input_markers: str = None, retry_failed_portals: bool = False):
    """Iterating over a portal list, validating that the portals use a relevant catalog software and exporting the validation results

    Code for checking validation markers and the related JSON export was partially taken from https://github.com/semantisch/crawley (© Daniil Dobriy)
    
    Args:
        ``input_list (str):`` the path of the CSV input file containing URLs - must be a file created previously by "add_prefixes()" or "validate_list()" - if "retry_failed_portals" is True, must be a file created previously by "validate_list()"
        
        ``output_list (str):`` the path of the CSV output file to be exported, containing validated URLs
        
        ``output_markers (str):`` the path of the JSON output file to be exported, containing portals and their detected validation markers
        
        ``input_markers (str, optional):`` the path of the JSON input file containing portals and their detected validation markers - must be a file created previously by "validate_list()"
        
        ``retry_failed_portals (bool, optional):`` whether or not to retry the portals for which the validation failed or the suspected API didn't work in a previous run - defaults to False
    """

    # Opening the file that contains the portal URLs
    prefixed_portals = pd.read_csv(input_list)

    # Only the active portals will be validated
    active_portals = prefixed_portals[prefixed_portals["active"] == True]

    # Retrying only those portals from a previous validation run for which the validation failed or the suspected API didn't work
    if retry_failed_portals == True:
        active_portals = active_portals[(active_portals["validated"] == False) | (active_portals["api_working"] == False)]

    # Adding columns that indicate if the portal could be validated and which catalog software it uses
    if retry_failed_portals == False:
        prefixed_portals["validated"] = False
        prefixed_portals["suspected_api"] = None
        prefixed_portals["api_working"] = None
        prefixed_portals["api_version"] = None

    # Defining an inner function to log errors
    def log(error):
        errors.append(str(type(error).__name__))
        prefixed_portals.loc[index, "error_type"] = str(errors)

    # Loading the validation markers
    config = json.load(open('../crawley-lite/config.json', 'r', encoding='utf-8'))

    # Creating or loading a dictionary for the portals with detected validation markers
    if retry_failed_portals == False or input_markers is None:
        validated_sites = {}
    else:
        with open(input_markers, "r", encoding = 'utf8') as file:
            validated_sites = json.load(file)

    # Showing information
    if retry_failed_portals == False:
        print("Skipping inactive portals...")
    else:
        print("Skipping all portals except those that previously could not be validated or had non-working APIs...")

    # Iterating over all active portals in the list
    for index, portal in active_portals.iterrows():
        # Print current portal and its position in the list of all portals
        print("Portal " + str(index + 1) + "/" + str(len(prefixed_portals)) + ": " + portal["url"])

        # Setting the portal URL as the base URL
        base_url = portal["url"]

        # Setting the variable that is used to stop the loop when the software is found
        software_found = False

        # (Re-)setting the errors variable
        errors = []

        # Removing the previous error when retrying a portal
        if retry_failed_portals == True:
            prefixed_portals.loc[index, "error_type"] = None

        # Requesting the site and retrieve its contents
        try:
            response = requests.get(base_url, timeout = 15)
            contents = response.text
        # If the request fails / times out, skipping to the next portal
        except Exception as e:
            log(e)
            continue
        
        # Searching for markers of data catalog platforms (CKAN, etc.) if the API software is not yet known
        if pd.isna(prefixed_portals.loc[index, "manually_checked_api"]):
            for platform_type in config:
                # Looping through validation markers for each platform
                for validation_marker in config[platform_type]["validate"]:
                    # Adding platform type to the dictionary if not there yet
                    if not platform_type in validated_sites:
                        validated_sites[platform_type] = {}
                    # Checking if the validation marker can be found in the current site's contents
                    if validation_marker.lower() in contents.lower():
                        # Adding the site to the validated sites for the platform
                        if not base_url in validated_sites[platform_type]:
                            validated_sites[platform_type][base_url] = []
                        # Adding the validation marker that was found
                        if validation_marker not in validated_sites[platform_type][base_url]:
                            validated_sites[platform_type][base_url].append(validation_marker)
                        # Setting variable to stop looping through the platforms
                        software_found = True
                # If a validation marker was found, saving the platform software
                if base_url in validated_sites[platform_type]:
                    prefixed_portals.loc[index, "suspected_api"] = platform_type
                    print(platform_type, "markers found")
                # If no validation marker was found, marking the software as unknown
                else:
                    prefixed_portals.loc[index, "suspected_api"] = "Unknown"

                # Setting the portal as validated
                prefixed_portals.loc[index, "validated"] = True
                
                # Stopping the loop through the platforms
                if software_found == True:
                    break
        # Not searching for markers if the API software is already known (was manually checked before)
        else:
            # Save the manually checked API as the suspected API software
            prefixed_portals.loc[index, "suspected_api"] = portal["manually_checked_api"]
            print(portal["manually_checked_api"], "portal was manually added, skipping search for markers")

            # Setting the portal as validated
            prefixed_portals.loc[index, "validated"] = True

        # Verifying that the detected API is available and working
        if prefixed_portals.loc[index, "suspected_api"] is not None and prefixed_portals.loc[index, "suspected_api"] != "Unknown":

            # Checking portals with CKAN markers
            if prefixed_portals.loc[index, "suspected_api"] == "CKAN":
                # Resetting version variable
                ckan_version = None
                
                try:
                    api_url = base_url + "/api/3/action/package_search"
                    response = requests.get(api_url, timeout = 15)
                    if json.loads(response.text)["success"] == True:
                        print("CKAN API working")
                        prefixed_portals.loc[index, "api_working"] = True
                        # Checking the API version
                        try:
                            api_version_url = base_url + "/api/3/action/status_show" 
                            response = requests.get(api_version_url, timeout = 15)
                            ckan_version = json.loads(response.text)["result"]["ckan_version"]
                            prefixed_portals.loc[index, "api_version"] = ckan_version
                        except Exception as e:
                            prefixed_portals.loc[index, "api_version"] = "Unknown"
                            log(e)
                except Exception as e:
                    print("CKAN API not working")
                    prefixed_portals.loc[index, "api_working"] = False
                    log(e)

            # Checking portals with Socrata markers
            elif prefixed_portals.loc[index, "suspected_api"] == "Socrata":
                try:
                    api_url = base_url + "/api/views/metadata/v1?method=help"
                    response = requests.get(api_url, timeout = 15)
                    if "id" in json.loads(response.text)["immutableFields"]:
                        print("Socrata API working")
                        prefixed_portals.loc[index, "api_working"] = True
                        # The "Socrata Metadata API" (not "SODA API"!) seems to only have one version
                        prefixed_portals.loc[index, "api_version"] = "v1.0"
                except Exception as e:
                    print("Socrata API not working")
                    prefixed_portals.loc[index, "api_working"] = False
                    log(e)

            # Checking portals with Opendatasoft markers
            elif prefixed_portals.loc[index, "suspected_api"] == "OpenDataSoft":
                # Reset versions variable
                opendatasoft_versions = None

                # Check API v2.x
                try:
                    api_url = base_url + "/api/explore/"
                    response = requests.get(api_url, timeout = 15)
                    opendatasoft_versions = json.loads(response.text)["versions"]
                    print("Opendatasoft API v2.x working")
                except Exception as e:
                    print("Opendatasoft API v2.x not working")
                    log(e)

                # Check API v1.0
                try:
                    api_url_old = base_url + "/api/datasets/1.0/search/?rows=1"
                    response = requests.get(api_url_old, timeout = 15)
                    # Trying to access a JSON key of a valid API response  
                    json.loads(response.text)["nhits"]
                    try:
                        opendatasoft_versions.insert(0, "v1.0")
                    except NameError:
                        opendatasoft_versions = "v1.0"
                    print("Opendatasoft API v1.0 working")
                except Exception as e:
                    print("Opendatasoft API v1.0 not working")
                    log(e)

                # Saving the working API versions
                if opendatasoft_versions is not None:
                    prefixed_portals.loc[index, "api_version"] = str(opendatasoft_versions)
                    prefixed_portals.loc[index, "api_working"] = True
                else:
                    prefixed_portals.loc[index, "api_working"] = False

    # Showing information
    if retry_failed_portals == False:
        print("Skipping inactive portals...")
    else:
        print("Skipping all portals except those that previously could not be validated or had non-working APIs...")

    # Exporting the validation results to a JSON file
    with open(output_markers, "w", encoding = 'utf8') as outfile:
        json.dump(validated_sites, outfile, ensure_ascii = False, indent = 4)

    # Printing the number of validated sites per platform type
    for platform_type in config:
        if platform_type in validated_sites:
            detected_markers = len(validated_sites[platform_type])
        else:
            detected_markers = 0
        print(f'\nPortals with detected {platform_type} markers: {detected_markers}')
        print(f'Portals with a manually added {platform_type} API endpoint: {len(prefixed_portals[prefixed_portals["manually_checked_api"] == platform_type])}')
        print(f'Portals with a working {platform_type} API: {len(prefixed_portals[(prefixed_portals["suspected_api"] == platform_type) & (prefixed_portals["api_working"] == True)])}')

    # Ordering the dataframe, sorting it by URL and saving it to a CSV file
    prefixed_portals = prefixed_portals[["url", "active", "validated", "manually_checked_api", "suspected_api", "api_working", "api_version", "error_type"]]
    prefixed_portals = prefixed_portals.sort_values(by=["url"])
    prefixed_portals.to_csv(output_list, index = None)


def analyze_list(validated_portals_file: str, show: str = True, export: bool = False):
    """Analyzing, presenting and saving the most important information about a validated portal list

    Args:
        ``validated_portals_file (str):`` the path of the CSV input file containing validated portal URLs - must be a file created previously by "validate_list()"

        ``show (bool, optional):`` whether or not to display the relevant dataframes and results - defaults to True

        ``export (bool, optional):`` whether or not to append the results to the statistics CSV file - defaults to False
    """    

    # Importing the list of validated portals 
    file = validated_portals_file
    validated_portals = pd.read_csv(file)

    # Creating a dataframe for the statistics
    statistics = pd.DataFrame(columns = ["file", "total", "active", "inactive", "validated", "unvalidated", "subpage_endpoints", "no_markers", "ckan_suspected", "ckan_working", "opendatasoft_suspected", "opendatasoft_working", "socrata_suspected", "socrata_working", "timestamp"])

    # File path, total portals, file modification timestamp
    statistics.loc[0, "file"] = file
    statistics.loc[0, "total"] = len(validated_portals)
    file_modification_unix_time = os.path.getmtime(file)
    timestamp = str(datetime.datetime.fromtimestamp(file_modification_unix_time))
    statistics.loc[0, "timestamp"] = timestamp

    # Showing the unique values of each column
    if show is True:
        for column in validated_portals:
            print(str(column) + ": " + str(validated_portals[column].unique()))

    # Portals that are active
    active_portals = validated_portals[validated_portals["active"] == True]
    active_portals.columns.name = 'Active portals'
    statistics.loc[0, "active"] = len(active_portals)
    if show is True:
        display(active_portals)

    # Portals that are inactive (i.e. did not respond)
    inactive_portals = validated_portals[validated_portals["active"] == False].copy()
    inactive_portals.columns.name = 'Inactive portals'
    statistics.loc[0, "inactive"] = len(inactive_portals)
    if show is True:
        display(inactive_portals)

    # Portals that could be validated
    successfully_validated_portals = validated_portals[validated_portals["validated"] == True]
    successfully_validated_portals.columns.name = "Validated portals"
    statistics.loc[0, "validated"] = len(successfully_validated_portals)
    if show is True:
        display(successfully_validated_portals)

    # Portals that could not be validated (e.g. due to a timeout)
    unvalidated_portals = validated_portals[validated_portals["validated"] == False]
    unvalidated_portals.columns.name = "Failed / unvalidated portals"
    statistics.loc[0, "unvalidated"] = len(unvalidated_portals)
    if show is True:
        display(unvalidated_portals)

    # Portals with non-standard subpage API endpoints that were manually checked and added
    subpage_endpoint_portals = validated_portals[validated_portals["manually_checked_api"].notna()]
    subpage_endpoint_portals.columns.name = "Portals with non-standard subpage API endpoints"
    statistics.loc[0, "subpage_endpoints"] = len(subpage_endpoint_portals)
    if show is True:
        display(subpage_endpoint_portals)

    # Portals for which no validation markers were found
    no_markers_portals = validated_portals[validated_portals["suspected_api"] == "Unknown"]
    no_markers_portals.columns.name = "Portals without markers"
    statistics.loc[0, "no_markers"] = len(no_markers_portals)
    if show is True:
        display(no_markers_portals)

    # Suspected CKAN portals (validation markers found or API manually checked) 
    ckan_markers_portals = validated_portals[validated_portals["suspected_api"] == "CKAN"]
    ckan_markers_portals.columns.name = "Suspected CKAN portals"
    statistics.loc[0, "ckan_suspected"] = len(ckan_markers_portals)
    if show is True:
        display(ckan_markers_portals)

    # Portals with working CKAN API
    ckan_working_api_portals = ckan_markers_portals[ckan_markers_portals["api_working"] == True]
    ckan_working_api_portals.columns.name = "Portals with working CKAN API"
    statistics.loc[0, "ckan_working"] = len(ckan_working_api_portals)
    if show is True:
        display(ckan_working_api_portals)

    # Suspected Opendatasoft portals (validation markers found or API manually checked) 
    opendatasoft_markers_portals = validated_portals[validated_portals["suspected_api"] == "OpenDataSoft"]
    opendatasoft_markers_portals.columns.name = "Suspected Opendatasoft portals"
    statistics.loc[0, "opendatasoft_suspected"] = len(opendatasoft_markers_portals)
    if show is True:
        display(opendatasoft_markers_portals)

    # Portals with working Opendatasoft API
    opendatasoft_working_api_portals = opendatasoft_markers_portals[opendatasoft_markers_portals["api_working"] == True]
    opendatasoft_working_api_portals.columns.name = "Portals with working Opendatasoft API"
    statistics.loc[0, "opendatasoft_working"] = len(opendatasoft_working_api_portals)
    if show is True:
        display(opendatasoft_working_api_portals)

    # Suspected Socrata portals (validation markers found or API manually checked) 
    socrata_markers_portals = validated_portals[validated_portals["suspected_api"] == "Socrata"]
    socrata_markers_portals.columns.name = "Suspected Socrata portals"
    statistics.loc[0, "socrata_suspected"] = len(socrata_markers_portals)
    if show is True:
        display(socrata_markers_portals)

    # Portals with working Socrata API
    socrata_working_api_portals = socrata_markers_portals[socrata_markers_portals["api_working"] == True]
    socrata_working_api_portals.columns.name = "Portals with working Socrata API"
    statistics.loc[0, "socrata_working"] = len(socrata_working_api_portals)
    if show is True:
        display(socrata_working_api_portals)

    # Showing statistics
    if show is True:
        display(statistics)

    # If the CSV file exists, checking if the portal list statistics are included in it already (same file path and modification timestamp)
    if export is True:
        try:
            existing_csv = pd.read_csv("data/validation_statistics.csv")
            if not (file in str(existing_csv["file"]) and timestamp in str(existing_csv["timestamp"])):
                raise Exception
            else:
                print("Statistics for this list are already in data/validation_statistics.csv")
        # If the CSV file doesn't exist or the portal list statistics are not included yet, create a new CSV or append to the existing one
        except:
            statistics.to_csv("data/validation_statistics.csv", mode = "a", index = False, header = not os.path.isfile("data/validation_statistics.csv"))
            print("Statistics were saved.")


def extract_working_apis(validated_portals_file: str, output_file: str):
    """Extracting the essential data from the validated portal list, keeping only the portals with working APIs, performing a final deduplication and exporting the final list

    Args:
        ``validated_portals_file (str):`` the path of the CSV input file containing validated portal URLs - must be a file created previously by "validate_list()"
        
        ``output_file (str):`` the path of the CSV file to be exported, containing the final list of portal APIs
    """
    
    # Opening the file that contains the validated portal URLs
    validated_portals = pd.read_csv(validated_portals_file)

    # Keeping only the portals with a working API
    working_portals = validated_portals[validated_portals["api_working"] == True].copy()

    # Removing "www." from the netloc and saving in a new column to find duplicates that appear with and without "www."
    for index, portal in working_portals.iterrows():
        working_portals.loc[index, "netloc_without_www"] = urlparse(portal["url"]).netloc.removeprefix("www.")

    # Finding all duplicate URLs and keeping both the first occurences and the duplicates
    # display(working_portals[working_portals.duplicated("netloc_without_www", False)].sort_values(["netloc_without_www", "url"]))

    # Counting the number of duplicates (without the first occurences)
    # print("Duplicate sites after removing \"www.\":", len(working_portals[working_portals.duplicated("netloc_without_www")]))

    # Removing the duplicates
    final_portals = working_portals.drop_duplicates(subset = "netloc_without_www", keep = "first", ignore_index = True) 

    # Showing the final portal list
    # display(final_portals)

    # Editing the columns, sorting the dataframe by URL and saving it to a CSV file
    final_portals = final_portals.rename(columns = {"suspected_api": "api_software"})
    final_portals = final_portals[["url", "api_working", "api_software", "api_version"]].sort_values(by=["url"])
    final_portals.to_csv(output_file, index = None)

