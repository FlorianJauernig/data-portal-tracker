# Loading required packages
import os
import json
import requests
import pandas as pd
from re import search
from time import sleep
from statistics import mean
from datetime import datetime
from dotenv import dotenv_values
from helpers import check_protocol, remove_double_slashes
from archiver_connector import ArchiverConnector

# Loading environment variables
config = dotenv_values("../.env")

# Calling the Archiver connector
archiver = ArchiverConnector(mode = "local")
# archiver = ArchiverConnector(mode = "production")

# Getting the project path
project_path = config["PATH"]

# Loading the portal list
portal_list = pd.read_csv(project_path + "data_portal_tracker/data/portals.csv")

# TESTING ONLY: loading a subset of the portal list that covers a wide range of API versions
portal_list_test = pd.read_csv(project_path + "data_portal_tracker/data/portals_test_subset.csv")


def crawl_opendatasoft_v1(portal_list: str, statistics_file: str):
    """Crawling all portals on the list that support the Opendatasoft API v1.0, inserting all datasets and metadata of each portal into the Archiver and saving statistics.

    Args:
        ``portal_list (str):`` the path of the CSV input file containing the final portal list - must be a file created previously by "extract_working_apis()" in the portal handler
        
        ``statistics_file (str):`` the path of the CSV file to be created or extended, containing the statistics for the crawled portals
    """

    # Getting the current timestamp
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")

    # Creating a dataframe to log failed API requests
    failed_api_requests = pd.DataFrame([(None, None, None, None, None, None)], columns = ["timestamp", "api_request_url", "dataset_url", "metadata_url", "source_url", "exception"])
    failed_api_requests_filename = project_path + "data_portal_tracker/logs/crawl_opendatasoft_v1_" + current_timestamp + "_fail.csv"

    # Setting file paths for the log files to be created by handle_dataset()
    log_file_success = project_path +  "data_portal_tracker/logs/handle_dataset_opendatasoft_v1_" + current_timestamp + "_success.csv"
    log_file_fail = project_path + "data_portal_tracker/logs/handle_dataset_opendatasoft_v1_" + current_timestamp + "_fail.csv"

    # Creating lists for the API base URLs and API method URLs
    api_base_urls = []
    api_method_urls = []

    # Creating a list of portals that use Opendatasoft v1.0 and have a working API
    for i in range(len(portal_list)):
        if ((portal_list["api_software"][i] == "OpenDataSoft") & ("v1.0" in portal_list["api_version"][i]) & (portal_list["api_working"][i] == True)):
            api_base_url = portal_list["url"][i]
            api_base_urls.append(api_base_url)
            api_method_urls.append(api_base_url + "/api/datasets/1.0/search/?")

    # Printing information
    print("Crawling portals supporting Opendatasoft v1.0")

    # Looping through the Opendatasoft v1.0 portals
    for i in range(len(api_method_urls)):

        # Creating a dataframe for the portal statistics
        portal_statistics = pd.DataFrame([(None, None, None, None, None)], columns = ["url", "api_software", "number_of_datasets", "number_of_supported_datasets", "timestamp"])

        # Setting the variable indicating if there are still unseen datasets on the portal
        datasets_available = True

        # Setting the number of datasets to be returned for each request
        datasets_per_request = 800

        # Setting the index of the first dataset to be returned
        index_of_current_dataset = 0

        # Resetting the number of datasets counted in the response to the current request
        datasets_in_current_response = 0

        # Resetting dataset, metadata and source variables for error logging purposes
        dataset_url = None
        metadata_url = None
        source_url = None
        metadata = None

        # Setting the maximum number of attempts in case of an exception before skipping the portal
        maximum_attempts = 3

        # Setting the number of the current attempt
        attempt_number = 1

        # Printing the portal
        print("\n" + "Portal " + str(i+1) + "/" + str(len(api_method_urls)) + ": " + api_base_urls[i])

        # Iterating over all datasets on the portal in batches of 800 until we run out of datasets
        while datasets_available:
            # Building the API request URL
            api_request_url = remove_double_slashes(api_method_urls[i] + "rows=" + str(datasets_per_request) + "&start=" + str(index_of_current_dataset))

            # Waiting before each request in order not to flood the API with requests
            sleep(2)
            
            try:
                # Making the API request and deserializing the JSON response string
                response = json.loads(requests.get(api_request_url).text)

                # Getting the total number of datasets on the portal
                total_number_of_datasets = response["nhits"]

                # During the first iteration / request (so only once)
                if(index_of_current_dataset == 0):
                    # Printing the total number of datasets
                    print("Total number of datasets: " + str(total_number_of_datasets))

                    # Saving information to the statistics dataframe
                    portal_statistics.loc[0, "url"] = api_base_urls[i]
                    portal_statistics.loc[0, "api_software"] = "Opendatasoft"
                    portal_statistics.loc[0, "number_of_datasets"] = int(total_number_of_datasets)
                    portal_statistics.loc[0, "timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    # Export statistics to a CSV file
                    portal_statistics.to_csv(statistics_file, mode = "a", index = False, header = not os.path.isfile(statistics_file))

                # Printing the current API request URL
                print("\n" + "Currently crawling: " + api_request_url + "\n")

                # Extracting the metadata from the current response
                metadata = response["datasets"]

                # Counting the number of datasets in the metadata
                datasets_in_current_response = len(metadata)
                
                # Iterating over all the metadata of the current response
                for j in range(datasets_in_current_response):
                    # Getting the ID of each dataset
                    dataset_id = str(metadata[j]["datasetid"])

                    # Building the metadata URL and dataset URL
                    metadata_url = remove_double_slashes(api_base_urls[i] + "/api/datasets/1.0/") + dataset_id
                    dataset_url = remove_double_slashes(api_base_urls[i] + "/api/records/1.0/download?dataset=") + dataset_id + "&format=csv"

                    """
                    # Optional: If the portal is the Opendatasoft data hub, building the URL of the original data source (= different site than the data hub) using the metadata.
                    if (search("https://data.opendatasoft.com", api_base_urls[i])):
                        # Starting from the second iteration, comparing the domain of the current and previous original source
                        if j != 0:
                            previous_domain = remove_double_slashes(metadata[j-1]["metas"]["source_domain_address"])
                        else:
                            previous_domain = None
                        current_domain = remove_double_slashes(metadata[j]["metas"]["source_domain_address"])

                        # Wait 2 seconds between two requests to the same portal
                        if current_domain == previous_domain:
                            sleep(2)
                        
                        # Checking the protocol, adding the protocol prefix and building the original source URL
                        original_source_url = check_protocol(remove_double_slashes(metadata[j]["metas"]["source_domain_address"] + "/explore/dataset/") + metadata[j]["metas"]["source_dataset"], show_details = False)
                    """
                    
                    # Building the source URL using the API base url
                    source_url = remove_double_slashes(api_base_urls[i] + "/explore/dataset/") + dataset_id

                    # Calling the Archiver connector to insert data into the Archiver
                    archiver.handle_dataset(dataset_url, metadata_url, source_url, log_file_success, log_file_fail)

                    # Printing dataset information
                    print("Dataset " + str(j + 1 + index_of_current_dataset) + "/" + str(total_number_of_datasets))
                    # print("Dataset URL: " + dataset_url)
                    # print("Metadata URL: " + metadata_url)
                    # print("Source URL: " + source_url + "\n")
                
                # Setting the index of the next dataset to be returned
                index_of_current_dataset += datasets_in_current_response 

                # Stopping the loop after this iteration if less than 800 datasets are returned (meaning that these are the last available datasets)
                if datasets_in_current_response != datasets_per_request:
                    datasets_available = False

                # Stopping the loop after this iteration if the index of the next requested dataset would exceed the index of the last available dataset
                if index_of_current_dataset >= total_number_of_datasets:
                    datasets_available = False
            except Exception as exception:
                # Saving the failed API requests to the dataframe, then saving them to a new CSV file or appending them to an existing one
                failed_api_requests.loc[0, "timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                failed_api_requests.loc[0, "api_request_url"] = api_request_url
                failed_api_requests.loc[0, "exception"] = repr(exception)
                try:
                    failed_api_requests.loc[0, "dataset_url"] = dataset_url
                    failed_api_requests.loc[0, "metadata_url"] = metadata_url
                    failed_api_requests.loc[0, "source_url"] = source_url
                except NameError:
                    pass
                failed_api_requests.to_csv(failed_api_requests_filename, mode = "a", index = False, header = not os.path.isfile(failed_api_requests_filename))
                print("An exception occurred!")

                # If the maximum number of attempts has been reached, skipping the portal 
                if attempt_number == maximum_attempts:
                    datasets_available = False
                # Otherwise, increasing the number of attempts by 1
                else:
                    attempt_number += 1
                    sleep(2)


def crawl_opendatasoft_v2(portal_list: str, statistics_file: str):
    """Crawling all portals on the list that support the Opendatasoft API v2.1, inserting all datasets and metadata of each portal into the Archiver and saving statistics.

    Args:
        ``portal_list (str):`` the path of the CSV input file containing the final portal list - must be a file created previously by "extract_working_apis()" in the portal handler
        
        ``statistics_file (str):`` the path of the CSV file to be created or extended, containing the statistics for the crawled portals
    """
        
    # Getting the current timestamp
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")

    # Creating a dataframe to log failed API requests
    failed_api_requests = pd.DataFrame([(None, None, None, None, None, None)], columns = ["timestamp", "api_request_url", "dataset_url", "metadata_url", "source_url", "exception"])
    failed_api_requests_filename = project_path + "data_portal_tracker/logs/crawl_opendatasoft_v2_" + current_timestamp + "_fail.csv"

    # Setting file paths for the log files to be created by handle_dataset()
    log_file_success = project_path +  "data_portal_tracker/logs/handle_dataset_opendatasoft_v2_" + current_timestamp + "_success.csv"
    log_file_fail = project_path + "data_portal_tracker/logs/handle_dataset_opendatasoft_v2_" + current_timestamp + "_fail.csv"

    # Creating lists for the API base URLs and API method URLs
    api_base_urls = []
    api_method_urls = []

    # Creating a list of portals that use Opendatasoft v2.1 and have a working API
    for i in range(len(portal_list)):
        if ((portal_list["api_software"][i] == "OpenDataSoft") & ("v2.1" in portal_list["api_version"][i]) & (portal_list["api_working"][i] == True)):
            api_base_url = portal_list["url"][i]
            api_base_urls.append(api_base_url)
            api_method_urls.append(api_base_url + "/api/explore/v2.1")

    # Printing information
    print("Crawling portals supporting Opendatasoft v2.1")

    # Looping through the Opendatasoft v2.1 portals
    for i in range(len(api_method_urls)):

        # Creating a dataframe for the portal statistics
        portal_statistics = pd.DataFrame([(None, None, None, None, None)], columns = ["url", "api_software", "number_of_datasets", "number_of_supported_datasets", "timestamp"])

        # Resetting dataset, metadata and source variables for error logging purposes
        dataset_url = None
        metadata_url = None
        source_url = None
        metadata = None

        # Setting the variable counting the number of supported datasets on the portal
        number_of_supported_datasets = 0
        
        # Setting the maximum number of attempts in case of an exception before skipping the portal
        maximum_attempts = 3

        # Setting the number of the current attempt
        attempt_number = 1

        # Setting the error variable
        error = False

        # Setting the variable that indicates whether the portal's catalog has been downloaded
        catalog_exported = False

        # Printing the portal
        print("\n" + "Portal " + str(i+1) + "/" + str(len(api_method_urls)) + ": " + api_base_urls[i])

        # Looping as long as the catalog has not been downloaded
        while not catalog_exported:
            # Building the API request URL
            api_request_url = remove_double_slashes(api_method_urls[i] + "/catalog/exports/json")

            try:
                # Making the API request and deserializing the JSON response string
                metadata = json.loads(requests.get(api_request_url).text)

                # Indicating successful export
                catalog_exported = True

                # Getting the total number of datasets on the portal
                total_number_of_datasets = len(metadata)

                # Comment this step out if the optional code below is used!
                number_of_supported_datasets = total_number_of_datasets

                # Printing the catalog API export URL
                print("\n" + "Currently crawling: " + api_request_url + "\n")
                
                # Iterating over all the metadata of the response
                for j in range(len(metadata)):
                    # Getting the ID of each dataset
                    dataset_id = str(metadata[j]["dataset_id"])

                    """
                    # Optional: Checking the available export formats of the dataset

                    # Waiting before each request in order not to flood the API with requests
                    # sleep(1)

                    dataset_formats_url = remove_double_slashes(api_base_urls[i] + "/api/explore/v2.1/catalog/datasets/") + dataset_id + "/exports"
                    dataset_formats = json.loads(requests.get(dataset_formats_url).text)

                    # Choosing CSV if available, else JSON (this list could be extended, check the URL above for options!)
                    for link in dataset_formats["links"]:
                        if "csv" in link.values():
                            dataset_format = "csv"
                            break
                        elif "json" in link.values():
                            dataset_format = "json"
                            break

                    # Increasing the number of supported datasets if the dataset is available in one of the specified formats, else skipping the dataset
                    if dataset_format in ["csv", "json"]:
                        number_of_supported_datasets += 1
                    else:
                        break
                        
                    # If the optional code is used:
                        # Swap the used dataset_url line below (comment / uncomment)
                        # Comment out the "number_of_supported_datasets = total_number_of_datasets" step above
                    """

                    # Building the metadata URL, dataset URL and source URL
                    metadata_url = remove_double_slashes(api_base_urls[i] + "/api/explore/v2.1/catalog/datasets/") + dataset_id
                    dataset_url = remove_double_slashes(api_base_urls[i] + "/api/explore/v2.1/catalog/datasets/") + dataset_id + "/exports/" + "csv"
                    # dataset_url = remove_double_slashes(api_base_urls[i] + "/api/explore/v2.1/catalog/datasets/") + dataset_id + "/exports/" + dataset_format
                    source_url = remove_double_slashes(api_base_urls[i] + "/explore/dataset/") + dataset_id

                    # Calling the Archiver connector to insert data into the Archiver
                    archiver.handle_dataset(dataset_url, metadata_url, source_url, log_file_success, log_file_fail)

                    # Printing dataset information
                    print("Dataset " + str(j + 1) + "/" + str(total_number_of_datasets))
                    # print("Dataset URL: " + dataset_url)
                    # print("Metadata URL: " + metadata_url)
                    # print("Source URL: " + source_url + "\n")
                
            except Exception as exception:
                # Saving the failed API requests to the dataframe, then saving them to a new CSV file or appending them to an existing one
                failed_api_requests.loc[0, "timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                failed_api_requests.loc[0, "api_request_url"] = api_request_url
                failed_api_requests.loc[0, "exception"] = repr(exception)
                try:
                    failed_api_requests.loc[0, "dataset_url"] = dataset_url
                    failed_api_requests.loc[0, "metadata_url"] = metadata_url
                    failed_api_requests.loc[0, "source_url"] = source_url
                except NameError:
                    pass
                failed_api_requests.to_csv(failed_api_requests_filename, mode = "a", index = False, header = not os.path.isfile(failed_api_requests_filename))
                print("An exception occurred!")

                # If the maximum number of attempts has been reached, skipping the portal 
                if attempt_number == maximum_attempts:
                    catalog_exported = True
                    error = True
                # Otherwise, increasing the number of attempts by 1
                else:
                    attempt_number += 1
                    sleep(2)

        # Printing the number of (total/supported) datasets
        print("\n" + "Total number of datasets on " + api_base_urls[i] + " : " + str(total_number_of_datasets))
        print("Number of supported datasets on " + api_base_urls[i] + " : " + str(number_of_supported_datasets) + "\n")

        # Saving information to the statistics dataframe
        portal_statistics.loc[0, "url"] = api_base_urls[i]
        portal_statistics.loc[0, "api_software"] = "Opendatasoft"
        if error is False:
            portal_statistics.loc[0, "number_of_datasets"] = int(total_number_of_datasets)
            portal_statistics.loc[0, "number_of_supported_datasets"] = int(number_of_supported_datasets)
        portal_statistics.loc[0, "timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Export statistics to a CSV file
        portal_statistics.to_csv(statistics_file, mode = "a", index = False, header = not os.path.isfile(statistics_file))

                
def crawl_ckan(portal_list: str, statistics_file: str):
    """Crawling all portals on the list that support the CKAN API v2.x, inserting all datasets (CKAN term: resources) and metadata of each portal into the Archiver and saving statistics.

    Args:
        ``portal_list (str):`` the path of the CSV input file containing the final portal list - must be a file created previously by "extract_working_apis()" in the portal handler
        
        ``statistics_file (str):`` the path of the CSV file to be created or extended, containing the statistics for the crawled portals
    """

    # Getting the current timestamp
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")

    # Creating a dataframe to log failed API requests
    failed_api_requests = pd.DataFrame([(None, None, None, None, None, None)], columns = ["timestamp", "api_request_url", "resource_url", "metadata_url", "source_url", "exception"])
    failed_api_requests_filename = project_path + "data_portal_tracker/logs/crawl_ckan_" + current_timestamp + "_fail.csv"

    # Setting file paths for the log files to be created by handle_dataset()
    log_file_success = project_path +  "data_portal_tracker/logs/handle_dataset_ckan_" + current_timestamp + "_success.csv"
    log_file_fail = project_path + "data_portal_tracker/logs/handle_dataset_ckan_" + current_timestamp + "_fail.csv"

    # Creating lists for the API base URLs and API method URLs
    api_base_urls = []
    api_method_urls = []

    # Creating a list of portals that use CKAN and have a working API
    for i in range(len(portal_list)):
        if ((portal_list["api_software"][i] == "CKAN") & (portal_list["api_working"][i] == True)):
            api_base_url = portal_list["url"][i]
            api_base_urls.append(api_base_url)
            api_method_urls.append(api_base_url + "/api/3/action/package_search?")

    # Printing information
    print("Crawling portals supporting CKAN")
    print("On CKAN portals, one dataset/package can contain multiple resources.")
    print('The term "resource" on CKAN portals is mostly equivalent to the term "dataset" used in this project.')

    # Looping through the CKAN portals
    for i in range(len(api_method_urls)):

        # Creating a dataframe for the portal statistics
        portal_statistics = pd.DataFrame([(None, None, None, None, None)], columns = ["url", "api_software", "number_of_datasets", "number_of_resources", "timestamp"])

        # Setting the variable indicating if there are still unseen datasets on the portal
        datasets_available = True

        # Setting the number of datasets to be returned for each request
        datasets_per_request = 800

        # Setting the index of the first dataset to be returned
        index_of_current_dataset = 0

        # Resetting the number of datasets counted in the response to the current request
        datasets_in_current_response = 0

        # Setting the variables counting the total number of datasets and resources on the portal
        total_number_of_datasets = 0
        total_number_of_resources = 0

        # Resetting resource, metadata and source variables for error logging purposes
        resource_url = None
        metadata_url = None
        source_url = None
        metadata = None

        # Setting the maximum number of attempts in case of an exception before skipping the portal
        maximum_attempts = 3

        # Setting the number of the current attempt
        attempt_number = 1

        # Setting the error variable
        error = False

        # Printing the portal
        print("\n" + "Portal " + str(i+1) + "/" + str(len(api_method_urls)) + ": " + api_base_urls[i])

        # Iterating over all datasets on the portal in batches of 800 until we run out of datasets
        while datasets_available:
            # Building the API request URL
            api_request_url = remove_double_slashes(api_method_urls[i] + "rows=" + str(datasets_per_request) + "&start=" + str(index_of_current_dataset))

            # Waiting before each request in order not to flood the API with requests
            sleep(2)
            
            try:
                # Making the API request and deserializing the JSON response string
                response = json.loads(requests.get(api_request_url).text)

                # Getting the total number of datasets on the portal
                total_number_of_datasets = response["result"]["count"]

                # During the first iteration / request (so only once)
                if(index_of_current_dataset == 0):
                    # Printing the total number of datasets
                    print("Total number of datasets: " + str(total_number_of_datasets))

                # Printing the current API request URL
                print("\n" + "Currently crawling: " + api_request_url)

                # Extracting the metadata from the current response
                metadata = response["result"]["results"]

                # Counting the number of datasets in the metadata
                datasets_in_current_response = len(metadata)
                
                # Iterating over all the metadata of the current response
                for j in range(datasets_in_current_response):

                    # Printing the current dataset number
                    print("\n" + "Dataset " + str(j + 1 + index_of_current_dataset) + "/" + str(total_number_of_datasets))

                    # Getting and printing the number of resources
                    number_of_resources = metadata[j]["num_resources"]
                    print("Number of resources: " + str(number_of_resources) + "\n")

                    # Adding the number to the total resource number
                    total_number_of_resources += number_of_resources

                    # Iterating over all of the resources of a dataset, if any
                    if number_of_resources != 0:
                        for k, resource in enumerate(metadata[j]["resources"]):
                            # Getting the dataset ID
                            dataset_id = metadata[j]["id"]

                            # Getting the resource URL
                            resource_url = resource["url"]

                            # Building the metadata URL (we are using the metadata of the dataset/package!)
                            metadata_url = remove_double_slashes(api_base_urls[i] + "/api/3/action/package_show?id=") + dataset_id
 
                            # Building the source URL (we are using the source of the dataset/package!)
                            source_url = remove_double_slashes(api_base_urls[i] + "/dataset/") + dataset_id

                            # Calling the Archiver connector to insert data into the Archiver
                            archiver.handle_dataset(resource_url, metadata_url, source_url, log_file_success, log_file_fail)

                            # Printing resource information
                            print("Resource " + str(k + 1) + "/" + str(number_of_resources))
                            # print("Resource URL: " + resource_url)
                            # print("Metadata URL: " + metadata_url)
                            # print("Source URL: " + source_url + "\n")
                    
                # Setting the index of the next dataset to be returned
                index_of_current_dataset += datasets_in_current_response 

                # Stopping the loop after this iteration if less than 800 datasets are returned (meaning that these are the last available datasets)
                if datasets_in_current_response != datasets_per_request:
                    datasets_available = False

                # Stopping the loop after this iteration if the index of the next requested dataset would exceed the index of the last available dataset
                if index_of_current_dataset >= total_number_of_datasets:
                    datasets_available = False
            except Exception as exception:
                # Saving the failed API requests to the dataframe, then saving them to a new CSV file or appending them to an existing one
                failed_api_requests.loc[0, "timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                failed_api_requests.loc[0, "api_request_url"] = api_request_url
                failed_api_requests.loc[0, "exception"] = repr(exception)
                try:
                    failed_api_requests.loc[0, "resource_url"] = resource_url
                    failed_api_requests.loc[0, "metadata_url"] = metadata_url
                    failed_api_requests.loc[0, "source_url"] = source_url
                except NameError:
                    pass
                failed_api_requests.to_csv(failed_api_requests_filename, mode = "a", index = False, header = not os.path.isfile(failed_api_requests_filename))
                print("An exception occurred!")

                # If the maximum number of attempts has been reached, skipping the portal 
                if attempt_number == maximum_attempts:
                    datasets_available = False
                    error = True
                # Otherwise, increasing the number of attempts by 1
                else:
                    attempt_number += 1
                    sleep(2)
        
        # Printing the total number of resources
        print("\n" + "Total number of resources on " + api_base_urls[i] + " : " + str(total_number_of_resources) + "\n")

        # Saving information to the statistics dataframe
        portal_statistics.loc[0, "url"] = api_base_urls[i]
        portal_statistics.loc[0, "api_software"] = "CKAN"
        if error is False:
            portal_statistics.loc[0, "number_of_datasets"] = int(total_number_of_datasets)
            portal_statistics.loc[0, "number_of_resources"] = int(total_number_of_resources)
        portal_statistics.loc[0, "timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Export statistics to a CSV file
        portal_statistics.to_csv(statistics_file, mode = "a", index = False, header = not os.path.isfile(statistics_file))


def crawl_socrata(portal_list: str, statistics_file: str):
    """Crawling all portals on the list that support the Socrata API, inserting all datasets and metadata of each portal into the Archiver and saving statistics.

    Args:
        ``portal_list (str):`` the path of the CSV input file containing the final portal list - must be a file created previously by "extract_working_apis()" in the portal handler
        
        ``statistics_file (str):`` the path of the CSV file to be created or extended, containing the statistics for the crawled portals
    """

    # Getting the current timestamp
    current_timestamp = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")

    # Creating a dataframe to log failed API requests
    failed_api_requests = pd.DataFrame([(None, None, None, None, None, None)], columns = ["timestamp", "api_request_url", "dataset_url", "metadata_url", "source_url", "exception"])
    failed_api_requests_filename = project_path + "data_portal_tracker/logs/crawl_socrata_" + current_timestamp + "_fail.csv"

    # Setting file paths for the log files to be created by handle_dataset()
    log_file_success = project_path +  "data_portal_tracker/logs/handle_dataset_socrata_" + current_timestamp + "_success.csv"
    log_file_fail = project_path + "data_portal_tracker/logs/handle_dataset_socrata_" + current_timestamp + "_fail.csv"

    # Creating lists for the API base URLs and API method URLs
    api_base_urls = []
    api_method_urls = []

    # Creating a list of portals that use Socrata and have a working API
    for i in range(len(portal_list)):
        if ((portal_list["api_software"][i] == "Socrata") & (portal_list["api_working"][i] == True)):
            api_base_url = portal_list["url"][i]
            api_base_urls.append(api_base_url)
            api_method_urls.append(api_base_url + "/api/views")

    # Printing information
    print("Crawling portals supporting Socrata")

    # Looping through the Socrata portals
    for i in range(len(api_method_urls)):

        # Creating a dataframe for the portal statistics
        portal_statistics = pd.DataFrame([(None, None, None, None, None)], columns = ["url", "api_software", "number_of_datasets", "number_of_supported_datasets", "timestamp"])

        # Setting the variable indicating if there are still unseen datasets on the portal
        datasets_available = True

        # Setting the number of datasets to be returned for each request
        datasets_per_request = 800

        # Setting the number of the first page to be requested (Socrata pagination starts with 1)
        current_page = 1

        # Setting the variables counting the number of (total/supported) datasets on the portal
        total_number_of_datasets = 0
        number_of_supported_datasets = 0

        # Resetting the number of datasets counted in the response to the current request
        datasets_in_current_response = 0

        # Resetting dataset, metadata and source variables for error logging purposes
        dataset_url = None
        metadata_url = None
        source_url = None
        metadata = None

        # Setting the maximum number of attempts in case of an exception before skipping the portal
        maximum_attempts = 3

        # Setting the number of the current attempt
        attempt_number = 1

        # Setting the error variable
        error = False

        # Printing the portal
        print("\n" + "Portal " + str(i+1) + "/" + str(len(api_method_urls)) + ": " + api_base_urls[i])

        # Iterating over all datasets on the portal in batches of 800 until we run out of datasets
        while datasets_available:
            # Building the API request URL
            api_request_url = remove_double_slashes(api_method_urls[i] + "?limit=" + str(datasets_per_request) + "&page=" + str(current_page))

            # Waiting before each request in order not to flood the API with requests
            sleep(2)

            # Printing the current API request URL
            print("\n" + "Currently crawling: " + api_request_url + "\n")
            
            try:
                # Making the API request and deserializing the JSON response string
                metadata = json.loads(requests.get(api_request_url).text)

                # Stopping the loop if a page after the first one is empty (no datasets are available anymore)
                if current_page > 1 and metadata == []:
                    datasets_available = False
                    break

                # Counting the number of datasets in the metadata
                datasets_in_current_response = len(metadata)

                # Iterating over all the metadata of the current response
                for j in range(datasets_in_current_response):

                    # Printing the current dataset number
                    print("Dataset " + str(j + 1 + total_number_of_datasets))

                    # Getting the dataset ID and asset type
                    dataset_id = metadata[j]["id"]
                    asset_type = metadata[j]["assetType"]

                    # Getting the dataset URL if the asset type is supported, otherwise skipping the dataset
                    if asset_type == "dataset":
                        dataset_url = remove_double_slashes(api_method_urls[i] + "/") + dataset_id + "/rows.csv?accessType=DOWNLOAD"
                    # After TESTING if the asset types "chart", "datalens", "filter" also always work with the method above, replace the if-statement above with ---> if asset_type == "dataset" or asset_type == "chart" or asset_type == "datalens" or asset_type == "filter": <---
                    elif asset_type == "file":
                        dataset_url = remove_double_slashes(api_base_urls[i] + "/download/") + dataset_id
                    else:
                        continue

                    # Increasing the number of supported datasets
                    number_of_supported_datasets += 1

                    # Building the metadata URL
                    metadata_url = remove_double_slashes(api_method_urls[i] + "/metadata/v1/") + dataset_id

                    # Building the source URL
                    source_url = remove_double_slashes(api_base_urls[i] + "/d/") + dataset_id

                    # Calling the Archiver connector to insert data into the Archiver
                    archiver.handle_dataset(dataset_url, metadata_url, source_url, log_file_success, log_file_fail)

                    # Printing dataset information
                    # print("Dataset URL: " + dataset_url)
                    # print("Metadata URL: " + metadata_url)
                    # print("Source URL: " + source_url + "\n")
                    
                # Increasing the number of the page to be requested
                current_page += 1

                # Adding the current datasets to the total dataset number
                total_number_of_datasets += datasets_in_current_response 

                # Stopping the loop after this iteration if less than 800 datasets are returned (meaning that these are the last available datasets)
                if datasets_in_current_response != datasets_per_request:
                    datasets_available = False
            except Exception as exception:
                # Saving the failed API requests to the dataframe, then saving them to a new CSV file or appending them to an existing one
                failed_api_requests.loc[0, "timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                failed_api_requests.loc[0, "api_request_url"] = api_request_url
                failed_api_requests.loc[0, "exception"] = repr(exception)
                try:
                    failed_api_requests.loc[0, "dataset_url"] = dataset_url
                    failed_api_requests.loc[0, "metadata_url"] = metadata_url
                    failed_api_requests.loc[0, "source_url"] = source_url
                except NameError:
                    pass
                failed_api_requests.to_csv(failed_api_requests_filename, mode = "a", index = False, header = not os.path.isfile(failed_api_requests_filename))
                print("An exception occurred!")

                # If the maximum number of attempts has been reached, skipping the portal 
                if attempt_number == maximum_attempts:
                    datasets_available = False
                    error = True
                # Otherwise, increasing the number of attempts by 1
                else:
                    attempt_number += 1
                    sleep(2)
        
        # Printing the number of (total/supported) datasets
        print("\n" + "Total number of datasets on " + api_base_urls[i] + " : " + str(total_number_of_datasets))
        print("Number of supported datasets on " + api_base_urls[i] + " : " + str(number_of_supported_datasets) + "\n")

        # Saving information to the statistics dataframe
        portal_statistics.loc[0, "url"] = api_base_urls[i]
        portal_statistics.loc[0, "api_software"] = "Socrata"
        if error is False:
            portal_statistics.loc[0, "number_of_datasets"] = int(total_number_of_datasets)
            portal_statistics.loc[0, "number_of_supported_datasets"] = int(number_of_supported_datasets)
        portal_statistics.loc[0, "timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Export statistics to a CSV file
        portal_statistics.to_csv(statistics_file, mode = "a", index = False, header = not os.path.isfile(statistics_file))

