# Importing required libraries
import os
import json
import pymongo
import requests
import pandas as pd
from time import sleep
from datetime import datetime
from urllib.parse import quote
from dotenv import dotenv_values


class ArchiverConnector:
    """A class containing functions that connect to the Archiver API (using HTTP requests) and the Archiver database (via MongoDB queries).
    """

    def __init__(self, mode: str):
        """Instantiating the class.

        Args:
            mode (str): which MongoDB to connect to - must be "local" or "production"
        """      

        # Loading environment variables 
        config = dotenv_values("../.env")

        # Setting Archiver API variables
        self.archiver_base_url = config["ARCHIVER_BASE_URL"]
        self.archiver_password = config["ARCHIVER_PASSWORD"]

        # Connecting to the MongoDB
        print("Connecting to MongoDB...")

        def check_mongodb(mongodb_url, mongodb_name):
            self.archiver_database = pymongo.MongoClient(mongodb_url)[mongodb_name]
            self.mapping_collection = self.archiver_database["datasets.mappings"]
            with pymongo.timeout(5):
                self.mapping_collection.find_one({"dataset_id": "12345"})
                print("Successfully connected.")

        if mode == "local":
            try:
                check_mongodb(config["LOCAL_MONGODB_URL"], config["LOCAL_MONGODB_NAME"])
            except:
                print("Connection failed.")
        elif mode == "production":
            try:
                check_mongodb(config["SERVER_MONGODB_URL_1"], config["SERVER_MONGODB_NAME"])
            except:
                print("Node 1 failed. Trying node 2...")
                try:
                    check_mongodb(config["SERVER_MONGODB_URL_2"], config["SERVER_MONGODB_NAME"])
                except:
                    print("Node 2 failed. Trying node 3...")
                    try:
                        check_mongodb(config["SERVER_MONGODB_URL_3"], config["SERVER_MONGODB_NAME"])
                    except:
                        print("Connection failed.")

    def api_get_dataset(self, dataset_url: str) -> dict:
        """Performing an API request to check if a dataset is indexed by the Archiver.

        Args:
            dataset_url (str): the URL of the dataset

        Returns:
            dict: {"request_success" = whether the request was successful, \n
                "dataset_found" = whether the dataset was found,  \n
                "dataset_id" = the ID of the dataset in the Archiver or None,  \n
                "message" = success message or failure message with details about the error}
        """

        # Encoding the dataset URL
        dataset_url = quote(dataset_url, safe = "")

        # Building the request URL and defining the HTTP headers
        archiver_request_url = self.archiver_base_url + 'api/v1/get/dataset/' + dataset_url

        headers = {
            'accept': 'application/json',
        }

        # Making the API call
        try:
            # Getting the response
            response = requests.get(url = archiver_request_url, headers = headers)

            # The request succeeded
            if(response.status_code == 200):
                # The dataset was found
                try:
                    return {"request_success": True, "dataset_found": True, "dataset_id": json.loads(response.content)["_id"], "message": "Success!"}
                # The dataset was not found (an exception occurred when accessing the "_id" value)
                except Exception as exception:
                    return {"request_success": True, "dataset_found": False, "message": "Exception: " + str(exception)}
            # The request failed (the response code is not 200)
            else:
                return {"request_success": False, "dataset_found": False, "message": "Response code: " + str(response.status_code)}
        # The request failed (an exception occurred when making the request)
        except Exception as exception:
            return {"request_success": False, "dataset_found": False, "message": "Exception: " + str(exception)}
        
    def api_add_dataset(self, dataset_url: str, source_url: str) -> dict:
        """Performing an API request to add a dataset to the Archiver.

        Args:
            dataset_url (str): the URL of the dataset
            source_url (str): the URL of the dataset's source

        Returns:
            dict: {"request_success" = whether the request was successful, \n
                "dataset_inserted" = whether the dataset was inserted}, \n
                "message" = success message or failure message with details about the error}
        """

        # Building the request URL and defining the HTTP headers, parameters and data
        archiver_request_url = self.archiver_base_url + 'api/v1/post/resource'

        headers = {
            'accept': '*/*',
            'Content-Type': 'application/json'
        }

        params = (
            ('secret', self.archiver_password),
        )

        data = json.dumps([{"href": str(dataset_url), "source": str(source_url)}])

        # Making the API call
        try:
            # Getting the response
            response = requests.post(url = archiver_request_url, headers = headers, params = params, data = data)
            inserted = json.loads(response.content)[0]["insertedDatasets"]

            # The request succeeded
            if response.status_code == 200:
                # The dataset was inserted
                if inserted == 1:
                    return {"request_success": True, "dataset_inserted": True, "message": "Success!"}
                # The dataset was not inserted
                elif inserted == 0:
                    return {"request_success": True, "dataset_inserted": False, "message": "Dataset not inserted!"}
            # The request failed (the response code is not 200)
            else:
                return {"request_success": False, "dataset_inserted": False, "message": "Response code: " + str(response.status_code)}
        # The request failed (an exception occurred when making the request)
        except Exception as exception:
            return {"request_success": False, "dataset_inserted": False, "message": "Exception: " + str(exception)}
    
    def mongodb_get_mapping(self, dataset_id: str, metadata_id: str) -> dict:
        """Checking if there is an existing mapping between a dataset and its metadata in the "datasets.mappings" collection.

        Args:
            dataset_id (str): the ID of the dataset in the Archiver
            metadata_id (str): the ID of the metadata in the Archiver

        Returns:
            dict: {"query_success" = whether the query was successful, \n
                "dataset_found" = whether the dataset was found in any mapping, \n
                "metadata_found" = whether the metadata was found in any mapping, \n
                "mapping_found" = whether a mapping between the dataset and the metadata was found, \n
                "message" = success message or failure message with details about the error}
        """

        # Executing queries to determine if the dataset and metadata exist in the collection and if they are mapped to each other
        try:
            if self.mapping_collection.find_one({"dataset_id": dataset_id}) is None:
                dataset_found = False
            else:
                dataset_found = True

            if self.mapping_collection.find_one({"metadata_id": metadata_id}) is None:
                metadata_found = False
            else:
                metadata_found = True

            if self.mapping_collection.find_one({"dataset_id": dataset_id, "metadata_id": metadata_id}) is None:
                mapping_found = False
            else:
                mapping_found = True

            return {"query_success": True, "dataset_found": dataset_found, "metadata_found": metadata_found, "mapping_found": mapping_found, "message": "Success!"}
        # The query failed, returning error details
        except Exception as exception:
            return {"query_success": False, "message": "Exception: " + str(exception)}

    def mongodb_add_mapping(self, dataset_id: str, metadata_id: str) -> dict:
        """Adding a mapping entry for the dataset and metadata in the Archiver MongoDB.

        Args:
            dataset_id (str): the ID of the dataset in the Archiver
            metadata_id (str): the ID of the metadata in the Archiver

        Returns:
            dict: {"inserted" = whether the mapping was inserted, \n
                "mapping_id" = the ID of the mapping document or None, \n
                "message" = success message or failure message with details about the error}
        """

        # Creating the dictionary to be inserted as a document
        document = {"dataset_id": dataset_id, "metadata_id": metadata_id, "added": datetime.now().isoformat()}

        # Inserting the mapping document into the collection
        try:
            insert_status = self.mapping_collection.insert_one(document)
            return {"inserted": insert_status.acknowledged, "mapping_id": insert_status.inserted_id, "message": "Success!"}
        # The insertion failed, returning error details
        except Exception as exception:
            return {"inserted": False, "message": "Exception: " + str(exception)}
        
    def handle_dataset(self, dataset_url: str, metadata_url: str, source_url: str, log_file_success: str, log_file_fail: str) -> dict:
        """Checking if a dataset and its metadata are both indexed by the Archiver and have a mapping that describes their relation. Any missing indexing or mapping is added.

        Args:
            dataset_url (str): the URL of the dataset
            metadata_url (str): the URL of the dataset's metadata
            source_url (str): the URL of the dataset's source
            log_file_success (str): the path of a CSV file logging successfully handled datasets
            log_file_fail (str): the path of a CSV file logging datasets for which an exception occurred

        Returns:
            dict: {"success" = whether the process was successfully completed, \n
                "dataset_added" = whether the dataset was inserted via the API, \n
                "metadata_added" = whether the metadata was inserted via the API, \n
                "mapping_added" = whether a mapping between the dataset and the metadata was added via the MongoDB, \n
                "dataset_id" = the ID of the dataset in the Archiver or None, \n
                "metadata_id" = the ID of the metadata in the Archiver or None, \n
                "message" = success message or failure message with details about the error}

        """

        # Getting the current timestamp
        current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Waiting - adjust this in case of problems with excessive requests to the Archiver API!
        sleep(1)

        # Creating a dataframe in case the dataset can be handled successfully
        completed_dataset = pd.DataFrame([(None, None, None, None, None, None, None, None, None, None)], columns = ["timestamp", "dataset_url", "metadata_url", "source_url", "dataset_id", "metadata_id", "dataset_added", "metadata_added", "mapping_added", "message"])
        completed_datasets_filename = log_file_success

        # Creating a dataframe in case the dataset cannot be handled successfully
        failed_dataset = pd.DataFrame([(None, None, None, None, None, None, None, None)], columns = ["timestamp", "dataset_url", "metadata_url", "source_url", "dataset_added", "metadata_added", "mapping_added", "message"])
        failed_datasets_filename = log_file_fail

        # Initially setting the variables that log whether an error or certain relevant actions occurred to False
        failed = False
        dataset_added = False
        metadata_added = False
        mapping_added = False

        # Creating a list containing the dataset URL and metadata URL
        data_list = [{"url": dataset_url, "type": "dataset"}, 
                     {"url": metadata_url, "type": "metadata"}]

        # For dataset and metadata, checking if the Archiver is indexing it already and adding it if necessary
        for data in data_list:
            # Checking if the dataset/metadata is indexed by the Archiver
            check_data = self.api_get_dataset(data["url"])

            # The request to check the dataset/metadata failed, the loop stops
            if check_data["request_success"] == False:
                failed = True
                fail_reason = "Initially getting the " + data["type"] + " via the Archiver API failed."
                break
            # The dataset/metadata was not found
            elif check_data["dataset_found"] == False:
                # Adding the dataset/metadata to the Archiver
                add_data = self.api_add_dataset(data["url"], source_url)

                # The request to add the dataset/metadata failed or it wasn't inserted, the loop stops
                if add_data["dataset_inserted"] == False:
                    failed = True
                    fail_reason = "Adding the " + data["type"] + " via the Archiver API failed."
                    break
                # The dataset/metadata was inserted
                elif add_data["dataset_inserted"] == True:
                    # Checking again if the dataset/metadata is indexed by the Archiver
                    check_data = self.api_get_dataset(data["url"])

                    # The request to check the dataset/metadata failed or it wasn't found, the loop stops
                    if check_data["dataset_found"] == False:
                        failed = True
                        fail_reason = "After successfully adding the " + data["type"] + ", getting it via the Archiver API failed."
                        break
                    # Logging the successful and verified insertion of the dataset/metadata
                    elif check_data["dataset_found"] == True:
                        if data["type"] == "dataset":
                            dataset_added = True
                        elif data["type"] == "metadata":
                            metadata_added = True

            # The dataset/metadata was found (either directly or after adding it)
            if check_data["dataset_found"] == True:
                # Saving the dataset ID
                if data["type"] == "dataset":
                    dataset_id = check_data["dataset_id"]
                # Saving the metadata ID
                elif data["type"] == "metadata":
                    metadata_id = check_data["dataset_id"]

        # No error occurred so far
        if failed == False:
            # Checking the dataset/metadata mapping
            check_mapping = self.mongodb_get_mapping(dataset_id, metadata_id)

            # The query to check the dataset/metadata mapping failed
            if check_mapping["query_success"] == False:
                failed = True
                fail_reason = "Initially getting the dataset/metadata mapping via the Archiver MongoDB failed."
            # The query succeeded, checking if the mapping exists already and adding it if necessary
            elif check_mapping["query_success"] == True:
                # The mapping already exists
                if check_mapping["mapping_found"] == True:
                    mapping_added = False
                # The mapping doesn't exist yet, so it is now added
                elif check_mapping["mapping_found"] == False:
                    add_mapping = self.mongodb_add_mapping(dataset_id, metadata_id)

                    # The query to add the dataset/metadata mapping failed or it wasn't inserted
                    if add_mapping["inserted"] == False:
                        failed = True
                        fail_reason = "Adding the mapping via the Archiver MongoDB failed."
                    # Logging the successful insertion of the mapping
                    elif add_mapping["inserted"] == True:
                            mapping_added = True

                # Checking the dataset/metadata mapping again
                check_mapping = self.mongodb_get_mapping(dataset_id, metadata_id)
                if check_mapping["mapping_found"] == False:
                    failed = True
                    fail_reason = "After successfully adding the dataset/metadata mapping, getting it via the Archiver MongoDB failed."

        # Saving the completed / failed dataset to the dataframe, then saving it to a new CSV file or appending it to an existing one
        if failed == True:
            failed_dataset.iloc[0,] = {"timestamp": current_timestamp, "dataset_url": dataset_url, "metadata_url": metadata_url, "source_url": source_url, "dataset_added": dataset_added, "metadata_added": metadata_added, "mapping_added": mapping_added, "message": "Failed: " + fail_reason}
            failed_dataset.to_csv(failed_datasets_filename, mode = "a", index = False, header = not os.path.isfile(failed_datasets_filename))
            return {"success": False, "dataset_added": dataset_added, "metadata_added": metadata_added, "mapping_added": mapping_added, "message": "Failed: " + fail_reason}
        elif failed == False:
            completed_dataset.iloc[0,] = {"timestamp": current_timestamp, "dataset_url": dataset_url, "metadata_url": metadata_url, "source_url": source_url, "dataset_id": dataset_id, "metadata_id": metadata_id, "dataset_added": dataset_added, "metadata_added": metadata_added, "mapping_added": mapping_added, "message": "Success! Data and mapping complete!"}
            completed_dataset.to_csv(completed_datasets_filename, mode = "a", index = False, header = not os.path.isfile(completed_datasets_filename))
            return {"success": True, "dataset_added": dataset_added, "metadata_added": metadata_added, "mapping_added": mapping_added, "dataset_id": dataset_id, "metadata_id": metadata_id, "message": "Success! Data and mapping complete!"}
