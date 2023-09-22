import requests
from time import sleep


def check_url(url: str) -> dict:
    """Requesting a URL and returning information about the response

    Args:
        url (str): the URL to be requested - must include a protocol prefix (http:// or https://)

    Returns:
        dict: {"request_success" = whether the request was successful, \n
                "response_code" = the HTTP response code,  \n
                "message" = success message or failure message with details about the error}
    """
    
    try:
        # Requesting the URL and getting the status code of the response
        response = requests.get(url, timeout = 15)
        response_code = response.status_code

        # Response indicates success
        if(response_code == 200):
            return {"request_success": True, "response_code": response_code, "message": "Worked"}
        # Response indicates redirect
        elif(response.is_redirect or response.is_permanent_redirect):
            return {"request_success": True, "response_code": response_code, "message": "Worked via redirect"}
        # Response indicates failure
        else:
            return {"request_success": False, "response_code": response_code, "message": "Did not work"}
    # Exception due to HTTPS failure
    except requests.exceptions.SSLError as exception:
        return {"request_success": False, "response_code": None, "message": "Did not work. HTTPS not implemented! Exception: " + str(exception)}
    # Exception due to other reasons
    except Exception as exception:
        return {"request_success": False, "response_code": None, "message": "Did not work. Exception: " + str(exception)}


def check_protocol(url: str, show_details: bool = True) -> str:
    """Requesting a URL with HTTPS and, if required, HTTP and returning the best possible variant of the URL

    Args:
        url (str): the URL for which the protocol should be checked - can be with or without HTTP(S) prefix
        show_details (bool, optional): whether or not to print details about the requests - defaults to True

    Returns:
        str: the working URL with protocol prefix (HTTPS > HTTP) or non-working URL without protocol prefix
    """

    # Removing the HTTP(S) protocol prefix if there is one
    if url.startswith("https://"):
        url = url.split("https://")[-1]
    elif url.startswith("http://"):
        url = url.split("http://")[-1]
    
    # Building the HTTP(S) portal URLs
    https_url = "https://" + url
    http_url = "http://" + url
    
    # Requesting the HTTPS URL
    https_request = check_url(https_url)
    if show_details == True:
        print("Protocol: HTTPS | Response code: " + str(https_request["response_code"]) + " | Message: " + str(https_request["message"]))
    # HTTPS URL works
    if (https_request["request_success"] == True):
        url = https_url
    else:
        # Waiting 1 second
        sleep(1)
        # Requesting the HTTP URL
        http_request = check_url(http_url)
        if show_details == True:
            print("Protocol: HTTP | Response code: " + str(http_request["response_code"]) + " | Message: " + str(http_request["message"]))
        # HTTP URL works
        if (http_request["request_success"] == True):
            url = http_url

    # Returning the final version of the URL
    return url


def remove_double_slashes(url: str) -> str:
    """Replacing a double forward slash with a single forward slash in any part of a HTTP(S) URL except for the protocol prefix

    Args:
        url (str): the URL to be modified which may contain double forward slashes

    Returns:
        str: the modified URL with only single forward slashes
    """

    if url.startswith("https://"):
        # Removing protocol prefix
        url = url.split("https://")[-1]

        # Replacing double slashes with a single slash
        url = url.replace("//", "/")

        # Restoring protocol prefix
        url = "https://" + url
    elif url.startswith("http://"):
        # Removing protocol prefix
        url = url.split("http://")[-1]

        # Replacing double slashes with a single slash
        url = url.replace("//", "/")

        # Restoring protocol prefix
        url = "http://" + url

    # Returning the modified URL
    return url

