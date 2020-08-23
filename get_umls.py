"""Get UMLS.

This is a modify module, the original one is called 'retrieve-cui-or-code.py' from UMLS Terminology Services REST API.

This module needs the Authentication module and the *api key* for UMLS Terminology Services REST API.

To avoid a wrong mapping between MeSH codes to UMLS codes, this script first take a MeSH code and look for the possible UMLS codes,

Tthen if it has more than one posible UMLS, it will compare the names using the module 'SequenceMatcher' from the 'difflib' library,

The UMLS will be the one with the high percentage of similarity.
"""
from Authentication import *
import requests
import json
import re
from difflib import SequenceMatcher



apikey = "836ab0e6-945c-4750-8259-0f4cb9c7d6df"
version = "current"
AuthClient = Authentication(apikey)

###################################
#get TGT for our session
###################################

tgt = AuthClient.gettgt()
uri = "https://uts-ws.nlm.nih.gov"

def get_umls(identifier,disorder_name,source):
    """ The parameter of this function are the MeSH id, the MeSH name and the source.

    Args:
        identifier (str): The identifier of the disorder.
        disorder_name (str): The name of the disorder.
        source (str): The name of the source that the identifier is, this name has to be in the way that are in UMLS.
    
    Returns:
        UMLS (str): The UMLS code
    """

    def similar(a, b):
        """ This function is for getting a percentage of similarity between the names of the diseases.

        Args:
            a (str): Name of the disorder.
            b (str): Name of the disorder.

        Returns:
            Percentage (float): The percentage of similitud between two strings. 

        
        """
        
        percentage = SequenceMatcher(None, a, b).ratio()
        return percentage

    content_endpoint = "/rest/content/"+str(version)+"/source/"+str(source)+"/"+str(identifier)+"/atoms"
    ##ticket is the only parameter needed for this call - paging does not come into play because we're only asking for one Json object
    query = {'ticket':AuthClient.getst(tgt)}
    r = requests.get(uri+content_endpoint,params=query)

    if not r.status_code == 404:
        r.encoding = 'utf-8'
        items  = json.loads(r.text)
        jsonData = items["result"]
        similarities=[]
        if len(jsonData) >2:
            for i in range(len(jsonData)):
                name = jsonData[i]["name"]
                similarity_percentage=similar(name,disorder_name)
                similarities.append(similarity_percentage)
            location=similarities.index(max(similarities))
            concept = jsonData[location]["concept"]
            umls = re.split('CUI/',concept)
            return umls[1]
        else:
            concept = jsonData[0]["concept"]
            name = jsonData[0]["name"]
            umls = re.split('CUI/',concept)
            return umls[1]
    else:
        string=disorder_name
        content_endpoint_string = "/rest/search/"+version
        query = {'string':string,'ticket':AuthClient.getst(tgt)}
        r_string = requests.get(uri+content_endpoint_string,params=query)
        r_string.encoding = 'utf-8'
        items_string  = json.loads(r_string.text)
        jsonData_string = items_string["result"]['results']
        similarities_string=[]
        if len(jsonData_string) >1:
            for i in range(len(jsonData_string)):
                name_string = jsonData_string[i]["name"]
                similarity_percentage_string=similar(name_string,disorder_name)
                similarities_string.append(similarity_percentage_string)
            location_string=similarities_string.index(max(similarities_string))
            umls = jsonData_string[location_string]["ui"]
            return umls
        else:
            umls = jsonData_string[0]["ui"]
            name_string = jsonData_string[0]["name"]
            return umls
