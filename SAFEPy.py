import requests
import zipfile
import os
import json
import re

# Define the URL
probe_url = "https://data.ntsb.gov/carol-main-public/api/Query/Main"
file_url = "https://data.ntsb.gov/carol-main-public/api/Query/FileExport"

#Load JSON into a more python-friendly dictionary
f = open('possible_values.json')
raw_json = json.load(f)

compressed_json = {}
for field in raw_json["fields"]:
    compressed_json[field["value"]] = {}
    tmp1_dict = compressed_json[field["value"]]
    if "subfields" in field.keys():
        for subfield in field["subfields"]:
            tmp1_dict[subfield["value"]] = {}
            tmp2_dict = tmp1_dict[subfield["value"]]
            tmp2_dict["input"] = subfield["input"]
            tmp2_dict["conditions"] = subfield["queryValues"][0]["conditions"]
            tmp2_dict["values"] = []
            for queryValue in subfield["queryValues"]:
                tmp2_dict["values"].append(queryValue["value"])
    else:
        tmp1_dict[None] = {}
        tmp1_dict[None]["input"] = field["input"]
        tmp1_dict[None]["conditions"] = field["queryValues"][0]["conditions"]
        tmp1_dict[None]["values"] = []
        for queryValue in field["queryValues"]:
            tmp1_dict[None]["values"].append(queryValue["value"])

class query_keys:
    """Query keys macro
    Has all possible values for each query field
    """
    fields = list(compressed_json.keys())
    subfields = [subfield for field in fields for subfield in compressed_json[field]]
    conditions = set([condition for field in fields for subfield in compressed_json[field] for condition in compressed_json[field][subfield]["conditions"]])
    values = set([condition for field in fields for subfield in compressed_json[field] for condition in compressed_json[field][subfield]["values"]])

class CAROLQuery:
    """CAROL Query class
    Builds queries using a set of rules and then probes the CAROL database to find results that match its query.
    Can also download the results as well.
    """
    def __init__(self):
        # Create a session with connection pooling
        self._session = requests.Session()
        self._data = compressed_json

        # Opening JSON file with all values
        f = open('possible_values.json')
        raw_json = json.load(f)

        #Creates an unfinished probe with rules to be added
        self._probe = {
            "ResultSetSize": 50,
            "ResultSetOffset": 0,
            "QueryGroups": [
                {
                    "QueryRules": [],
                    "AndOr": "and",
                    "inLastSearch": False,
                    "editedSinceLastSearch": False
                }
            ],
            "AndOr": "and",
            "SortColumn": None,
            "SortDescending": True,
            "TargetCollection": "cases",
            "SessionId": 217242
        }

        #Creates an unfinished payload (download probe) with rules to be added
        self._payload = {
            "QueryGroups": [
                {
                    "QueryRules": [],
                    "AndOr": "or",
                    "inLastSearch": False,
                    "editedSinceLastSearch": False
                }
            ],
            "AndOr": "and",
            "TargetCollection": "cases",
            "ExportFormat": "summary",
            "SessionId": 217242,
            "ResultSetSize": 50,
            "SortDescending": True
        }

    def __del__(self):
        self._session.close()

    def addQueryRule(self, field, subfield, condition, values):
        """Adds a query rule to the CAROLQuery class.
        """
        if condition and subfield and field:
            input_t = self._data[field][subfield]["input"]

        elif field == "HasSafetyRec":
            input_t = self._data[field][subfield]["input"]

        else:
            field = "Narrative"
            subfield = "Factual"
            condition = "contains"
            input_t = self._data[field][subfield]["input"]


        rule =  {
            "RuleType": "Simple",
            "Values": [values],
            "Columns": [f"{field}.{subfield}"],
            "Operator": condition,
            "selectedOption": {
                "FieldName": subfield,
                "DisplayText": "",
                "Columns": [f"{field}.{subfield}"],
                "Selectable": True,
                "InputType": input_t,
                "RuleType": 0,
                "Options": None,
                "TargetCollection": "cases",
                "UnderDevelopment": False
            },
            "overrideColumn": ""
        }

        self._probe["QueryGroups"][0]["QueryRules"].append(rule)
        self._payload["QueryGroups"][0]["QueryRules"].append(rule)

    def clear(self):
        """Clears existing query rules.
        """
        print("Clearing query parameters...")
        self._probe["QueryGroups"][0]["QueryRules"] = []
        self._payload["QueryGroups"][0]["QueryRules"] = []
        print("Query parameters cleared!")

    def query(self):
        """Sends a query probe to the CAROL Database.
        """

        # Send the probe POST request
        response = None
        try:
            # print query parameters currently working on
            print("Querying CAROL...")
            response = self._session.post(probe_url, json=self._probe, timeout=60)
        except requests.exceptions.Timeout:
            print("The request timed out")
        except requests.exceptions.RequestException as e:
            # handle other types of exceptions
            print("An error occurred: ", e)

        # check the response
        result_list_count = None
        max_result_count_reached = None
        if response is None:
            return
        else:
            # Ensure we got a successful response
            response.raise_for_status()

            print("\nsuccessful probe")

            # Convert the response to JSON
            response_json = response.json()

            # Extract the 'ResultListCount' and 'MaxResultCountReached' values
            result_list_count = response_json['ResultListCount']
            max_result_count_reached = response_json['MaxResultCountReached']

            print(f'Result count: {result_list_count}')
            print(f'Max reached: {max_result_count_reached}\n')

    def download(self):
        """Sends a download probe to the CAROL database.
        """

        # Send the file POST request
        response = None
        try:
            # print dots to signify working
            print(f"Downloading query from CAROL...")
            response = self._session.post(file_url, json=self._payload, timeout=60)
        except requests.exceptions.Timeout:
            print("The request timed out")
        except requests.exceptions.RequestException as e:
            # handle other types of exceptions
            print("An error occurred: ", e)

        # check the response
        if response is None:
            return
        else:
            # Ensure we got a successful response
            response.raise_for_status()

            print("\nsuccessful file request")

            folder = response.headers.get('Content-Disposition')

            # Check if the header exists
            if folder is not None:
                # Split the header on whitespace and equals sign
                parts = folder.split()

                for part in parts:
                    if part.startswith('filename='):
                        # Extract the filename
                        # skip the first 9 characters ('filename=') and the last 4 ('.zip')
                        folder = part[9:-4]
                        break
            else:
                print("No Content-Disposition header found.")

            # Create the output directory if it doesn't exist
            os.makedirs('./output', exist_ok=True)

            # write zip to file
            with open(f'./output/{folder}.zip', 'wb') as f:
                f.write(response.content)

            # create a ZipFile object
            with zipfile.ZipFile(f'./output/{folder}.zip', 'r') as zip_ref:
                # extract all the contents of the zip file to the current directory
                zip_ref.extractall(f'./output/')

            # remove the zip file
            os.remove(f'./output/{folder}.zip')

def query_decide(value):
    """Using pattern matching, decides which field, subfield, and condition a arbitrary value falls under.
    """
    date_regex = r"[0-9]{1,2}[\/|-][0-9]{1,2}[\/|-][0-9]{2,4}"

    if re.match(date_regex, value):
        return "Event", "EventDate", "is after"

    return "Narrative", "Factual", "contains"

def query_key_sort(value):
    str_methods = [str, str.lower, str.capitalize, str.upper]
    is_matched = False
    for m in str_methods:
        value = m(value)
        if value in query_keys.fields:
            return 0, value
        if value in query_keys.subfields:
            return 1, value
        if value in query_keys.conditions:
            return 2, value
        if value in query_keys.values:
            return 3, value
    return -1, value

def query_rule_sort(arg):
    rule = [None]*4

    if (type(arg) == str) or (len(arg) == 1):
            if type(arg) == tuple: arg = arg[0]
            rule[0:3] = query_decide(arg)
            rule[3] = arg

    #If two arguments found
    elif (len(arg) == 2):
        pass

    #If three argument found
    elif (len(arg) == 3):
        arg_list = list(arg)
        tmp = []
        while arg_list:
            a = arg_list.pop()
            key, a_match = query_key_sort(a)
            if key < 0:
                tmp.append(a)
            else:
                rule[key] = a_match
        while tmp:
            t = tmp.pop()
            t_splt = t.split()
            if (len(t_splt) == 2):
                for t_str in t_splt:
                    key, t_match = query_key_sort(t_str)
                    if key < 0:
                        rule[3] = t
                    else:
                        rule[key] = t_match
            else:
                rule[3] = t
    
    #If four arguments found
    elif (len(arg) == 4):
        for a in arg:
            key, a_match = query_key_sort(a)
            rule[key] = a_match

    return rule

def query(*args, download = False):
    """A one-time query to the CAROL Database.
    The queries are input as a list of tuples or strings.
    """

    #If no arguments, raise ValueError
    if len(args) == 0:
        raise ValueError("No queries found")

    #Query class
    q = CAROLQuery()

    #Sorts through the args
    for arg in args:
        field, subfield, condition, value = query_rule_sort(arg)
        
        #Check to make sure all query parameters were filed
        e_list = []
        if not field:
            e_list.append("Field")
        if not subfield and field != "HasSafetyRec":
            e_list.append("Subfield")
        if not condition:
            e_list.append("Condition")
        if not value:
            e_list.append("Value")
            
        if len(e_list):
            raise ValueError(f"Incorrect {e_list} found in argument {arg}.")

        #Add query rule from args
        q.addQueryRule(field, subfield, condition, value)

    #Run the query
    q.query()

    #Download query
    if (download):
        q.download()
    
    #Return query object
    return q

