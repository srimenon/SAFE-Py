import requests
import zipfile
import os
import json
import re
from dateutil import parser
from datetime import datetime
import time
from multiprocessing import Manager, Pool, Lock, cpu_count
from functools import partial
import copy
import pandas as pd

probe_url = "https://data.ntsb.gov/carol-main-public/api/Query/Main"
file_url = "https://data.ntsb.gov/carol-main-public/api/Query/FileExport"

# Load JSON into dictionary
f = open('possible_values.json')
raw_json = json.load(f)

# create a compressed version of the json
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
     
def extract_available_options(json_file_path):
    '''Extracts available options from a JSON file with a specific structure.'''
    
    # Load the JSON file
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    
    # Initialize a list to store the available options
    available_options = []

    # Define a helper function to extract the available options
    def extract_options(field, parent_field=None):
        field_name = field['value']
        if 'subfields' in field:
            for subfield in field['subfields']:
                extract_options(subfield, parent_field=field_name)
        elif 'queryValues' in field:
            for query_value in field['queryValues']:
                value = query_value['value']
                for condition in query_value['conditions']:
                    available_options.append([parent_field, field_name, condition, value])
        else:
            # Case where the field/subfield has no subfields or queryValues
            available_options.append([parent_field, field_name, None, None])

    # Iterate over the fields in the JSON data to extract the available options
    for field in data['fields']:
        extract_options(field)
    
    return available_options

# multiprocessing lock init
def init(ql, dl, fl, cl):
    '''Initializes the locks for multiprocessing'''
    
    global query_lock
    global download_lock
    global file_lock
    global complete_lock
    query_lock = ql
    download_lock = dl
    file_lock = fl
    complete_lock = cl

class MalformedQueryError(Exception):
    '''Exception raised for errors in the input.'''
    pass

class query_keys:
    '''
    Query keys macro
    Has all possible values for each query field
    '''
    fields = list(compressed_json.keys())
    subfields = [subfield for field in fields for subfield in compressed_json[field]]
    conditions = set([condition for field in fields for subfield in compressed_json[field] for condition in compressed_json[field][subfield]["conditions"]])
    values = set([condition for field in fields for subfield in compressed_json[field] for condition in compressed_json[field][subfield]["values"]])

class query_rule:
    def __init__(self, field=None, subfield=None, condition=None, value=None):
        self.field = field
        self.subfield = subfield
        self.condition = condition
        self.value = value

class CAROLQuery:
    '''
    CAROL Query class
    Builds queries using a set of rules and then probes the CAROL database to find results that match its query. Can also download the results as well.
    '''
    def __init__(self):
        '''Initializes the CAROLQuery class.'''
        
        # Create a session with connection pooling
        self._session = requests.Session()
        self._data = compressed_json

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
            "AndOr": "or",
            "SortColumn": None,
            "SortDescending": True,
            "TargetCollection": "cases",
            "SessionId": 100000
        }

        #Creates an unfinished payload (download probe) with rules to be added
        self._payload = {
            "QueryGroups": [
                {
                    "QueryRules": [],
                    "AndOr": "and",
                    "inLastSearch": False,
                    "editedSinceLastSearch": False
                }
            ],
            "AndOr": "or",
            "TargetCollection": "cases",
            "ExportFormat": "summary",
            "SessionId": 100100,
            "ResultSetSize": 50,
            "SortDescending": True
        }
        
        self._query_group = {
            "QueryRules": [],
            "AndOr": "and",
            "inLastSearch": False,
            "editedSinceLastSearch": False
        }
        
        self._curr_group_index = 0
        self._result_list_count = None
        self._max_result_count_reached = None
        self._date_constraints = []
        self._general_constraints = []
        self._values = []
        self._used_rule_sets = []
        
    def __del__(self):
        '''Closes the session when the object is deleted.'''
        self._session.close()

    def addQueryGroup(self, rule, condition, subfield, has_key_constraint):
        '''Adds a query group to the CAROLQuery class.'''
        if has_key_constraint:
            # if we already have a rule in the current query group
            if len(self._probe["QueryGroups"][self._curr_group_index]["QueryRules"]) > 2 and subfield != "ID":
                self._probe["QueryGroups"].append(copy.deepcopy(self._query_group))
                self._payload["QueryGroups"].append(copy.deepcopy(self._query_group))
                # update group count
                self._curr_group_index = len(self._probe["QueryGroups"]) - 1
                # add the constraint to gen list
                self._general_constraints.append(rule)             
            elif len(self._probe["QueryGroups"][self._curr_group_index]["QueryRules"]) == 0 and subfield == "ID" and condition == "is greater than":
                self._query_group["QueryRules"].append(rule)
            elif len(self._probe["QueryGroups"][self._curr_group_index]["QueryRules"]) == 1 and subfield == "ID" and condition == "is less than":
                self._query_group["QueryRules"].append(rule)
            elif subfield != "ID":
                self._general_constraints.append(rule)
        else:
            # if no group yet
            if len(self._probe["QueryGroups"]) == 1 and len(self._probe["QueryGroups"][self._curr_group_index]["QueryRules"]) == 0:
                if subfield != "ID":
                    self._general_constraints.append(rule)
            else:
                # Create a new query group for every rule
                self._probe["QueryGroups"].append(copy.deepcopy(self._query_group))
                self._payload["QueryGroups"].append(copy.deepcopy(self._query_group))
                self._curr_group_index = len(self._probe["QueryGroups"]) - 1
                if subfield != "ID":
                    self._general_constraints.append(rule)

    def addQueryRule(self, field, subfield, condition, values, andOr, has_key_constraint):
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
            
        self._values.append(f'{subfield} {condition} {values}')

        rule = None
        if subfield:
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
        else:
            rule =  {
                "RuleType": "Simple",
                "Values": [values],
                "Columns": [f"{field}"],
                "Operator": condition,
                "selectedOption": {
                    "FieldName": subfield,
                    "DisplayText": "",
                    "Columns": [f"{field}"],
                    "Selectable": True,
                    "InputType": input_t,
                    "RuleType": 0,
                    "Options": None,
                    "TargetCollection": "cases",
                    "UnderDevelopment": False
                },
                "overrideColumn": ""
            }
        
        # the current number of groups - 1
        self._curr_group_index = len(self._probe["QueryGroups"]) - 1
        
        # if "or" logic and not the first rule
        if not andOr:
            self.addQueryGroup(rule, condition, subfield, has_key_constraint)
                    
        # append the rule to the probe and the download payload
        self._probe["QueryGroups"][self._curr_group_index]["QueryRules"].append(rule)
        self._payload["QueryGroups"][self._curr_group_index]["QueryRules"].append(rule)

    def clear(self):
        '''Clears existing query rules.'''
        
        print("Clearing query parameters...")
        self._probe["QueryGroups"][0]["QueryRules"] = []
        self._payload["QueryGroups"][0]["QueryRules"] = []
        print("Query parameters cleared!")
              
    def query(self, download=False):
        '''Sends a query probe to the CAROL Database.'''

        # Send the probe POST request
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
        response = None
        try:
            if download:
                with query_lock:
                    # avoids erroring concurrent api requests
                    time.sleep(2)
            # print query parameters currently working on
            print("Querying CAROL...")
            print(f"Query for {self._values}")
            # print(self._probe)
            response = self._session.post(probe_url, json=self._probe, timeout=60, headers=headers)
                
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
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                print(f"An error occured querying {self._values}: {e}")

            print("\nsuccessful probe")

            # Convert the response to JSON
            response_json = response.json()

            # Extract the 'ResultListCount' and 'MaxResultCountReached' values
            self._result_list_count = response_json['ResultListCount']
            self._max_result_count_reached = response_json['MaxResultCountReached']

            print(f'Result count: {self._result_list_count}')
            print(f'Max reached: {self._max_result_count_reached}\n')

    def download(self, filenames, number_complete, total_queries):
        '''Sends a download probe to the CAROL database.'''

        # Send the file POST request
        response = None
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}
        try:
            with query_lock:
                # avoids erroring concurrent api requests
                time.sleep(5)
            # print dots to signify working
            print(f"Downloading data from CAROL...")
            response = self._session.post(file_url, json=self._payload, timeout=60, headers=headers)
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
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                print(f"An error occured downloading {self._values}: {e}")

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
                
            # lock critical section for multiprocessing
            with file_lock:
                # Create the output directory if it doesn't exist
                os.makedirs('./output', exist_ok=True)

                # write zip to file
                with open(f'./output/{folder}.zip', 'wb') as f:
                    f.write(response.content)

                # create a ZipFile object
                with zipfile.ZipFile(f'./output/{folder}.zip', 'r') as zip_ref:
                    # extract all the contents of the zip file to the current directory
                    zip_ref.extractall(f'./output/{self._values}')
                    filenames.append(f'./output/{self._values}/{folder}.csv')

                # remove the zip file
                os.remove(f'./output/{folder}.zip')
                
                with complete_lock:
                    number_complete.value += 1
                print(f"Completed {number_complete.value} of (at most) {total_queries.value}")
            
def to_standard_date_format(cond_str, date_str):
    '''Converts a date string to a standard format.'''
    
    try:
        date_obj = parser.parse(date_str)
        print(f"Detected condition: {cond_str.strip()}\n" + 
            f"Detected date: {date_str}\n" +
            f"Adding date condition to query.")
        return date_obj.strftime('%Y-%m-%d')
    except:
        raise ValueError(f"\nDetected condition: {cond_str.strip()}\n" + 
            f"Detected date: {date_str}\n" + 
            f"Valid conditions are: is on or before, is on or after, is before, is after, is, is not\n" +
            "Date condition is not in a valid format. Please enter dates in the following format: '<condition> mm/dd/yyyy'.")

def long_search(input_string):
    '''
    Checks if the input string ends with a punctuation
    or if the string is longer than 10 words.

    :param input_string: str, the input string to be checked
    :return: bool, True if the string ends with punctuation or is longer than 10 words, False otherwise
    '''
    # Define a set of punctuation characters
    punctuation_characters = {'.', '!', '?', ',', ';', ':', '-', '(', ')', '[', ']', '{', '}', '...', '\'', '\"'}
    
    # Check if the string ends with a punctuation character
    ends_with_punctuation = input_string[-1] in punctuation_characters if input_string else False
    
    # Count the number of words in the string
    word_count = len(input_string.split())
    
    # Check if the string has more than 10 words
    is_longer_than_ten_words = word_count > 10
    
    # Return True if either condition is met
    return ends_with_punctuation or is_longer_than_ten_words

def query_decide(value: str):
    '''Using pattern matching, decides which field, subfield, and condition an arbitrary value falls under.'''

    # Checks for full sentence
    if long_search(value):
        while True:
            user_input = input(f"Your input was: {value}\nFull sentences input to SAFEPy will search for the string in the Factual Narrative. Continue? (yes/no): ")
            normalized_input = user_input.lower().strip()

            # Check if user_input is "yes" (case-insensitive)
            if normalized_input == "no" or normalized_input == "n":
                raise MalformedQueryError("Aborting the query. Reformat and try again.")
            elif normalized_input == "yes" or normalized_input == "y":
                break
            else:
                print("Invalid input. Please enter 'yes' or 'no'.")
    
    # Date decision with greedy check
    normalized_value = value.lower().strip()
    try:
        parsed_date = parser.parse(normalized_value)
        # Check if the parsed date is in the future and adjust the year if necessary
        if parsed_date > datetime.now():
            parsed_date = parsed_date.replace(year=parsed_date.year - 100)
        print(f"Using default condition: is on or after\n" + 
            f"Detected date: {parsed_date}\n" +
            f"Adding date condition to query.")
        return "Event", "EventDate", "is on or after", parsed_date.strftime('%Y-%m-%d')
    except:
        pass

    # Date decision for dates including 'is on or before', ...
    cond_date_regex = r'^(is(?:(?: on or)?(?: before| after)| not)?) (.+)+$'
    match = re.match(cond_date_regex, normalized_value)
    if match:
        return "Event", "EventDate", match.group(1), to_standard_date_format(match.group(1), match.group(2))
    
    # # Check valid fields
    # query_key_section, _ = query_key_sort(normalized_value)
    # if query_key_section == 0:
    #     print("Found field")
    # elif query_key_section == 1:
    #     print("Found subfield")
    # elif query_key_section == 2:
    #     print("Found condition")
    # elif query_key_section == 3:
    #     print("Found value")
    # else:
    print(f"Searching factual narrative for: {normalized_value}")
    return "Narrative", "Factual", "contains", normalized_value

def query_key_sort(value):
    '''Sorts an arbitrary value into a field, subfield, condition, or value.'''
    str_methods = [str, str.lower, str.capitalize, str.upper]
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
    return -1, value.lower()

def query_rule_sort(arg):
    '''Sorts an arbitrary value into a field, subfield, condition, or value.'''
    rule = [None]*4

    # if one argument
    if (type(arg) == str) or (len(arg) == 1):
        if type(arg) == tuple: arg = arg[0]
        rule[0:4] = query_decide(arg)

    # If two arguments found
    elif (len(arg) == 2):
        pass

    # If three arguments found
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
        if rule[1] == 'EventDate':
            _, _, _, rule[3] = query_decide(rule[3])
            
    # if 5 or more args found
    else:
        raise MalformedQueryError("Too many arguments. Query rules must have 4 or fewer arguments.")

    return rule

def generate_key_segments_and(keys_per_segment, key_constraints):
    '''Generates key segments for the AND case.'''
    
    matching_keys = [[0, 200000]]
    for constraint in key_constraints:
        if 'greater than' in constraint:
            for key_pair in matching_keys:
                # if the key pair is within the constraint, then update the lower bound
                if key_pair[0] < int(constraint.split(' ')[-1]) + 1 < key_pair[1]:
                    key_pair[0] = int(constraint.split(' ')[-1]) + 1
                elif key_pair[1] < int(constraint.split(' ')[-1]):
                    matching_keys.remove(key_pair)
        elif 'less than' in constraint:
            for key_pair in matching_keys:
                # if the key pair is within the constraint, then update the lower bound
                if key_pair[0] < int(constraint.split(' ')[-1]) + 1 < key_pair[1]:
                    key_pair[1] = int(constraint.split(' ')[-1]) - 1
                elif key_pair[0] > int(constraint.split(' ')[-1]) + 1:
                    matching_keys.remove(key_pair)        
        elif 'not' in constraint:
            for key_pair in matching_keys:
                if key_pair[0] == int(constraint.split(' ')[-1]):
                    key_pair[0] += 1
                elif key_pair[1] == int(constraint.split(' ')[-1]):
                    key_pair[1] -= 1
                if key_pair[0] < int(constraint.split(' ')[-1]) < key_pair[1]:
                    matching_keys.append([key_pair[0], int(constraint.split(' ')[-1]) - 1])
                    matching_keys.append([int(constraint.split(' ')[-1]) + 1, key_pair[1]])
                    matching_keys.remove(key_pair)
        else:
            matching_keys = [((int(constraint.split(' ')[-1])), (int(constraint.split(' ')[-1])))]
            
    # Break the matching_keys into smaller segments of size keys_per_segment
    final_segments = []
    for key_pair in matching_keys:
        lower, upper = key_pair
        for i in range(lower, upper + 1, keys_per_segment):
            final_segments.append([i, min(i + keys_per_segment - 1, upper)])
    
    return final_segments

def generate_key_segments_or(keys_per_segment, key_constraints):
    '''Generates key segments for the OR case.'''
    
    global_greater = 200001
    global_lesser = -1
    is_conditions = []
    not_cond = None
    for constraint in key_constraints:
        if 'greater than' in constraint:
            global_greater = min(global_greater, int(constraint.split(' ')[-1]) + 1)
        elif 'less than' in constraint:
            global_lesser = max(global_lesser, int(constraint.split(' ')[-1]) - 1)
        elif 'not' in constraint:
            if not not_cond:
                not_cond = int(constraint.split(' ')[-1])
            elif not_cond and int(constraint.split(' ')[-1]) != not_cond or not_cond < global_lesser or not_cond > global_greater or not_cond in is_conditions:
                return [(x, x + keys_per_segment) for x in range(0, 200000, keys_per_segment)]                
        else:
            if not_cond and not_cond == int(constraint.split(' ')[-1]):
                return [(x, x + keys_per_segment) for x in range(0, 200000, keys_per_segment)]                
            is_conditions.append(int(constraint.split(' ')[-1]))
            
    # divide segments
    segments = []
    if global_greater <= global_lesser + 1:
        segments = [(x, min(x + keys_per_segment - 1, 200000)) for x in range(0, 200001, keys_per_segment)]
    elif global_greater > 200000 and global_lesser < 0:
        segments = [(x, min(x + keys_per_segment - 1, 200000)) for x in range(0, 200001, keys_per_segment)]
    elif global_greater > 200000:
        lesser = [(x, min(x + keys_per_segment - 1, global_lesser)) for x in range(0, global_lesser, keys_per_segment)]
        is_conds = [(x, x) for x in is_conditions if x > global_lesser]
        segments = lesser + is_conds
    elif global_lesser < 0:
        greater = [(x, min(x + keys_per_segment - 1, 200000)) for x in range(global_greater, 200001, keys_per_segment)]  # 200000 is the maximum possible key
        is_conds = [(x, x) for x in is_conditions if x < global_greater]
        segments = is_conds + greater
    else:
        # if one of the is conditions is between the lesser and greater conditions, then add to the list
        lesser = [(x, min(x + keys_per_segment - 1, global_lesser)) for x in range(0, global_lesser, keys_per_segment)]
        greater = [(x, min(x + keys_per_segment - 1, 200000)) for x in range(global_greater, 200001, keys_per_segment)]  # 200000 is the maximum possible key
        is_conds = [(x, x) for x in is_conditions if global_lesser < x < global_greater]
        segments = lesser + is_conds + greater
        
    comp_segments = calculate_complementary_keys(segments, keys_per_segment)
    return segments, comp_segments
    
def calculate_complementary_keys(segments, keys_per_segment):
    '''Calculates the complementary keys for a given set of segments.'''
    
    total_keys = set(range(200001))
    
    keys_in_segments = set()
    for start, end in segments:
        keys_in_segments.update(range(start, end + 1))
    complementary_keys = total_keys - keys_in_segments
    
    # Convert the set of complementary keys to a sorted list
    complementary_keys_list = sorted(list(complementary_keys))
    
    # Divide the complementary keys into segments
    complementary_segments = []
    for i in range(0, len(complementary_keys_list), keys_per_segment):
        start = complementary_keys_list[i]
        if i + keys_per_segment - 1 < len(complementary_keys_list):
            end = min(complementary_keys_list[i + keys_per_segment - 1], start + keys_per_segment - 1)
        else:
            end = complementary_keys_list[-1]
        complementary_segments.append((start, end))
    
    return complementary_segments
    
def format_segments_as_constraints(segments, general_constraints, key_complement):
    '''Formats a list of segments as a list of constraints.'''
    
    # handle keys first
    constraints = []
    for segment in segments:
        
        # Create a tuple of query rules for the start and end dates
        start_key, end_key = segment
        start_query_rule = query_rule("Event", "ID", "is greater than", str(start_key - 1))
        end_query_rule = query_rule("Event", "ID", "is less than", str(end_key + 1))
        key_tuple = (start_query_rule, end_query_rule)
        
        # Convert the tuple to a list
        extended_list = list(key_tuple)

        if not key_complement:
            # Extend the list with elements from general constraints
            extended_list.extend(general_constraints) 

        # Convert the extended list back to a tuple
        constraints.append(tuple(extended_list))
        
    if key_complement:
        general_constraints = [x for x in general_constraints if x.subfield != 'ID']
        for segment in key_complement:
                # Create a tuple of query rules for the start and end dates
                start_key, end_key = segment
                start_query_rule = query_rule("Event", "ID", "is greater than", str(start_key - 1))
                end_query_rule = query_rule("Event", "ID", "is less than", str(end_key + 1))
                key_tuple = (start_query_rule, end_query_rule)
                
                # Convert the tuple to a list
                extended_list = list(key_tuple)
    
                # Extend the list with elements from general constraints
                extended_list.extend(general_constraints) 
    
                # Convert the extended list back to a tuple
                constraints.append(tuple(extended_list))
    
    return constraints

def aggregate_csv_files(csv_files):
    '''Aggregates csv files from separte folders into a single CSV file.'''
    
    if not csv_files:
        print("No results returned.")
        return

    # Create an empty DataFrame to store aggregated data
    aggregated_df = pd.DataFrame()

    # Iterate through CSV files and append data to the aggregated DataFrame
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            aggregated_df = pd.concat([aggregated_df, df], ignore_index=True)
        except Exception as e:
            print(f"Error reading {csv_file}: {e}")

    # Specify the output aggregated CSV file path
    aggregated_csv_file = "./output/aggregated_data.csv"

    # Save the aggregated data to the CSV file
    aggregated_df.to_csv(aggregated_csv_file, index=False)

    print(f"\nAggregated data saved to {aggregated_csv_file}")
    print(f"Search Results: {aggregated_df.shape[0]}")
    
def submit_query(*args, **kwargs):
    '''A single query to the CAROL Database.
    The queries are input as a list of tuples or strings.
    '''

    # Query class
    q = CAROLQuery()

    # Sorts through the args
    for rule in args:

        # Add query rule from args
        q.addQueryRule(rule.field, rule.subfield, rule.condition, rule.value, kwargs['require_all'], kwargs['has_key_constraint'])

    # add "or" or "and" to values
    q._values.append(f"require_all = {kwargs['require_all']}")
    
    # Run the query
    if not kwargs['only_download']:
        q.query(download = kwargs['download'])
    else:
        q._result_list_count = 1

    # Download query
    if (kwargs['download']):
        if q._result_list_count > 0:
            q.download(kwargs['csv_files'], kwargs['number_complete'], kwargs['total_queries'])
        elif (q._result_list_count == 0):
            with complete_lock:
                kwargs['total_queries'].value -= 1
    
    # Return query object
    return q

def query(*args, download = False, require_all = True):
    '''A one-time query to the CAROL Database.
    The queries are input as a list of tuples or strings.
    '''
        
    start_time = time.time()
    
    key_constraints = []
    general_constraints = []

    # If no arguments, raise ValueError
    if len(args) == 0:
        raise ValueError("No queries found")

    # Sorts through the args
    for arg in args:
        field, subfield, condition, value = query_rule_sort(arg)
        
        # Check to make sure all query parameters were filled
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
        
        # create query_rule object
        rule = query_rule(field, subfield, condition, value)
        
        if subfield == "ID" and download == True:
            key_constraints.append(f'{condition} {value}')
            general_constraints.append(rule)
        else:
            general_constraints.append(rule)
        
    has_key_constraint = len(key_constraints) > 0
    gen_rule = tuple(general_constraints)
    if download == False:
        submit_query(*gen_rule, download = download, require_all = require_all, only_download = False, has_key_constraint = has_key_constraint)
    else:
        
        global_lower_bound_rule = None
        global_upper_bound_rule = None
        
        one_request = False
        print("Checking number of datapoints for request...\n")
        result_count = submit_query(*gen_rule, download = False, require_all = require_all, only_download = one_request, has_key_constraint = False)._result_list_count
        if 0 < result_count < 3500:
            print("Good news! We can download the data in one request.")
            one_request = True
        elif result_count == 0:
            print("No results found.")
            return
        else:
            print("Query too big for one reqeust. Dividing into segments and optimizing search\n")
            optimizing_result_count = 0
            if not has_key_constraint:
                # Initialize the search bounds
                lower_bound = 0
                upper_bound = 200000
                segment_size = 400

                # search for the lowest valid segment
                while upper_bound - lower_bound > segment_size:
                    middle = lower_bound + (upper_bound - lower_bound) // 2
                    print(f"Searching in the range ({lower_bound}, {middle})...\n")

                    # Create key constraints for the lower half of the search space
                    lower_bound_rule = query_rule("Event", "ID", "is greater than", str(lower_bound - 1))
                    upper_bound_rule = query_rule("Event", "ID", "is less than", str(middle))

                    # Check all rules in gen_rule
                    found = False
                    if require_all:
                        modified_gen_rule = gen_rule + (lower_bound_rule, upper_bound_rule)

                        # Resubmit the query with the updated search space
                        optimizing_result_count = submit_query(*modified_gen_rule, download=False, require_all=True, only_download=one_request, has_key_constraint=has_key_constraint)._result_list_count
                        
                        if optimizing_result_count > 0:
                            found = True
                    else:
                        found = False
                        for rule in gen_rule:
                            modified_gen_rule = (rule, ) + (lower_bound_rule, upper_bound_rule)

                            # Resubmit the query with the updated search space
                            optimizing_result_count = submit_query(*modified_gen_rule, download=False, require_all=True, only_download=one_request, has_key_constraint=has_key_constraint)._result_list_count
                            
                            if optimizing_result_count > 0:
                                found = True
                                break
                            
                    print(f'Found {optimizing_result_count} results in the range ({lower_bound}, {middle})')

                    if found:
                        # If a result is found in the lower half, continue searching in the lower half
                        upper_bound = middle
                    else:
                        # If no result is found in the lower half, search in the upper half
                        lower_bound = middle + 1

                # set global lower bound
                global_lower_bound_rule = query_rule("Event", "ID", "is greater than", str(lower_bound - 1))
                
                # reinit search bounds
                lower_bound = 0
                upper_bound = 200000
                segment_size = 400

                # search for the highest valid segmentq
                while upper_bound - lower_bound > segment_size:
                    middle = lower_bound + (upper_bound - lower_bound) // 2
                    print(f"Searching in the range ({middle}, {upper_bound})...\n")

                    # Create key constraints for the lower half of the search space
                    lower_bound_rule = query_rule("Event", "ID", "is greater than", str(middle - 1))
                    upper_bound_rule = query_rule("Event", "ID", "is less than", str(upper_bound))

                    # Check all rules in gen_rule
                    found = False
                    if require_all:
                        modified_gen_rule = gen_rule + (lower_bound_rule, upper_bound_rule)

                        # Resubmit the query with the updated search space
                        optimizing_result_count = submit_query(*modified_gen_rule, download=False, require_all=True, only_download=one_request, has_key_constraint=has_key_constraint)._result_list_count
                        
                        if optimizing_result_count > 0:
                            found = True
                    else:
                        found = False
                        for rule in gen_rule:
                            modified_gen_rule = (rule,) + (lower_bound_rule, upper_bound_rule)

                            # Resubmit the query with the updated search space
                            optimizing_result_count = submit_query(*modified_gen_rule, download=False, require_all=True, only_download=one_request, has_key_constraint=has_key_constraint)._result_list_count
                            
                            if optimizing_result_count > 0:
                                found = True
                                break

                    print(f'Found {optimizing_result_count} results in the range ({middle}, {upper_bound})')

                    if found:
                        # If a result is found in the upper half, continue searching in the upper half
                        lower_bound = middle
                    else:
                        # If no result is found in the lower half, search in the upper half
                        upper_bound = middle - 1

                # set global upper bound
                global_upper_bound_rule = query_rule("Event", "ID", "is less than", str(upper_bound))

        complement_flag = True
        if global_lower_bound_rule:
            # restrict based on optimization
            key_constraints.append(f'{global_lower_bound_rule.condition} {global_lower_bound_rule.value}')
            complement_flag = False
        if global_upper_bound_rule:
            # restrict based on optimization
            key_constraints.append(f'{global_upper_bound_rule.condition} {global_upper_bound_rule.value}')
            complement_flag = False
    
        # generate key segments correlating with key constraints
        key_segments = []
        key_complement = []
        key_segment_length = 400
        has_key_constraint = True
        if require_all:
            key_segments = generate_key_segments_and(key_segment_length, key_constraints)
        elif not key_constraints:
            key_segments, _ = generate_key_segments_or(key_segment_length, key_constraints)
        elif key_constraints:
            key_segments, key_complement = generate_key_segments_or(key_segment_length, key_constraints)

        # generate query segments
        query_segments = []
        if complement_flag:
            query_segments = format_segments_as_constraints(key_segments, general_constraints, key_complement)
        else:
            query_segments = format_segments_as_constraints(key_segments, general_constraints, [])
                    
        # Create a multiprocessing Pool with the desired number of processes
        num_processes = cpu_count()  # Use all available CPU cores
        ql = Lock() # query lock
        dl = Lock() # download lock
        fl = Lock() # file lock
        cl = Lock() # complete lock
        pool = Pool(initializer=init, initargs=(ql, dl, fl, cl), processes=num_processes)
        

        # Create a multiprocessing-safe list to store CSV file names generated by each process
        manager = Manager()
        csv_files = manager.list()
        number_complete = manager.Value('i', 0)
        total_queries = manager.Value('i', len(query_segments))
        
        # Use the map function to distribute the segments among processes
        with pool as p:
            if one_request:
                p.starmap(partial(submit_query, download = download, require_all = require_all, csv_files = csv_files, number_complete = number_complete, total_queries = manager.Value('i', 1), only_download = one_request, has_key_constraint = False), [gen_rule])
            else:
                p.starmap(partial(submit_query, download = download, require_all = require_all, csv_files = csv_files, number_complete = number_complete, total_queries = total_queries, only_download = one_request, has_key_constraint = has_key_constraint), query_segments)

        # Close the pool to free up resources
        pool.close()
        pool.join()  # Wait for all processes to finish
        
        # for segment in query_segments:
        #     print(segment[0])
        #     process_segment(segment)

        csv_files = list(csv_files)
        csv_files.sort(reverse=True)
        aggregate_csv_files(csv_files)
    
    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Execution time: {execution_time:.6f} seconds")

if __name__ == '__main__':
    
    # query(("Factual narrative", "does not contain"))
            
    # query(('HasSafetyRec', 'is', 'true'), download=False)

    # non key query with 2 conditions
    query(('Event', 'EventDate', 'is on or after', '9-23-2010'), ('Event', 'EventDate', 'is on or before', '10/23/2013'), download=True, require_all=True)
    
    # non key query with 3 conditions
    # query(('Aircraft', 'AircraftCategory', 'is', 'BLIM'), ('Aircraft', 'Damage', 'is', 'None'), ('Event', 'EventDate', 'is on or after', '9/23/2020'), require_all=False, download=True)
    
    # # key query with 2 conditions - one and one
    # query(('Event', 'ID', 'is greater than', '50000'), ('Aircraft', 'AircraftCategory', 'is', 'HELI'), require_all=True, download=True)
    
    # # key query with 3 conditions - two and one
    # query(('Event', 'ID', 'is greater than', '193455'), ('Event', 'ID', 'is less than', '3334'), ('Aircraft', 'AircraftCategory', 'is', 'BLIM'), require_all=True, download=True)
    
    # # key query with 3 conditions - three and one
    # query(('Event', 'ID', 'is greater than', '193455'), ('Event', 'ID', 'is less than', '3334'), ('Aircraft', 'AircraftCategory', 'is', 'BLIM'), require_all=False, download=True)
    
    
    # query(('Event', 'EventDate', 'is on or after', '10-23-20'), download=True, require_all=True)
    # query(('Aircraft', 'AircraftCategory', 'is', 'BLIM'), ('Aircraft', 'Damage', 'is', 'None'), require_all=False, download=True)
    # query(('Event', 'ID', 'is greater than', '193455'), ('Event', 'ID', 'is less than', '3334'), require_all=False, download=True)
    # query("is on or after 1/1/2023", "is before 1/1/1949", download=True, require_all=False)
    # query("fire", "engine power", download=True, require_all=False)