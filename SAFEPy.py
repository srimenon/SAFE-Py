import requests
import zipfile
import os
import json
import re
from datetime import datetime, timedelta
import calendar
import time
from multiprocessing import Pool, Lock, Semaphore, cpu_count

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

# multiprocessing lock init
def init(l,s):
    global lock
    global sem
    lock = l
    sem = s

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
            "AndOr": "and",
            "TargetCollection": "cases",
            "ExportFormat": "summary",
            "SessionId": 100000,
            "ResultSetSize": 50,
            "SortDescending": True
        }
        
        self.result_list_count = None
        self.max_result_count_reached = None
        
    def __del__(self):
        self._session.close()

    def addQueryRule(self, field, subfield, condition, values, andOr):
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
            
        self._values = values

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
        
        if andOr:
            self._probe["QueryGroups"][0]["AndOr"] = "and"
            self._payload["QueryGroups"][0]["AndOr"] = "and"
        else:
            self._probe["QueryGroups"][0]["AndOr"] = "or"
            self._payload["QueryGroups"][0]["AndOr"] = "or"
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
            with sem:
                # avoids erroring concurrent api requests
                time.sleep(0.3)
            # print query parameters currently working on
            print("Querying CAROL...")
            response = self._session.post(probe_url, json=self._probe, timeout=60)
                
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
            self.result_list_count = response_json['ResultListCount']
            self.max_result_count_reached = response_json['MaxResultCountReached']

            print(f'Result count: {self.result_list_count}')
            print(f'Max reached: {self.max_result_count_reached}\n')

    def download(self):
        """Sends a download probe to the CAROL database.
        """

        # Send the file POST request
        response = None
        try:
            # print dots to signify working
            print(f"Downloading data from CAROL...")
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
            with lock:
                # Create the output directory if it doesn't exist
                os.makedirs('./output', exist_ok=True)

                # write zip to file
                with open(f'./output/{folder}.zip', 'wb') as f:
                    f.write(response.content)

                # create a ZipFile object
                with zipfile.ZipFile(f'./output/{folder}.zip', 'r') as zip_ref:
                    # extract all the contents of the zip file to the current directory
                    zip_ref.extractall(f'./output/{self._values}')

                # remove the zip file
                os.remove(f'./output/{folder}.zip')
            
def to_standard_date_format(date_str):
    input_formats = [
        '%m/%d/%y', '%m-%d-%y', '%d/%m/%y', '%d-%m-%y',
        '%m/%d/%Y', '%m-%d-%Y', '%d/%m/%Y', '%d-%m-%Y',
    ]
    output_format = '%Y-%m-%d'
    
    for format_str in input_formats:
        try:
            date_obj = datetime.strptime(date_str, format_str)
            return date_obj.strftime(output_format)
        except ValueError:
            pass
    
    return None

def query_decide(value: str):
    """Using pattern matching, decides which field, subfield, and condition an arbitrary value falls under.
    """
    cond_date_regex = r'(.+?)? ?(\d{1,2}[\/|-]\d{1,2}[\/|-]\d{2,4})'
    
    match = re.match(cond_date_regex, value)
    if match.group(1):
        return "Event", "EventDate", match.group(1), to_standard_date_format(match.group(2))
    elif match.group(2):
        return "Event", "EventDate", "is after", to_standard_date_format(match.group(2))

    return "Narrative", "Factual", "contains", value

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
        rule[0:4] = query_decide(arg)

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

def generate_time_periods_and(constraints):
    # initialize time period to maximum interval
    start_date = datetime(1948, 10, 24)
    time_periods = [[start_date, datetime.today()]]

    # process all constraints
    for constraint in constraints:
        
        # extract condition date
        parts = constraint.split()
        condition_date = datetime.strptime(parts[-1], '%m/%d/%Y')
        
        # check condition date bounds
        if condition_date < start_date or datetime.today() < condition_date:
            raise ValueError(f"Date {parts[-1]} should be between 10/24/1948 and today.")

        # compare condition date to all current periods
        for period in time_periods:
            if 'before' in constraint:
                if 'on' in constraint: # is on or before
                    if period[0] <= condition_date < period[1]: # if condition within period
                        period[1] = condition_date
                    elif condition_date < period[0]: # if condition before period
                        time_periods.remove(period)
                else: # is before
                    if period[0] < condition_date <= period[1]: # if condition within period
                        period[1] = condition_date - timedelta(days=1)
                    elif condition_date <= period[0]: # if condition before period
                        time_periods.remove(period)
            elif 'after' in constraint:
                if 'on' in constraint: # is on or after
                    if period[0] < condition_date <= period[1]: # if condition within period
                        period[0] = condition_date
                    elif period[1] < condition_date: # if condition after period
                        time_periods.remove(period)
                else: # is after
                    if period[0] <= condition_date < period[1]: # if condition within period
                        period[0] = condition_date + timedelta(days=1)
                    elif period[1] <= condition_date: # if condition after period
                        time_periods.remove(period)
            elif 'not' in constraint and period[0] <= condition_date <= period[1]: # is not
                time_periods.append([period[0], condition_date - timedelta(days=1)])
                time_periods.append([condition_date + timedelta(days=1), period[1]])
                time_periods.remove(period)
                break
            elif 'is' in constraint and period[0] <= condition_date <= period[1]: # is
                period[0] = period[1] = condition_date
                break
            else: # is but not within current period
                time_periods.remove(period)

    return time_periods

def generate_time_periods_or(constraints):
    # initialize time period to maximum interval
    start_date = datetime(1948, 10, 24)
    time_periods = []
    
    # init with dates just outside acceptable range
    first_date_before_range = datetime(1948, 10, 23)
    latest_before = first_date_before_range
    tomorrow = datetime.today() + timedelta(days=1)
    earliest_after = tomorrow
    is_conditions = []
    not_cond = None

    # process all constraints
    for constraint in constraints:
        
        # extract condition date
        parts = constraint.split()
        condition_date = datetime.strptime(parts[-1], '%m/%d/%Y')
        
        # check condition date bounds
        if condition_date < start_date or datetime.today() < condition_date:
            raise ValueError(f"Date {parts[-1]} should be between 10/24/1948 and today.")
        
        if 'before' in constraint:
            if 'on' in constraint and (latest_before == first_date_before_range or latest_before < condition_date):
                latest_before = condition_date
            elif latest_before == first_date_before_range or latest_before < condition_date - timedelta(days=1): # is before
                latest_before = condition_date - timedelta(days=1)
        elif 'after' in constraint:
            if 'on' in constraint and (earliest_after == tomorrow or condition_date < earliest_after): # is on or after
                earliest_after = condition_date
            elif earliest_after == tomorrow or condition_date + timedelta(days = 1) < earliest_after: # is after
                earliest_after = condition_date + timedelta(days = 1)
        elif 'not' in constraint: # is not
            if not_cond and not_cond is not condition_date: # non-matching not conditions
                return [[start_date, datetime.today()]]
            elif condition_date in is_conditions: # matching not condition and is condition
                return [[start_date, datetime.today()]]
            elif latest_before > condition_date or earliest_after < condition_date: # not outside the bounds of existing cond
                return [[start_date, datetime.today()]]
            else: # our time period is everything except not condition
                time_periods = [[start_date, condition_date - timedelta(days=1)], [condition_date + timedelta(days=1), datetime.today()]]
                not_cond = condition_date
        elif 'is' in constraint:
            if condition_date == not_cond:
                return [[start_date, datetime.today()]]
            is_conditions.append(condition_date)
        else: # is but not within current period
            raise ValueError(f"Constraint is not formatted properly. It must include: 'is', 'is not', 'is on or before', 'is before', 'is on or after', or 'is after'.")
        
    # check not condition
    if not_cond:
        if latest_before > not_cond or earliest_after < not_cond:
            return [[start_date, datetime.today()]]
        else:
            return sorted(time_periods)
            
    # check latest_before and earliest_after
    if latest_before >= earliest_after: # if we have before and after conditions covering whole range
        time_periods = [[start_date, datetime.today()]]
    else: # check the is conditions to add dates
        time_periods = [[start_date, latest_before], [earliest_after, datetime.today()]]
        # include is conditions
        for is_cond in is_conditions:
            if latest_before < is_cond < earliest_after:
                time_periods.append([is_cond, is_cond])

    return sorted(time_periods)

def divide_into_year_segments(time_periods):
    year_segments = []

    for period in time_periods:
        start_date, end_date = period

        period_year = start_date.year
        while period_year <= end_date.year:
            next_year_start = datetime(period_year + 1, 1, 1)
            if next_year_start > end_date:
                year_segments.append((start_date, end_date))
            else:
                year_segments.append((start_date, next_year_start - timedelta(days=1)))
            period_year += 1
            start_date = next_year_start

    return year_segments

def divide_into_half_year_segments(time_periods):
    half_year_segments = []

    for period in time_periods:
        start_date, end_date = period

        while start_date <= end_date:
            next_half_year_start = start_date + timedelta(days=183)
            
            if next_half_year_start > end_date:
                half_year_segments.append((start_date, end_date))
            else:
                half_year_segments.append((start_date, next_half_year_start - timedelta(days=1)))
                
            start_date = next_half_year_start

    return half_year_segments

def divide_into_quarter_year_segments(time_periods):
    quarter_year_segments = []

    for period in time_periods:
        start_date, end_date = period

        while start_date <= end_date:
            next_quarter_start = start_date + timedelta(days=91)
            
            if next_quarter_start > end_date:
                quarter_year_segments.append((start_date, end_date))
            else:
                quarter_year_segments.append((start_date, next_quarter_start - timedelta(days=1)))
                
            start_date = next_quarter_start

    return quarter_year_segments

def divide_into_month_segments(time_periods):
    month_segments = []

    for period in time_periods:
        start_date, end_date = period

        while start_date <= end_date:
            _, last_day = calendar.monthrange(start_date.year, start_date.month)
            next_month_start = datetime(start_date.year, start_date.month, last_day) + timedelta(days=1)
            
            if next_month_start > end_date:
                month_segments.append((start_date, end_date))
            else:
                month_segments.append((start_date, next_month_start - timedelta(days=1)))
                
            start_date = next_month_start

    return month_segments

def format_segments_as_constraints(segments):
    constraints = []

    for segment in segments:
        start_date, end_date = segment
        constraints.append((f"is on or after {start_date.strftime('%m/%d/%Y')}", f"is on or before {end_date.strftime('%m/%d/%Y')}"))

    return constraints

def process_segment(segment):
    query(segment[0], segment[1], download=True, requireAll=True)

def query(*args, download = False, requireAll = True):
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
        
        #Check to make sure all query parameters were filled
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
        q.addQueryRule(field, subfield, condition, value, requireAll)

    #Run the query
    q.query()

    #Download query
    if (download and q.result_list_count > 0):
        q.download()
    
    #Return query object
    return q

if __name__ == '__main__':
    # Sample random queries
        # query("engine power", datetime.today() - timedelta(days=1), datetime.today())
        # query('How many airplanes crash because of airplane failure'
        
    # query(("engine power", "Narrative", "Factual", "contains"))
    # query("1/1/13")
    # query("is on or after 1/1/13", "is before 1/1/14", download=True, requireAll=True)
    # query(("Analysis Narrative", "does not contain", "alcohol"))
    # query(("fire", "after 1/1/13", "before 1/1/14"))
    constraints = [
    # 'is on or after 10/24/1948',
    'is on or after 1/1/2020',
    'is before 3/1/2022',
    # 'is before 6/3/2017',
    # 'is 6/6/2013',
    # 'is not 6/6/2020',
    # 'is not 1/1/1995',
    # 'is 10/24/1950',
    # 'is after 3/14/2013',
    # 'is on or before 4/12/2013'
    ]
        
    # time_periods = generate_time_periods_and(constraints)
    time_periods = generate_time_periods_and(constraints)

    for period in time_periods:
        period_start, period_end = period
        print(f"{period_start.strftime('%m/%d/%Y')} - {period_end.strftime('%m/%d/%Y')}")
        
    segments = divide_into_month_segments(time_periods)

    print()
    for period in segments:
        period_start, period_end = period
        print(f"{period_start.strftime('%m/%d/%Y')} - {period_end.strftime('%m/%d/%Y')}")
        
    query_segments = format_segments_as_constraints(segments)
    
    # Max number of concurrent processes for probe query
    max_concurrent_processes = 1
            
    # Create a multiprocessing Pool with the desired number of processes
    num_processes = cpu_count()  # Use all available CPU cores
    l = Lock()
    s = Semaphore(max_concurrent_processes)
    pool = Pool(initializer=init, initargs=(l,s), processes=num_processes)
    
    start_time = time.time()
    
    # Use the map function to distribute the segments among processes
    pool.map(process_segment, query_segments)

    # Close the pool to free up resources
    pool.close()
    pool.join()  # Wait for all processes to finish
    
    # for segment in query_segments:
    #     print(segment[0])
    #     process_segment(segment)
    
    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Execution time: {execution_time:.6f} seconds")
