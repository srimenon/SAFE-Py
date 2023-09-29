<p align="center">
  <img src="SAFELogoBar.png"/>
</p>

# SAFEPy
This repository contains the SAFEPy Programmable Query Application for querying the National Transportation Safety Board (NTSB) investigations database.
This was developed by Gage Broberg, Wyatt McGinnis, Srihari Menon and Prof Nancy Currie-Gregg from the Systems Analysis & Functional Evaluation Laboratory (SAFELab) at Texas A&M University.
This tool is open-source and free-to-use. Please reference our publication when you do!

## How it works
SAFEPy works the same way that querying from the Carol Query web interface does, but is much more robust! When you submit a request to SAFEPy via the query() function, SAFEPy does the following:
1. Tries to adapt the request to the requirements of the NTSB servers
2. Based on the query parameters you give it, SAFEPy decides whether the query can be safely downloaded to your machine in one request, or if the query needs to be based to NTSB in smaller chunks.
3. Passes the revised query to NTSB asynchronously through http requests
4. Places received data from NTSB in a folder called './output' in the current directory
   1. Data chunks from queries that could not be completed in a single request in folders named with query parameters that generated them, so that you can always tell what data you have
5. After completing all necessary queries, combines all data received into the file './output/aggregated_data.csv'

## Implementation
The application is contained in a single python file `SAFEPy.py` and can be added using the standard python `import`. The module itself contains the `CAROLQuery` class, which is used by the application to interact with the CAROL database, along with the standard `query` function, which takes in a set of rules to query CAROL with.. 

## Multiprocessing and performance limitations
Queries resulting in 3500 accidents or more can take over 60 seconds to return, causing the http request to time out. To avoid this, SAFEPy breaks up your large queries into smaller ones that the NTSB servers can handle without error to ensure that you aren't left hanging with a 504 server timeout!

The application leverages multiprocessing to take advantage of the user's hardware. Because of this, performance is dependent on the number of CPUs on the user's machine. However, it is important to note that the speed of the application is limited by the speed and concurrency level of the NTSB servers. For large queries (yielding over 150000 accidents), please allow up to 1 hour for all data to be transferred. Queries yielding under 3500 datapoints will be completed in under 60 seconds. In general, completion time for queries is proportional to the number of resulting accident datapoints.

## Quick Start
Below is a small script demonstrating different queries that can be processed using the `query` function. For a full list of available queries, take a look at the [query_options](query_options.md) file.

```
import SAFEPy

# Different types of queries
q1 = ("engine power", "Narrative", "Factual", "contains")
q2 = ("Factual Narrative", "does not contain", "alcohol")
q3 = "01/01/2000"
q4 = ("fire",)

# A query can take any number of rules, as long as it is input as a string or tuple. 
# These tuples can be in any order, the query function will sort and structure it. 
SAFEPy.query(q1)
SAFEPy.query(q3, q4)
SAFEPy.query(q2, q1, q3)
```
## SAFEPy Library
The SAFEPy library contains the `CAROLQuery` class, the `query` function, and other helper functions used to sort input parameters into their respective fields.

### query()
The `query()` function is the main workhorse of the SAFEPy library. It takes an arbritrary number of different arguments and converts them into query rules, which it then uses to create a CAROLQuery object to probe the CAROL database. A single argument is formatted as either a string or a tuple of strings, which are sorted into rules using helper functions. For query fields that are missing from an argument, the application uses the existing elements along with a dictionary of known values to fill in the missing values. If the program cannot decide which fields fit the existing arguments, it raises an exception and halts the program. These arguments can contain key words and dates. For a full list of available queries, take a look at the [query_options](query_options.md) file.

### Date queries
Date queries can be submitted with just the date. This will search the database for records with a date `on or after` the entered date. Examples of some valid and invalid singular date queries are shown below:
```
# Valid date query
q = "September 27, 2023"
q = "27 Sep 2023"
q = "09/27/2023" (mm/dd/yyyy)
q = "27/09/2023" (dd/mm/yyyy)
q = "2023/09/27" (yyyy/mm/dd)
q = "9/27/23" (m/d/yy)
q = "27/9/23" (d/m/yy)
q = "2023/9/27" (yyyy/m/d)
q = "2023-09-27"
q = "2023.09.27"

# Invalid date query
q = "today"
q = datetime.today()
```

### 1 element queries
All 1 element queries will be considered valid. SAFEPy will first attempt to parse the query string as a date (e.g. as shown above). If unable to parse as a date, SAFEPy will search the Factual Narrative for the information. Examples of non-date 1 element queries are shown below:
```
q = "engine power"
q = ("fire",)
```

### 2 element queries
2 element queries will be considered invalid.
```
#Invalid 2-rule query
q1_2 = ("engine loss", "08/29/2005")
```

### 3 element queries
Each element in 3 element queries will be checked to see if it matches one of the valid inputs, and SAFEPy will attempt to sort it into a rule if possible. The following queries are examples of valid 3 element queries:
```
q = ("Factual narrative", "does not contain", "fuel exhaustion")
q = ("contains", "student", "analysis narrative")
```

### 4 element queries (___recommended___)
4 element queries provide the most robust querying in SAFEPy and are the recommended way to query. Elements can be placed into the query object in any order, and SAFEPy will sort the elements into the proper category for you. A list of all valid 4 element queries can be found in the [query_options](query_options.md) file.
```
q = ("03/31/1990", "EventDate", "is after", "Event")
q = ("false", "AmateurBuilt", "is", "Aircraft")
q = ('Event', 'ID', 'is less than', '3334')
```

### Combining queries
Each distinct query is represented as a single string or a tuple of strings, separated by commas. Combining two queries is done by having separate tuples or strings, not combining the queries into one tuple. Valid and invalid multi-rule queries are shown below. You can combine any number of queries.

```
#Valid 2-rule combined query
q1 = "engine loss"
q2 = ("12/10/2010", "EventDate", "is before", "Event")
query(q1, q2)

#Valid 5-rule combined query
q1 = "alcohol"
q2 = "08/29/2005"
q3 = ("contains", "student", "factual narrative")
q4 = ("12/10/2015", "EventDate", "is before", "Event")
q5 = ("false", "AmateurBuilt", "is", "Aircraft")
query(q1, q2, q3, q4, q5)
```

### Combining queries with `And` and `Or` and the `require_all` key word argument
By default in SAFEPy, queries like the above examples will be combined with `and` logic. This means that the query
```
q1 = "engine loss"
q2 = ("12/10/2010", "EventDate", "is before", "Event")
query(q1, q2)
```
is equivalent to searching for "engine loss" in the factual narrative `and` an event date that is before 12/10/2010. There is an optional key word argument for combined queries called `require_all`. If you want to search for "engine loss" in the factual narrative `or` an event date that is before 12/10/2010 you would do so as follows:
```
q1 = "engine loss"
q2 = ("12/10/2010", "EventDate", "is before", "Event")
query(q1, q2, require_all=False)
```

### Downloading data and the `download` key word argument
By default in SAFEPy, queried data is not downloaded. However, you can choose to download data on a query-by-query basis by setting the download key word argument to True. This would look like the following:
```
q1 = "engine loss"
q2 = ("12/10/2010", "EventDate", "is before", "Event")
query(q1, q2, download=True)
```
Segments of the requested data will be downloaded to ./output/['{query info here}'] until SAFEPy has finished downloading all data. Then, all data will be collected in the file ./output/aggregated_data.csv
