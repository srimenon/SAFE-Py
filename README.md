<p align="center">
  <img src="SAFELogoBar.png"/>
</p>

# SAFEPy
This repository contains the SAFEPy Programmable Query Application for querying the National Transportation Safety Board investigations database.
This was developed by Gage Broberg, Wyatt McGinnis, Srihari Menon and Prof Nancy Currie-Gregg from the Systems Analysis & Functional Evaluation Laboratory (SAFELab) at Texas A&M University.
This tool is open-source and free-to-use. Please reference our publication when you do!

## Implementation
The application is contained in a single python file `SAFEPy.py` and can be added using the standard python `import`. The module itself contains the `CAROLQuery` class, which is used by the application to interact with the CAROL database, along with the standard `query` function, which takes in a set of rules to query CAROL with.

## Quick Start
Below is a small script demonstrating different queries that can be processed using the `query` function.

```
import SAFEPy

#Different types of queries
q1 = ("engine power", "Narrative", "Factual", "contains")
q2 = ("Factual Narrative", "does not contain", "alcohol")
q3 = "01/01/2000"
q4 = ("fire",)

#A query can take any number of rules, as long as it is input as a string or tuple. 
#These tuples can be in any order, the query function will sort and structure it. 
SAFEPy.query(q1)
SAFEPy.query(q3, q4)
SAFEPy.query(q2, q1, q3)
```
## SAFEPy Library
The SAFEPy library contains the `CAROLQuery` class, the `query` function, and other helper functions used to sort input parameters into their respective fields.

### query
The `query` function is the main workhorse of the SAFEPy library. It takes an arbritrary number of different arguments and converts them into query rules, which it then uses to create a CAROLQuery object to probe the CAROL database. A single argument is formatted as either a string or a tuple of strings, which are sorted into rules using helper functions. For query fields that are missing from an argument, the application uses the existing elements along with a dictionary of known values to fill in the missing values. If the program cannot decide which fields fit the existing arguments, it raises an exception and halts the program. These arguments can contain key words or dates in the format `MM-DD-YYYY, MM-DD-YY, MM/DD/YYYY, MM/DD/YY` as shown below.
```
#Valid date argument
q = "01/01/2000"
q = "02-20-12"

#Invalid date argument
q = "2000/01/01"
q = "05\23\1999"

#Valid 1 element arguments
q = "engine power"
q = ("fire",)

#Valid 3 element arguments
q = ("Factual narrative", "does not contain", "fuel exhaustion")
q = ("contains", "student", "analysis narrative")

#Valid 4 element arguments
q = ("03/31/1990", "EventDate", "is after", "Event")
q = ("false", "AmateurBuilt", "is", "Aircraft")
```
Each distinct argument is represented as a single string or a tuple, separated by commas. Combining two queries is done by having separate tuples or strings, not combining the queries into one tuple. Valid and invalid multi-rule queries are shown below.

```
#Valid 2-rule query
q1 = "engine loss"
q2 = ("12/10/2010", "EventDate", "is before", "Event")
query(q1, q2)

#Valid 5-rule query
q1 = "alcohol"
q2 = "08/29/2005"
q3 = ("contains", "student", "factual narrative")
q4 = ("12/10/2015", "EventDate", "is before", "Event")
q5 = ("false", "AmateurBuilt", "is", "Aircraft")
query(q1, q2, q3, q4, q5)

#Invalid 2-rule query
q1_2 = ("engine loss", "08/29/2005")
query(q1_2)

#Invalid 3-rule query
q1_2 = ("engine loss", "student")
q3 = "08/29/2005"
query(q1_2, q3)
```

### CAROLQuery
The `CAROLQuery` class is used to build a query from different structured rules and to use the rules to probe and download data from CAROL. CAROLQuery adds rules using the `addQueryRule(field, subfield, condition, value)` function. The `query` and `download` functions send different payloads to CAROL which query the database and download data, respectively. Below is a code snippet showing a basic query and download using the CAROLQuery class. 

```
import SAFEPy

q = SAFEPy.CAROLQuery()
q.addQueryRule("AviationOperation", "AirMedical", "is", "true")
q.query()
q.download()
```

### query_key_sort
`query_key_sort` is a helper function which takes in a value and return if it belongs in the field, subfield, condition, or value parameters used to query CAROL.

### query_rule_sort
`query_rule_sort` is a helper function which takes in a 1 to 4 element argument and converts it into a structured rule containing the 4 elements need by CAROL; field, subfield, condition and value.
