<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


# How to use the Druid Query Driver

## Program description

The Druid Query Driver is a python script that simulates a Druid query workload.
You can use a JSON config file to describe the characteristics of the workload you want the Druid Query Driver to simulate.
The script uses this config file to generate SQL queries to the Druid API.

Here are the commands to set up the Python environment:

```
apt-get install python3
apt-get update
apt-get install -y python3-pip

pip install python-dateutil
pip install numpy
pip install requests
```

Run the program as follows:

```python DruidQueryDriver.py <options>```

Options include:

```
-f <configuration file name>
-o <TARGET, STDOUT or BOTH>
-n <total number of queries to generate>
-t <duration for generating records>
```

Use the _-f_ option to designate a configuration file name.
If you omit the _-f_ option, the script reads the configuration from _stdin_.

The _-o_ option tells the driver where to output queries (either to the target specified in the config file, stdout, or both - the default is TARGET).

The other two options control how long the script runs.
If neither option is present, the script will run indefinitely.
The _-t_ and _-n_ options are exclusive (use one or the other).
Time durations may be specified in terms of seconds, minutes or hours.
For example, specify 30 seconds as follows:

```
-t 30S
```

Specify 10 minutes as follows:

```
-t 10M
```

Or, specify 1 hour as follows:

```
-t 1H
```

## Config File

The config file contains JSON describing the query workload (see the _examples_ folder for example config files).

The config file has the following format:

```
{
  "queries": [...]
}
```

_queries_ is a list of _query_ objects.

### Query objects

```
{
  "target": <Druid query target URL>,
  "delay": <distribution description describing the delay>,
  "query": <SQL query pattern string>
  "variables": [ <zero or more variable objects>... ]
}
```

Each _query_ object has:
- _target_ which is a string indicating the Druid API where the query generator will send the queries (including host, port and path)
- _delay_ is a distribution describing the delay between queries (in seconds)
- _query_ is the SQL query pattern string that may include variables for substitution of stochastic values
- _variables_ is a list of variable definition objects


#### Target description

The target is a simple URL string including the host, the port and path.


For example:
```
"target": "localhost:8888/druid/v2/sql"
```

#### Delay description

The delay is a distribution description (see below) describing the number of seconds to delay between queries.

For example:
```
"delay": {"type": "exponential", "mean": 1}
```

#### Query pattern

The query pattern is a string that describes the basic shape of the SQL query.
The query pattern is SQL and may include variables, which the generator will dereference and replace with a stochastic value.

For example:
```
"query": "SELECT * FROM clickstream-data WHERE TIME_IN_INTERVAL(\"__time\", '$begin_time/$end_time')"
```

Where <i>$begin_time</i> and <i>$end_time</i> are variable references that the generator replaces with stochastic values when the generator creates the actual query string.
Note that in this example, we need to escape the double quotes with a backslash in the final query.


The previous query pattern might cause the generator to emit the following query:

```
{"query": "SELECT * FROM clickstream-data WHERE TIME_IN_INTERVAL(\"__time\", '2008-09-03T13:57:56.666/2022-12-13T15:17:48.060')"}
```

#### Variables

Variables are values within the _query_ pattern that the generator dereferences to obtain stochastic values.
The _query_ pattern designates variables with a _$_ prefix (e.g., $begin_time).
When the query generator creates a query string instance, the generator generates a stochastic value based on the named variable definition.
Note that the generator will use the same generated value for a specific variable name for the duration of the query string instance.

The form of variable objects vary based on the variable type as described in the following sections.

###### { "type": "enum" ...}

Enum variables specify the set of all possible values, as well as a distribution for selecting from the set.
Enums have the following format:

```
{
  "type": "enum",
  "name": "<variable name>",
  "values": [...],
  "cardinality_distribution": <distribution descriptor object>
}
```

Where:
- <i>name</i> is the name of the variable
- <i>values</i> is a list of the values
- <i>cardinality_distribution</i> informs the cardinality selection of the generated values


###### { "type": "string" ...}

String variable specification entries have the following format:

```
{
  "type": "string",
  "name": "<variable name>",
  "length_distribution": <distribution descriptor object>,
  "cardinality": <int value>,
  "cardinality_distribution": <distribution descriptor object>,
  "chars": "<list characters used to build strings>"
}
```

Where:
- <i>name</i> is the name of the variable
- <i>length_distribution</i> describes the length of the string values - Some distribution configurations may result in zero-length strings
- <i>cardinality</i> indicates the number of unique values for this variable (zero for unconstrained cardinality)
- <i>cardinality_distribution</i> informs the cardinality selection of the generated values (omit if cardinality is zero)
- <i>chars</i> (optional) is a list (e.g., "ABC123") of characters that may be used to generate strings - if not specified, all printable characters will be used

##### { "type": "int" ...}

Integer variable specification entries have the following format:

```
{
  "type": "int",
  "name": "<variable name>",
  "distribution": <distribution descriptor object>,
  "cardinality": <int value>,
  "cardinality_distribution": <distribution descriptor object>
}
```

Where:
- <i>name</i> is the name of the variable
- <i>distribution</i> describes the distribution of values the driver generates (rounded to the nearest int value)
- <i>cardinality</i> indicates the number of unique values for this variable (zero for unconstrained cardinality)
- <i>cardinality_distribution</i> skews the cardinality selection of the generated values

###### { "type": "float" ...}

Float variable specification entries have the following format:

```
{
  "type": "float",
  "name": "<variable name>",
  "distribution": <distribution descriptor object>,
  "cardinality": <int value>,
  "cardinality_distribution": <distribution descriptor object>,
  "percent_missing": <percentage value>
}
```

Where:
- <i>name</i> is the name of the variable
- <i>distribution</i> describes the distribution of float values the driver generates
- <i>cardinality</i> indicates the number of unique values for this variable (zero for unconstrained cardinality)
- <i>cardinality_distribution</i> skews the cardinality selection of the generated values
- <i>precision</i> (optional) the number digits after the decimal - if omitted all digits are included

###### { "type": "timestamp" ...}

Timestamp variable specification entries have the following format:

```
{
  "type": "timestamp",
  "name": "<variable name>",
  "distribution": <distribution descriptor object>,
  "cardinality": <int value>,
  "cardinality_distribution": <distribution descriptor object>}
```

Where:
- <i>name</i> is the name of the variable
- <i>distribution</i> describes the distribution of timestamp values the driver generates
- <i>cardinality</i> indicates the number of unique values for this variable (zero for unconstrained cardinality)
- <i>cardinality_distribution</i> skews the cardinality selection of the generated timestamps (optional - omit for unconstrained cardinality)


###### { "type": "ipaddress" ...}

IP address variable specification entries have the following format:

```
{
  "type": "ipaddress",
  "name": "<variable name>",
  "distribution": <distribution descriptor object>,
  "cardinality": <int value>,
  "cardinality_distribution": <distribution descriptor object>
}
```

Where:
- <i>name</i> is the name of the variable
- <i>distribution</i> describes the distribution of IP address values the driver generates
- <i>cardinality</i> indicates the number of unique values for this variable (zero for unconstrained cardinality)
- <i>cardinality_distribution</i> skews the cardinality selection of the generated timestamps (optional - omit for unconstrained cardinality)

Note that the data driver generates IP address values as <i>int</i>s according to the distribution, and then converts the int value to an IP address.

##### Cardinality

Many variable types may include a _Cardinality_ (the one exception is the _enum_ variable type).
Cardinality defines how many unique values the driver may generate.
Setting _cardinality_ to 0 provides no constraint to the number of unique values.
But, setting _cardinality_ > 0 causes the driver to create a list of values, and the length of the list is the value of _cardinality_.

When _cardinality_ is greater than 0, <i>cardinality_distribution</i> informs how the driver selects items from the cardinality list.
We can think of the cardinality list as a list with zero-based indexing, and the <i>cardinality_distribution</i> determines how the driver will select an index into the cardinality list.
After using the <i>cardinality_distribution</i> to produce an index, the driver constrains the index so as to be a valid value (i.e., 0 <= index < length of cardinality list).
Note that while _uniform_ and _normal_ distributions make sense to use as distribution specifications, _constant_ distributions only make sense if the cardinality list contains only a single value.
Further, for cardinality > 0, avoid an _exponential_ distribution as it will round down any values that are too large and produces a distorted distribution.

As an example of cardinality, imagine the following String variable definition:

```
{
  "type": "string",
  "name": "Str1",
  "length_distribution": {"type": "uniform", "min": 3, "max": 6},
  "cardinality": 5,
  "cardinality_distribution": {"type": "uniform", "min": 0, "max": 4},
  "chars": "abcdefg"
}
```

This defines a String variable named Str1 with five unique values that are three to six characters in length.
The driver will select from these five unique values uniformly (by selecting indices in the range of [0,4]).
All values may only consist of strings containing the letters a-g.

#### Distribution descriptor objects

Use _distribution descriptor objects_ to parameterize various characteristics of the config file (e.g., inter-arrival times, variable values, etc.) according to the config file syntax described in this document.

There are four types of _distribution descriptor objects_: Constant, Uniform, Exponential and Normal. Here are the formats of each of these types:

##### Constant distributions

The constant distribution generates the same single value.

```
{
  "type": "constant",
  "value": <value>
}
```

Where _value_ is the value generated by this distribution.

##### Uniform distribution

Uniform distribution generates values uniformly between _min_ and _max_ (inclusive).

```
{
  "type": "uniform",
  "min": <value>,
  "max": <value>
}
```

Where:
- _min_ is the minimum value sampled
- _max_ is the maximum value sampled

##### Exponential distribution

Exponenital distributions generate values following an exponential distribution around the mean.

```
{
  "type": "exponential",
  "mean": <value>
}
```

Where _mean_ is the resulting average value of the distribution.

##### Normal distribution

Normal distributions generate values with a normal (i.e., bell-shaped) distribution.

```
{
  "type": "normal",
  "mean": <value>,
  "stddev": <value>
}

```

Where:
- _mean_ is the average value
- _stddev_ is the stadard deviation of the distribution

Note that negative values generated by the normal distribution may be forced to zero when necessary (e.g., interarrival times).
