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

### Generate Delta Table for Unit Tests

To test Delta Lake ingestion, use the Python script `create_delta_table.py` to generate a sample Delta table.
Set up the required dependencies by installing those specified in `requirements.txt`.
```shell
pip install -r requirements.txt
```

Specifically, `pyspark` and `delta-spark` requirements are needed to generate Delta records. Once all the dependencies are installed,
you can run the python script:
```python
python3 create_delta_table.py
```

By default, the script uses `append` mode to generate 10 random records and writes the
Delta table to `resources/people-delta-table`. You can override the defaults by supplying the command line arguments:

```shell
python3 create_delta_table.py -h

usage: create_delta_table.py [-h] [--save_mode {append,overwrite}] [--save_path SAVE_PATH] [--num_records NUM_RECORDS]

Script to write a Delta Lake table.

optional arguments:
  -h, --help            show this help message and exit
  --save_mode {append,overwrite}
                        Specify write mode (append/overwrite) (default: append)
  --save_path SAVE_PATH
                        Save path for Delta table (default: <DRUID_BASE_PATH>/druid/extensions-contrib/druid-deltalake-extensions/src/test/resources/people-delta-table)
  --num_records NUM_RECORDS
                        Specify number of Delta records to write (default: 10)
```

Check in the Delta table generated and update `DeltaTestUtil.java` as needed for validation.
