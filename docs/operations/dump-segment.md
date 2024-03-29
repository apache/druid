---
id: dump-segment
title: "dump-segment tool"
---

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


The DumpSegment tool can be used to dump the metadata or contents of an Apache Druid segment for debugging purposes. Note that the
dump is not necessarily a full-fidelity translation of the segment. In particular, not all metadata is included, and
complex metric values may not be complete.

To run the tool, point it at a segment directory and provide a file for writing output:

```
java -classpath "/my/druid/lib/*" -Ddruid.extensions.loadList="[]" org.apache.druid.cli.Main \
  tools dump-segment \
  --directory /home/druid/path/to/segment/ \
  --out /home/druid/output.txt
```

If you use JDK 11 and above, you need to add the following additional parameters
```
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```
The following is an example

```
java --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  -classpath "/my/druid/lib/*" \
  -Ddruid.extensions.loadList="[]" org.apache.druid.cli.Main \
  tools dump-segment \
  --directory /home/druid/path/to/segment/ \
  --out /home/druid/output.txt
```

### Output format

#### Data dumps

By default, or with `--dump rows`, this tool dumps rows of the segment as newline-separate JSON objects, with one
object per line, using the default serialization for each column. Normally all columns are included, but if you like,
you can limit the dump to specific columns with `--column name`.

For example, one line might look like this when pretty-printed:

```
{
  "__time": 1442018818771,
  "added": 36,
  "channel": "#en.wikipedia",
  "cityName": null,
  "comment": "added project",
  "count": 1,
  "countryIsoCode": null,
  "countryName": null,
  "deleted": 0,
  "delta": 36,
  "isAnonymous": "false",
  "isMinor": "false",
  "isNew": "false",
  "isRobot": "false",
  "isUnpatrolled": "false",
  "iuser": "00001553",
  "metroCode": null,
  "namespace": "Talk",
  "page": "Talk:Oswald Tilghman",
  "regionIsoCode": null,
  "regionName": null,
  "user": "GELongstreet"
}
```

#### Metadata dumps

With `--dump metadata`, this tool dumps metadata instead of rows. Metadata dumps generated by this tool are in the same
format as returned by the [SegmentMetadata query](../querying/segmentmetadataquery.md).

#### Bitmap dumps

With `--dump bitmaps`, this tool will dump bitmap indexes instead of rows. Bitmap dumps generated by this tool include
dictionary-encoded string columns only. The output contains a field "bitmapSerdeFactory" describing the type of bitmaps
used in the segment, and a field "bitmaps" containing the bitmaps for each value of each column. These are base64
encoded by default, but you can also dump them as lists of row numbers with `--decompress-bitmaps`.

Normally all columns are included, but if you like, you can limit the dump to specific columns with `--column name`.

Sample output:

```
{
  "bitmapSerdeFactory": {
    "type": "roaring"
  },
  "bitmaps": {
    "isRobot": {
      "false": "//aExfu+Nv3X...",
      "true": "gAl7OoRByQ..."
    }
  }
}
```


#### Nested column dumps

With `--dump nested`, this tool can be used to examine Druid [nested columns](../querying/nested-columns.md). Using
`nested` always requires exactly one `--column name` argument, and takes an optional argument to specify a specific
nested field in [JSONPath syntax](../querying/sql-json-functions.md#jsonpath-syntax), `--nested-path $.path.to.field`.
If `--nested-path` is not specified, the output will contain the list of nested fields and their types, the global
value dictionaries, and the list of null rows.

Sample output:
```json
{
  "nest": {
    "fields": [
      {
        "path": "$.x",
        "types": [
          "LONG"
        ]
      },
      {
        "path": "$.y",
        "types": [
          "DOUBLE"
        ]
      },
      {
        "path": "$.z",
        "types": [
          "STRING"
        ]
      }
    ],
    "dictionaries": {
      "strings": [
        {
          "globalId": 0,
          "value": null
        },
        {
          "globalId": 1,
          "value": "a"
        },
        {
          "globalId": 2,
          "value": "b"
        }
      ],
      "longs": [
        {
          "globalId": 3,
          "value": 100
        },
        {
          "globalId": 4,
          "value": 200
        },
        {
          "globalId": 5,
          "value": 400
        }
      ],
      "doubles": [
        {
          "globalId": 6,
          "value": 1.1
        },
        {
          "globalId": 7,
          "value": 2.2
        },
        {
          "globalId": 8,
          "value": 3.3
        }
      ],
      "nullRows": []
    }
  }
}
```

If `--nested-path` is specified, the output will instead contain the types of the nested field, the local value
dictionary, including the 'global' dictionary id and value, the uncompressed bitmap index for each value (list of row
numbers which contain the value), and a dump of the column itself, which contains the row number, raw JSON form of the
nested column itself, the local dictionary id of the field for that row, and the value for the field for the row.

Sample output:
```json
{
  "bitmapSerdeFactory": {
    "type": "roaring"
  },
  "nest": {
    "$.x": {
      "types": [
        "LONG"
      ],
      "dictionary": [
        {
          "localId": 0,
          "globalId": 0,
          "value": null,
          "rows": [
            4
          ]
        },
        {
          "localId": 1,
          "globalId": 3,
          "value": "100",
          "rows": [
            3
          ]
        },
        {
          "localId": 2,
          "globalId": 4,
          "value": "200",
          "rows": [
            0,
            2
          ]
        },
        {
          "localId": 3,
          "globalId": 5,
          "value": "400",
          "rows": [
            1
          ]
        }
      ],
      "column": [
        {
          "row": 0,
          "raw": {
            "x": 200,
            "y": 2.2
          },
          "fieldId": 2,
          "fieldValue": "200"
        },
        {
          "row": 1,
          "raw": {
            "x": 400,
            "y": 1.1,
            "z": "a"
          },
          "fieldId": 3,
          "fieldValue": "400"
        },
        {
          "row": 2,
          "raw": {
            "x": 200,
            "z": "b"
          },
          "fieldId": 2,
          "fieldValue": "200"
        },
        {
          "row": 3,
          "raw": {
            "x": 100,
            "y": 1.1,
            "z": "a"
          },
          "fieldId": 1,
          "fieldValue": "100"
        },
        {
          "row": 4,
          "raw": {
            "y": 3.3,
            "z": "b"
          },
          "fieldId": 0,
          "fieldValue": null
        }
      ]
    }
  }
}
```

### Command line arguments

|argument|description|required?|
|--------|-----------|---------|
|--directory file|Directory containing segment data. This could be generated by unzipping an "index.zip" from deep storage.|yes|
|--output file|File to write to, or omit to write to stdout.|yes|
|--dump TYPE|Dump either 'rows' (default), 'metadata', 'bitmaps', or 'nested' for examining nested columns.|no|
|--column columnName|Column to include. Specify multiple times for multiple columns, or omit to include all columns.|no|
|--filter json|JSON-encoded [query filter](../querying/filters.md). Omit to include all rows. Only used if dumping rows.|no|
|--time-iso8601|Format __time column in ISO8601 format rather than long. Only used if dumping rows.|no|
|--decompress-bitmaps|Dump bitmaps as arrays rather than base64-encoded compressed bitmaps. Only used if dumping bitmaps.|no|
|--nested-path|Specify a specific nested column field using [JSONPath syntax](../querying/sql-json-functions.md#jsonpath-syntax). Only used if dumping a nested column.|no| 
