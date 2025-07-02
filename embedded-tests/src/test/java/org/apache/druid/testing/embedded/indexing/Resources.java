/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.testing.embedded.indexing;

/**
 * Constants and utility methods used in embedded cluster tests.
 */
public class Resources
{
  public static final String CSV_DATA_10_DAYS =
      "2025-06-01T00:00:00.000Z,shirt,105"
      + "\n2025-06-02T00:00:00.000Z,trousers,210"
      + "\n2025-06-03T00:00:00.000Z,jeans,150"
      + "\n2025-06-04T00:00:00.000Z,t-shirt,53"
      + "\n2025-06-05T00:00:00.000Z,microwave,1099"
      + "\n2025-06-06T00:00:00.000Z,spoon,11"
      + "\n2025-06-07T00:00:00.000Z,television,1100"
      + "\n2025-06-08T00:00:00.000Z,plant pots,75"
      + "\n2025-06-09T00:00:00.000Z,shirt,99"
      + "\n2025-06-10T00:00:00.000Z,toys,101";

  /**
   * Full task payload for an "index" task. The payload has the following format
   * arguments:
   * <ol>
   * <li>Data which can be provided as a single CSV string. The data is expected
   * to have 3 columns: "time", "item" and "value". (e.g {@link #CSV_DATA_10_DAYS})</li>
   * <li>Datasource name</li>
   * </ol>
   */
  public static final String INDEX_TASK_PAYLOAD_WITH_INLINE_DATA
      = "{"
        + "  \"type\": \"index\","
        + "  \"spec\": {"
        + "    \"ioConfig\": {"
        + "      \"type\": \"index\","
        + "      \"inputSource\": {"
        + "        \"type\": \"inline\","
        + "        \"data\": \"%s\""
        + "      },\n"
        + "      \"inputFormat\": {"
        + "        \"type\": \"csv\","
        + "        \"findColumnsFromHeader\": false,"
        + "        \"columns\": [\"time\",\"item\",\"value\"]"
        + "      }"
        + "    },"
        + "    \"tuningConfig\": {"
        + "      \"type\": \"index_parallel\","
        + "      \"partitionsSpec\": {"
        + "        \"type\": \"dynamic\","
        + "        \"maxRowsPerSegment\": 1000"
        + "      }"
        + "    },"
        + "    \"dataSchema\": {"
        + "      \"dataSource\": \"%s\","
        + "      \"timestampSpec\": {"
        + "        \"column\": \"time\","
        + "        \"format\": \"iso\""
        + "      },"
        + "      \"dimensionsSpec\": {"
        + "        \"dimensions\": []"
        + "      },"
        + "      \"granularitySpec\": {"
        + "        \"segmentGranularity\": \"DAY\""
        + "      }"
        + "    }"
        + "  }"
        + "}";
}
