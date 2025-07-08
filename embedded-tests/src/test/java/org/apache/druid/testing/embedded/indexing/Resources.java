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
  /**
   * 10 rows (1 row per day) of inline CSV data with 3 columns (time, item, value).
   */
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

  public static final String JSON_DATA_2_ROWS =
      "{\"isRobot\":true,\"language\":\"en\",\"timestamp\":\"2013-08-31T00:00:11.080Z\","
      + "\"flags\":\"NB\",\"isUnpatrolled\":false,\"page\":\"Salo Toraut\","
      + "\"diffUrl\":\"https://sv.wikipedia.org/w/index.php?oldid=36099284&rcid=89369918\","
      + "\"added\":31,\"comment\":\"Botskapande Indonesien omdirigering\","
      + "\"commentLength\":35,\"isNew\":true,\"isMinor\":false,\"delta\":31,"
      + "\"isAnonymous\":false,\"user\":\"maytas1\",\"deltaBucket\":0.0,\"deleted\":0,"
      + "\"namespace\":\"Main\"}"
      + "\n{\"isRobot\":true,\"language\":\"en\",\"timestamp\":\"2013-08-31T00:00:11.080Z\","
      + "\"flags\":\"NB\",\"isUnpatrolled\":false,\"page\":\"Salo Toraut\","
      + "\"diffUrl\":\"https://sv.wikipedia.org/w/index.php?oldid=36099284&rcid=89369918\","
      + "\"added\":31,\"comment\":\"Botskapande Indonesien omdirigering\",\"commentLength\":35,"
      + "\"isNew\":true,\"isMinor\":false,\"delta\":11,\"isAnonymous\":false,\"user\":\"maytas2\","
      + "\"deltaBucket\":0.0,\"deleted\":0,\"namespace\":\"Main\"}\n";

  public static final String JSON_DATA_1_ROW =
      "{\"isRobot\":true,\"language\":\"en\",\"timestamp\":\"2013-08-31T00:00:11.080Z\","
      + "\"flags\":\"NB\",\"isUnpatrolled\":false,\"page\":\"Salo Toraut\","
      + "\"diffUrl\":\"https://sv.wikipedia.org/w/index.php?oldid=36099284&rcid=89369918\","
      + "\"added\":31,\"comment\":\"Botskapande Indonesien omdirigering\","
      + "\"commentLength\":35,\"isNew\":true,\"isMinor\":false,\"delta\":31,"
      + "\"isAnonymous\":false,\"user\":\"maytas3\",\"deltaBucket\":0.0,\"deleted\":0,\"namespace\":\"Main\"}\n";

  public static final String WIKIPEDIA_1_JSON = "data/json/wikipedia_1.json";
  public static final String WIKIPEDIA_2_JSON = "data/json/wikipedia_2.json";
  public static final String WIKIPEDIA_3_JSON = "data/json/wikipedia_3.json";
}
