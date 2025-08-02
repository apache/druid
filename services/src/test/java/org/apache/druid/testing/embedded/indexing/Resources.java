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

import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.http.client.utils.URIBuilder;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Constants and utility methods used in embedded cluster tests.
 */
public class Resources
{
  public static class InlineData
  {
    /**
     * 10 rows (1 row per day) of inline CSV data with 3 columns (time, item, value).
     */
    public static final String CSV_10_DAYS =
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

    public static final String JSON_2_ROWS =
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

    public static final String JSON_1_ROW =
        "{\"isRobot\":true,\"language\":\"en\",\"timestamp\":\"2013-08-31T00:00:11.080Z\","
        + "\"flags\":\"NB\",\"isUnpatrolled\":false,\"page\":\"Salo Toraut\","
        + "\"diffUrl\":\"https://sv.wikipedia.org/w/index.php?oldid=36099284&rcid=89369918\","
        + "\"added\":31,\"comment\":\"Botskapande Indonesien omdirigering\","
        + "\"commentLength\":35,\"isNew\":true,\"isMinor\":false,\"delta\":31,"
        + "\"isAnonymous\":false,\"user\":\"maytas3\",\"deltaBucket\":0.0,\"deleted\":0,\"namespace\":\"Main\"}\n";
  }

  public static class DataFile
  {
    public static final String TINY_WIKI_1_JSON = "data/json/tiny_wiki_1.json";
    public static final String TINY_WIKI_2_JSON = "data/json/tiny_wiki_2.json";
    public static final String TINY_WIKI_3_JSON = "data/json/tiny_wiki_3.json";
  }

  public static class HttpData
  {
    public static HttpInputSource wikipedia1Day()
    {
      try {
        return new HttpInputSource(
            List.of(new URIBuilder("https://druid.apache.org/data/wikipedia.json.gz").build()),
            null,
            null,
            null,
            null,
            new HttpInputSourceConfig(null, null)
        );
      }
      catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Contains queries (that can be run with {@code cluster.runSql(...)}) and
   * their corresponding results.
   */
  public static class Query
  {
    // Queries used with Task.BASIC_INDEX
    public static final String SELECT_MIN_MAX_TIME = "SELECT MIN(__time), MAX(__time) FROM %s";
    public static final String SELECT_APPROX_COUNT_DISTINCT =
        "SELECT"
        + " APPROX_COUNT_DISTINCT_DS_THETA(\"thetaSketch\"),"
        + " APPROX_COUNT_DISTINCT_DS_HLL(\"HLLSketchBuild\")"
        + " FROM %s";
    public static final String SELECT_EARLIEST_LATEST_USER =
        "SELECT EARLIEST(\"user\"), LATEST(\"user\") FROM %s WHERE __time < '2013-09-01'";
    public static final String SELECT_COUNT_OF_CHINESE_PAGES =
        "SELECT \"page\", COUNT(*) AS \"rows\", SUM(\"added\"), 10 * SUM(\"added\") AS added_times_ten"
        + " FROM %s"
        + " WHERE \"language\" = 'zh' AND __time < '2013-09-01'"
        + " GROUP BY 1"
        + " HAVING added_times_ten > 9000";
  }
}
