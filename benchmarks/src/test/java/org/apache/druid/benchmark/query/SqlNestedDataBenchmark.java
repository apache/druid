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

package org.apache.druid.benchmark.query;

import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlNestedDataBenchmark extends SqlBaseQueryBenchmark
{
  private static final List<String> QUERIES = ImmutableList.of(
      // ===========================
      // non-nested reference queries
      // ===========================
      // 0,1: timeseries, 1 columns
      "SELECT SUM(long1) FROM druid.nested",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested",
      // 2,3: timeseries, 2 columns
      "SELECT SUM(long1), SUM(long2) FROM druid.nested",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)), SUM(JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT)) FROM druid.nested",
      // 4,5: timeseries, 3 columns
      "SELECT SUM(long1), SUM(long2), SUM(double3) FROM druid.nested",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)), SUM(JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT)), SUM(JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE)) FROM druid.nested",
      // 6,7: group by string with 1 agg
      "SELECT string1, SUM(long1) FROM druid.nested GROUP BY 1 ORDER BY 2",
      "SELECT JSON_VALUE(nested, '$.nesteder.string1'), SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested GROUP BY 1 ORDER BY 2",
      // 8,9: group by string with 2 agg
      "SELECT string1, SUM(long1), SUM(double3) FROM druid.nested GROUP BY 1 ORDER BY 2",
      "SELECT JSON_VALUE(nested, '$.nesteder.string1'), SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)), SUM(JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE)) FROM druid.nested GROUP BY 1 ORDER BY 2",
      // 10,11: time-series filter string
      "SELECT SUM(long1) FROM druid.nested WHERE string1 = '10000' OR string1 = '1000'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.string1') = '10000' OR JSON_VALUE(nested, '$.nesteder.string1') = '1000'",
      // 12,13: time-series filter long
      "SELECT SUM(long1) FROM druid.nested WHERE long2 = 10000 OR long2 = 1000",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) = 10000 OR JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) = 1000",
      // 14,15: time-series filter double
      "SELECT SUM(long1) FROM druid.nested WHERE double3 < 10000.0 AND double3 > 1000.0",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) < 10000.0 AND JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) > 1000.0",
      // 16,17: group by long filter by string
      "SELECT long1, SUM(double3) FROM druid.nested WHERE string1 = '10000' OR string1 = '1000' GROUP BY 1 ORDER BY 2",
      "SELECT JSON_VALUE(nested, '$.long1' RETURNING BIGINT), SUM(JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.string1') = '10000' OR JSON_VALUE(nested, '$.nesteder.string1') = '1000' GROUP BY 1 ORDER BY 2",
      // 18,19: group by string filter by long
      "SELECT string1, SUM(double3) FROM druid.nested WHERE long2 < 10000 AND long2 > 1000 GROUP BY 1 ORDER BY 2",
      "SELECT JSON_VALUE(nested, '$.nesteder.string1'), SUM(JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) < 10000 AND JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) > 1000 GROUP BY 1 ORDER BY 2",
      // 20,21: group by string filter by double
      "SELECT string1, SUM(double3) FROM druid.nested WHERE double3 < 10000.0 AND double3 > 1000.0 GROUP BY 1 ORDER BY 2",
      "SELECT JSON_VALUE(nested, '$.nesteder.string1'), SUM(JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) < 10000.0 AND JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) > 1000.0 GROUP BY 1 ORDER BY 2",
      // 22, 23:
      "SELECT long2 FROM druid.nested WHERE long2 IN (1, 19, 21, 23, 25, 26, 46)",
      "SELECT JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) IN (1, 19, 21, 23, 25, 26, 46)",
      // 24, 25
      "SELECT long2 FROM druid.nested WHERE long2 IN (1, 19, 21, 23, 25, 26, 46) GROUP BY 1",
      "SELECT JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) IN (1, 19, 21, 23, 25, 26, 46) GROUP BY 1",
      // 26, 27
      "SELECT SUM(long1) FROM druid.nested WHERE double3 < 1005.0 AND double3 > 1000.0",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) < 1005.0 AND JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) > 1000.0",
      // 28, 29
      "SELECT SUM(long1) FROM druid.nested WHERE double3 < 2000.0 AND double3 > 1000.0",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) < 2000.0 AND JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) > 1000.0",
      // 30, 31
      "SELECT SUM(long1) FROM druid.nested WHERE double3 < 3000.0 AND double3 > 1000.0",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) < 3000.0 AND JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) > 1000.0",
      // 32,33
      "SELECT SUM(long1) FROM druid.nested WHERE double3 < 5000.0 AND double3 > 1000.0",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) < 5000.0 AND JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) > 1000.0",
      // 34,35 smaller cardinality like range filter
      "SELECT SUM(long1) FROM druid.nested WHERE string1 LIKE '1%'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.string1') LIKE '1%'",
      // 36,37 smaller cardinality like predicate filter
      "SELECT SUM(long1) FROM druid.nested WHERE string1 LIKE '%1%'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.string1') LIKE '%1%'",
      // 38-39 moderate cardinality like range
      "SELECT SUM(long1) FROM druid.nested WHERE string5 LIKE '1%'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.string5') LIKE '1%'",
      // 40, 41 big cardinality lex range
      "SELECT SUM(long1) FROM druid.nested WHERE string5 > '1'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.string5') > '1'",
      // 42, 43 big cardinality like predicate filter
      "SELECT SUM(long1) FROM druid.nested WHERE string5 LIKE '%1%'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.string5') LIKE '%1%'",
      // 44, 45 big cardinality like filter + selector filter with different ordering
      "SELECT SUM(long1) FROM druid.nested WHERE string5 LIKE '%1%' AND string1 = '1000'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.string5') LIKE '%1%' AND JSON_VALUE(nested, '$.nesteder.string1') = '1000'",
      "SELECT SUM(long1) FROM druid.nested WHERE string1 = '1000' AND string5 LIKE '%1%'",
      "SELECT SUM(JSON_VALUE(nested, '$.long1' RETURNING BIGINT)) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.string1') = '1000' AND JSON_VALUE(nested, '$.nesteder.string5') LIKE '%1%'",
      //48,49 bigger in
      "SELECT long2 FROM druid.nested WHERE long2 IN (1, 19, 21, 23, 25, 26, 46, 50, 51, 55, 60, 61, 66, 68, 69, 70, 77, 88, 90, 92, 93, 94, 95, 100, 101, 102, 104, 109, 111, 113, 114, 115, 120, 121, 122, 134, 135, 136, 140, 142, 150, 155, 170, 172, 173, 174, 180, 181, 190, 199, 200, 201, 202, 203, 204)",
      "SELECT JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) IN (1, 19, 21, 23, 25, 26, 46, 50, 51, 55, 60, 61, 66, 68, 69, 70, 77, 88, 90, 92, 93, 94, 95, 100, 101, 102, 104, 109, 111, 113, 114, 115, 120, 121, 122, 134, 135, 136, 140, 142, 150, 155, 170, 172, 173, 174, 180, 181, 190, 199, 200, 201, 202, 203, 204)",
      //50, 51 bigger in group
      "SELECT long2 FROM druid.nested WHERE long2 IN (1, 19, 21, 23, 25, 26, 46, 50, 51, 55, 60, 61, 66, 68, 69, 70, 77, 88, 90, 92, 93, 94, 95, 100, 101, 102, 104, 109, 111, 113, 114, 115, 120, 121, 122, 134, 135, 136, 140, 142, 150, 155, 170, 172, 173, 174, 180, 181, 190, 199, 200, 201, 202, 203, 204) GROUP BY 1",
      "SELECT JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) IN (1, 19, 21, 23, 25, 26, 46, 50, 51, 55, 60, 61, 66, 68, 69, 70, 77, 88, 90, 92, 93, 94, 95, 100, 101, 102, 104, 109, 111, 113, 114, 115, 120, 121, 122, 134, 135, 136, 140, 142, 150, 155, 170, 172, 173, 174, 180, 181, 190, 199, 200, 201, 202, 203, 204) GROUP BY 1",
      "SELECT long2 FROM druid.nested WHERE double3 IN (1.0, 19.0, 21.0, 23.0, 25.0, 26.0, 46.0, 50.0, 51.0, 55.0, 60.0, 61.0, 66.0, 68.0, 69.0, 70.0, 77.0, 88.0, 90.0, 92.0, 93.0, 94.0, 95.0, 100.0, 101.0, 102.0, 104.0, 109.0, 111.0, 113.0, 114.0, 115.0, 120.0, 121.0, 122.0, 134.0, 135.0, 136.0, 140.0, 142.0, 150.0, 155.0, 170.0, 172.0, 173.0, 174.0, 180.0, 181.0, 190.0, 199.0, 200.0, 201.0, 202.0, 203.0, 204.0)",
      "SELECT JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) IN (1.0, 19.0, 21.0, 23.0, 25.0, 26.0, 46.0, 50.0, 51.0, 55.0, 60.0, 61.0, 66.0, 68.0, 69.0, 70.0, 77.0, 88.0, 90.0, 92.0, 93.0, 94.0, 95.0, 100.0, 101.0, 102.0, 104.0, 109.0, 111.0, 113.0, 114.0, 115.0, 120.0, 121.0, 122.0, 134.0, 135.0, 136.0, 140.0, 142.0, 150.0, 155.0, 170.0, 172.0, 173.0, 174.0, 180.0, 181.0, 190.0, 199.0, 200.0, 201.0, 202.0, 203.0, 204.0)",
      "SELECT long2 FROM druid.nested WHERE double3 IN (1.0, 19.0, 21.0, 23.0, 25.0, 26.0, 46.0, 50.0, 51.0, 55.0, 60.0, 61.0, 66.0, 68.0, 69.0, 70.0, 77.0, 88.0, 90.0, 92.0, 93.0, 94.0, 95.0, 100.0, 101.0, 102.0, 104.0, 109.0, 111.0, 113.0, 114.0, 115.0, 120.0, 121.0, 122.0, 134.0, 135.0, 136.0, 140.0, 142.0, 150.0, 155.0, 170.0, 172.0, 173.0, 174.0, 180.0, 181.0, 190.0, 199.0, 200.0, 201.0, 202.0, 203.0, 204.0) GROUP BY 1",
      "SELECT JSON_VALUE(nested, '$.nesteder.long2' RETURNING BIGINT) FROM druid.nested WHERE JSON_VALUE(nested, '$.nesteder.double3' RETURNING DOUBLE) IN (1.0, 19.0, 21.0, 23.0, 25.0, 26.0, 46.0, 50.0, 51.0, 55.0, 60.0, 61.0, 66.0, 68.0, 69.0, 70.0, 77.0, 88.0, 90.0, 92.0, 93.0, 94.0, 95.0, 100.0, 101.0, 102.0, 104.0, 109.0, 111.0, 113.0, 114.0, 115.0, 120.0, 121.0, 122.0, 134.0, 135.0, 136.0, 140.0, 142.0, 150.0, 155.0, 170.0, 172.0, 173.0, 174.0, 180.0, 181.0, 190.0, 199.0, 200.0, 201.0, 202.0, 203.0, 204.0) GROUP BY 1"
  );


  @Param({
      "0",
      "1",
      "2",
      "3",
      "4",
      "5",
      "6",
      "7",
      "8",
      "9",
      "10",
      "11",
      "12",
      "13",
      "14",
      "15",
      "16",
      "17",
      "18",
      "19",
      "20",
      "21",
      "22",
      "23",
      "24",
      "25",
      "26",
      "27",
      "28",
      "29",
      "30",
      "31",
      "32",
      "33",
      "34",
      "35",
      "36",
      "37",
      "38",
      "39",
      "40",
      "41",
      "42",
      "43",
      "44",
      "45",
      "46",
      "47",
      "48",
      "49",
      "50",
      "51",
      "52",
      "53",
      "54",
      "55"
  })
  private String query;

  @Override
  public String getQuery()
  {
    return QUERIES.get(Integer.parseInt(query));
  }

  @Override
  public List<String> getDatasources()
  {
    return ImmutableList.of(SqlBenchmarkDatasets.NESTED);
  }
}
