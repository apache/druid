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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.Map;

/**
 * Benchmark that tests various SQL queries.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlExpressionBenchmark extends SqlBaseQueryBenchmark
{
  private static final List<String> QUERIES = ImmutableList.of(
      // ===========================
      // non-expression reference queries
      // ===========================
      // 0: non-expression timeseries reference, 1 columns
      "SELECT SUM(long1) FROM expressions",
      // 1: non-expression timeseries reference, 2 columns
      "SELECT SUM(long1), SUM(long2) FROM expressions",
      // 2: non-expression timeseries reference, 3 columns
      "SELECT SUM(long1), SUM(long4), SUM(double1) FROM expressions",
      // 3: non-expression timeseries reference, 4 columns
      "SELECT SUM(long1), SUM(long4), SUM(double1), SUM(float3) FROM expressions",
      // 4: non-expression timeseries reference, 5 columns
      "SELECT SUM(long1), SUM(long4), SUM(double1), SUM(float3), SUM(long5) FROM expressions",
      // 5: group by non-expr with 1 agg
      "SELECT string2, SUM(long1) FROM expressions GROUP BY 1 ORDER BY 2",
      // 6: group by non-expr with 2 agg
      "SELECT string2, SUM(long1), SUM(double3) FROM expressions GROUP BY 1 ORDER BY 2",
      // ===========================
      // expressions
      // ===========================
      // 7: math op - 2 longs
      "SELECT SUM(long1 * long2) FROM expressions",
      // 8: mixed math - 2 longs, 1 double
      "SELECT SUM((long1 * long2) / double1) FROM expressions",
      // 9: mixed math - 2 longs, 1 double, 1 float
      "SELECT SUM(float3 + ((long1 * long4)/double1)) FROM expressions",
      // 10: mixed math - 3 longs, 1 double, 1 float
      "SELECT SUM(long5 - (float3 + ((long1 * long4)/double1))) FROM expressions",
      // 11: all same math op - 3 longs, 1 double, 1 float
      "SELECT SUM(long5 * float3 * long1 * long4 * double1) FROM expressions",
      // 12: cos
      "SELECT cos(double2) FROM expressions",
      // 13: unary negate
      "SELECT SUM(-long4) FROM expressions",
      // 14: string long
      "SELECT SUM(PARSE_LONG(string1)) FROM expressions",
      // 15: string longer
      "SELECT SUM(PARSE_LONG(string3)) FROM expressions",
      // 16: time floor, non-expr col + reg agg
      "SELECT TIME_FLOOR(__time, 'PT1H'), string2, SUM(double4) FROM expressions GROUP BY 1,2 ORDER BY 3",
      // 17: time floor, non-expr col + expr agg
      "SELECT TIME_FLOOR(__time, 'PT1H'), string2, SUM(long1 * double4) FROM expressions GROUP BY 1,2 ORDER BY 3",
      // 18: time floor + non-expr agg (timeseries) (non-expression reference)
      "SELECT TIME_FLOOR(__time, 'PT1H'), SUM(long1) FROM expressions GROUP BY 1 ORDER BY 1",
      // 19: time floor + expr agg (timeseries)
      "SELECT TIME_FLOOR(__time, 'PT1H'), SUM(long1 * long4) FROM expressions GROUP BY 1 ORDER BY 1",
      // 20: time floor + non-expr agg (group by)
      "SELECT TIME_FLOOR(__time, 'PT1H'), SUM(long1) FROM expressions GROUP BY 1 ORDER BY 2",
      // 21: time floor + expr agg (group by)
      "SELECT TIME_FLOOR(__time, 'PT1H'), SUM(long1 * long4) FROM expressions GROUP BY 1 ORDER BY 2",
      // 22: time floor offset by 1 day + non-expr agg (group by)
      "SELECT TIME_FLOOR(TIMESTAMPADD(DAY, -1, __time), 'PT1H'), SUM(long1) FROM expressions GROUP BY 1 ORDER BY 1",
      // 23: time floor offset by 1 day + expr agg (group by)
      "SELECT TIME_FLOOR(TIMESTAMPADD(DAY, -1, __time), 'PT1H'), SUM(long1 * long4) FROM expressions GROUP BY 1 ORDER BY 1",
      // 24: group by long expr with non-expr agg
      "SELECT (long1 * long2), SUM(double1) FROM expressions GROUP BY 1 ORDER BY 2",
      // 25: group by non-expr with expr agg
      "SELECT string2, SUM(long1 * long4) FROM expressions GROUP BY 1 ORDER BY 2",
      // 26: group by string expr with non-expr agg
      "SELECT CONCAT(string2, '-', long2), SUM(double1) FROM expressions GROUP BY 1 ORDER BY 2",
      // 27: group by string expr with expr agg
      "SELECT CONCAT(string2, '-', long2), SUM(long1 * double4) FROM expressions GROUP BY 1 ORDER BY 2",
      // 28: group by single input string low cardinality expr with expr agg
      "SELECT CONCAT(string2, '-', 'expressions'), SUM(long1 * long4) FROM expressions GROUP BY 1 ORDER BY 2",
      // 29: group by single input string high cardinality expr with expr agg
      "SELECT CONCAT(string3, '-', 'expressions'), SUM(long1 * long4) FROM expressions GROUP BY 1 ORDER BY 2",
      // 30: logical and operator
      "SELECT CAST(long1 as BOOLEAN) AND CAST (long2 as BOOLEAN), COUNT(*) FROM expressions GROUP BY 1 ORDER BY 2",
      // 31: isnull, notnull
      "SELECT long5 IS NULL, long3 IS NOT NULL, count(*) FROM expressions GROUP BY 1,2 ORDER BY 3",
      // 32: time shift, non-expr col + reg agg, regular
      "SELECT TIME_SHIFT(__time, 'PT1H', 3), string2, SUM(double4) FROM expressions GROUP BY 1,2 ORDER BY 3",
      // 33: time shift, non-expr col + expr agg, sequential low cardinality
      "SELECT TIME_SHIFT(MILLIS_TO_TIMESTAMP(long1), 'PT1H', 1), string2, SUM(long1 * double4) FROM expressions GROUP BY 1,2 ORDER BY 3",
      // 34: time shift + non-expr agg (timeseries) (non-expression reference), zipf distribution low cardinality
      "SELECT TIME_SHIFT(MILLIS_TO_TIMESTAMP(long2), 'PT1H', 1), string2, SUM(long1 * double4) FROM expressions GROUP BY 1,2 ORDER BY 3",
      // 35: time shift + expr agg (timeseries), zipf distribution high cardinality
      "SELECT TIME_SHIFT(MILLIS_TO_TIMESTAMP(long3), 'PT1H', 1), string2, SUM(long1 * double4) FROM expressions GROUP BY 1,2 ORDER BY 3",
      // 36: time shift + non-expr agg (group by), uniform distribution low cardinality
      "SELECT TIME_SHIFT(MILLIS_TO_TIMESTAMP(long4), 'PT1H', 1), string2, SUM(long1 * double4) FROM expressions GROUP BY 1,2 ORDER BY 3",
      // 37: time shift + expr agg (group by), uniform distribution high cardinality
      "SELECT TIME_SHIFT(MILLIS_TO_TIMESTAMP(long5), 'PT1H', 1), string2, SUM(long1 * double4) FROM expressions GROUP BY 1,2 ORDER BY 3",
      // 38,39: array element filtering
      "SELECT string1, long1 FROM expressions WHERE ARRAY_CONTAINS(\"multi-string3\", 100) GROUP BY 1,2",
      "SELECT string1, long1 FROM expressions WHERE ARRAY_OVERLAP(\"multi-string3\", ARRAY[100, 200]) GROUP BY 1,2",
      // 40: regex filtering
      "SELECT string4, COUNT(*) FROM expressions WHERE REGEXP_EXTRACT(string1, '^1') IS NOT NULL OR REGEXP_EXTRACT('Z' || string2, '^Z2') IS NOT NULL GROUP BY 1",
      // 41: complicated filtering
      "SELECT string2, SUM(long1) FROM expressions WHERE string1 = '1000' AND string5 LIKE '%1%' AND (string3 in ('1', '10', '20', '22', '32') AND long2 IN (1, 19, 21, 23, 25, 26, 46) AND double3 < 1010.0 AND double3 > 1000.0 AND (string4 = '1' OR REGEXP_EXTRACT(string1, '^1') IS NOT NULL OR REGEXP_EXTRACT('Z' || string2, '^Z2') IS NOT NULL)) GROUP BY 1 ORDER BY 2",
      // 42: array_contains expr
      "SELECT ARRAY_CONTAINS(\"multi-string3\", 100) FROM expressions",
      "SELECT ARRAY_CONTAINS(\"multi-string3\", ARRAY[1, 2, 10, 11, 20, 22, 30, 33, 40, 44, 50, 55, 100]) FROM expressions",
      "SELECT ARRAY_OVERLAP(\"multi-string3\", ARRAY[1, 100]) FROM expressions",
      "SELECT ARRAY_OVERLAP(\"multi-string3\", ARRAY[1, 2, 10, 11, 20, 22, 30, 33, 40, 44, 50, 55, 100]) FROM expressions",
      // 46: filters with random orders
      "SELECT string2, SUM(long1) FROM expressions WHERE string5 LIKE '%1%' AND string1 = '1000' GROUP BY 1 ORDER BY 2",
      "SELECT string2, SUM(long1) FROM expressions WHERE string5 LIKE '%1%' AND (string3 in ('1', '10', '20', '22', '32') AND long2 IN (1, 19, 21, 23, 25, 26, 46) AND double3 < 1010.0 AND double3 > 1000.0 AND (string4 = '1' OR REGEXP_EXTRACT(string1, '^1') IS NOT NULL OR REGEXP_EXTRACT('Z' || string2, '^Z2') IS NOT NULL)) AND string1 = '1000' GROUP BY 1 ORDER BY 2"
  );

  @Param({
      "singleString",
      "fixedWidth",
      "fixedWidthNonNumeric",
      "always"
  })
  private String deferExpressionDimensions;

  @Param({
      // non-expression reference
      "0",
      "1",
      "2",
      "3",
      "4",
      "5",
      "6",
      // expressions, etc
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
      "47"
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
    return ImmutableList.of(SqlBenchmarkDatasets.EXPRESSIONS);
  }

  @Override
  protected Map<String, Object> getContext()
  {
    final Map<String, Object> context = ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize,
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize,
        GroupByQueryConfig.CTX_KEY_DEFER_EXPRESSION_DIMENSIONS, deferExpressionDimensions
    );
    return context;
  }
}
