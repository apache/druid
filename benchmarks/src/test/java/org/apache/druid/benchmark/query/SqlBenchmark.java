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

/**
 * Benchmark that tests various SQL queries.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlBenchmark extends SqlBaseQueryBenchmark
{
  static final List<String> QUERIES = ImmutableList.of(
      // 0, 1, 2, 3: Timeseries, unfiltered
      "SELECT COUNT(*) FROM druid.basic",
      "SELECT APPROX_COUNT_DISTINCT_BUILTIN(hyper) FROM druid.basic",
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM druid.basic",
      "SELECT FLOOR(__time TO MINUTE), SUM(sumLongSequential), SUM(sumFloatNormal) FROM druid.basic GROUP BY 1",

      // 4: Timeseries, low selectivity filter (90% of rows match)
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM druid.basic WHERE dimSequential NOT LIKE '%3'",

      // 5: Timeseries, high selectivity filter (0.1% of rows match)
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM druid.basic WHERE dimSequential = '311'",

      // 6: Timeseries, mixing low selectivity index-capable filter (90% of rows match) + cursor filter
      "SELECT SUM(sumLongSequential), SUM(sumFloatNormal) FROM druid.basic\n"
      + "WHERE dimSequential NOT LIKE '%3' AND maxLongUniform > 10",

      // 7: Timeseries, low selectivity toplevel filter (90%), high selectivity filtered aggregator (0.1%)
      "SELECT\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential = '311'),\n"
      + "  SUM(sumFloatNormal)\n"
      + "FROM druid.basic\n"
      + "WHERE dimSequential NOT LIKE '%3'",

      // 8: Timeseries, no toplevel filter, various filtered aggregators with clauses repeated.
      "SELECT\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential = '311'),\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential <> '311'),\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential LIKE '%3'),\n"
      + "  SUM(sumLongSequential) FILTER(WHERE dimSequential NOT LIKE '%3'),\n"
      + "  SUM(sumLongSequential),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential = '311'),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential <> '311'),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential LIKE '%3'),\n"
      + "  SUM(sumFloatNormal) FILTER(WHERE dimSequential NOT LIKE '%3'),\n"
      + "  SUM(sumFloatNormal),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential = '311'),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential <> '311'),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential LIKE '%3'),\n"
      + "  COUNT(*) FILTER(WHERE dimSequential NOT LIKE '%3'),\n"
      + "  COUNT(*)\n"
      + "FROM druid.basic",

      // 9: Timeseries, toplevel time filter, time-comparison filtered aggregators
      "SELECT\n"
      + "  SUM(sumLongSequential)\n"
      + "    FILTER(WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-01-01 12:00:00'),\n"
      + "  SUM(sumLongSequential)\n"
      + "    FILTER(WHERE __time >= TIMESTAMP '2000-01-01 12:00:00' AND __time < TIMESTAMP '2000-01-02 00:00:00')\n"
      + "FROM druid.basic\n"
      + "WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-01-02 00:00:00'",

      // 10, 11: GroupBy two strings, unfiltered, unordered
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential) FROM druid.basic GROUP BY 1, 2",
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential), COUNT(*) FROM druid.basic GROUP BY 1, 2",

      // 12, 13, 14: GroupBy one string, unfiltered, various aggregator configurations
      "SELECT dimZipf FROM druid.basic GROUP BY 1",
      "SELECT dimZipf, COUNT(*) FROM druid.basic GROUP BY 1 ORDER BY COUNT(*) DESC",
      "SELECT dimZipf, SUM(sumLongSequential), COUNT(*) FROM druid.basic GROUP BY 1 ORDER BY COUNT(*) DESC",

      // 15, 16: GroupBy long, unfiltered, unordered; with and without aggregators
      "SELECT maxLongUniform FROM druid.basic GROUP BY 1",
      "SELECT maxLongUniform, SUM(sumLongSequential), COUNT(*) FROM druid.basic GROUP BY 1",

      // 17, 18: GroupBy long, filter by long, unordered; with and without aggregators
      "SELECT maxLongUniform FROM druid.basic WHERE maxLongUniform > 10 GROUP BY 1",
      "SELECT maxLongUniform, SUM(sumLongSequential), COUNT(*) FROM druid.basic WHERE maxLongUniform > 10 GROUP BY 1",
      // 19: ultra mega union matrix
      "WITH matrix (dimZipf, dimSequential) AS (\n"
      + "  (\n"
      + "    SELECT '100', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '100'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '110', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '110'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '120', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '120'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '130', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '130'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '140', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '140'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '150', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '150'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '160', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '160'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '170', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '170'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '180', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '180'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '190', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '190'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '200', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '200'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '210', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '210'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '220', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '220'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '230', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '230'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '240', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '240'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '250', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '250'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '260', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '260'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '270', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '270'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '280', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '280'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '290', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '290'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '300', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '300'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '310', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '310'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '320', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '320'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '330', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '330'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '340', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '340'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '350', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '350'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '360', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '360'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '370', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '370'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT '380', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE dimZipf = '380'\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + "UNION ALL\n"
      + "  (\n"
      + "    SELECT 'other', dimSequential\n"
      + "    FROM (SELECT * FROM druid.basic WHERE dimUniform != 1)\n"
      + "    WHERE\n"
      + "      dimZipf NOT IN (\n"
      + "        '100', '110', '120', '130', '140', '150', '160', '170', '180', '190',\n"
      + "        '200', '210', '220', '230', '240', '250', '260', '270', '280', '290',\n"
      + "        '300', '310', '320', '330', '340', '350', '360', '370', '380'\n"
      + "      )\n"
      + "    GROUP BY dimSequential\n"
      + "  )\n"
      + ")\n"
      + "SELECT * FROM matrix",

      // 20: GroupBy, doubles sketches
      "SELECT dimZipf, APPROX_QUANTILE_DS(sumFloatNormal, 0.5), DS_QUANTILES_SKETCH(maxLongUniform) "
      + "FROM druid.basic "
      + "GROUP BY 1",

      // 21, 22: stringy stuff
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential) FROM druid.basic WHERE dimUniform NOT LIKE '%3' GROUP BY 1, 2",
      "SELECT dimZipf, SUM(sumLongSequential) FROM druid.basic WHERE dimSequential = '311' GROUP BY 1 ORDER BY 1",
      // 23: full scan
      "SELECT * FROM druid.basic",
      "SELECT * FROM druid.basic WHERE dimSequential IN ('1', '2', '3', '4', '5', '10', '11', '20', '21', '23', '40', '50', '64', '70', '100')",
      "SELECT * FROM druid.basic WHERE dimSequential > '10' AND dimSequential < '8500'",
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential) FROM druid.basic WHERE dimSequential IN ('1', '2', '3', '4', '5', '10', '11', '20', '21', '23', '40', '50', '64', '70', '100') GROUP BY 1, 2",
      "SELECT dimSequential, dimZipf, SUM(sumLongSequential) FROM druid.basic WHERE dimSequential > '10' AND dimSequential < '8500' GROUP BY 1, 2",

      // 28, 29, 30, 31: Approximate count distinct of strings
      "SELECT APPROX_COUNT_DISTINCT_BUILTIN(dimZipf) FROM druid.basic",
      "SELECT APPROX_COUNT_DISTINCT_DS_HLL(dimZipf) FROM druid.basic",
      "SELECT APPROX_COUNT_DISTINCT_DS_HLL_UTF8(dimZipf) FROM druid.basic",
      "SELECT APPROX_COUNT_DISTINCT_DS_THETA(dimZipf) FROM druid.basic",
      // 32: LATEST aggregator long
      "SELECT LATEST(long1) FROM druid.expressions",
      // 33: LATEST aggregator double
      "SELECT LATEST(double4) FROM druid.expressions",
      // 34: LATEST aggregator double
      "SELECT LATEST(float3) FROM druid.expressions",
      // 35: LATEST aggregator double
      "SELECT LATEST(float3), LATEST(long1), LATEST(double4) FROM druid.expressions",
      // 36,37: filter numeric nulls
      "SELECT SUM(long5) FROM druid.expressions WHERE long5 IS NOT NULL",
      "SELECT string2, SUM(long5) FROM druid.expressions WHERE long5 IS NOT NULL GROUP BY 1",
      // 38: EARLIEST aggregator long
      "SELECT EARLIEST(long1) FROM druid.expressions",
      // 39: EARLIEST aggregator double
      "SELECT EARLIEST(double4) FROM druid.expressions",
      // 40: EARLIEST aggregator float
      "SELECT EARLIEST(float3) FROM druid.expressions",
      // 41: nested OR filter
      "SELECT dimSequential, COUNT(*) from druid.basic WHERE dimSequential = '1' AND (dimMultivalEnumerated IN ('Hello', 'World', 'druid.basic', 'Bar', 'Baz') OR sumLongSequential = 1) GROUP BY 1"
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
      "41"
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
    return ImmutableList.of(SqlBenchmarkDatasets.BASIC);
  }
}
