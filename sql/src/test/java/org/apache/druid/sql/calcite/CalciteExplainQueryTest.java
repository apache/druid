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

package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for EXPLAIN PLAN queries.
 */
public class CalciteExplainQueryTest extends BaseCalciteQueryTest
{
  @Test
  public void testExplainCountStarOnView()
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    final String query = "EXPLAIN PLAN FOR SELECT COUNT(*) FROM view.aview WHERE dim1_firstchar <> 'z'";
    final String legacyExplanation = NullHandling.replaceWithDefault()
                                     ? "DruidQueryRel(query=[{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"filter\":{\"type\":\"and\",\"fields\":[{\"type\":\"selector\",\"dimension\":\"dim2\",\"value\":\"a\"},{\"type\":\"not\",\"field\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":\"z\",\"extractionFn\":{\"type\":\"substring\",\"index\":0,\"length\":1}}}]},\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}}], signature=[{a0:LONG}])\n"
                                     : "DruidQueryRel(query=[{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"v0\",\"expression\":\"substring(\\\"dim1\\\", 0, 1)\",\"outputType\":\"STRING\"}],\"filter\":{\"type\":\"and\",\"fields\":[{\"type\":\"equals\",\"column\":\"dim2\",\"matchValueType\":\"STRING\",\"matchValue\":\"a\"},{\"type\":\"not\",\"field\":{\"type\":\"equals\",\"column\":\"v0\",\"matchValueType\":\"STRING\",\"matchValue\":\"z\"}}]},\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}}], signature=[{a0:LONG}])\n";
    final String explanation = NullHandling.replaceWithDefault()
                               ? "[{"
                                 + "\"query\":{\"queryType\":\"timeseries\","
                                 + "\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},"
                                 + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                                 + "\"filter\":{\"type\":\"and\",\"fields\":[{\"type\":\"selector\",\"dimension\":\"dim2\",\"value\":\"a\"},{\"type\":\"not\",\"field\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":\"z\",\"extractionFn\":{\"type\":\"substring\",\"index\":0,\"length\":1}}}]},"
                                 + "\"granularity\":{\"type\":\"all\"},"
                                 + "\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],"
                                 + "\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}},"
                                 + "\"signature\":[{\"name\":\"a0\",\"type\":\"LONG\"}],"
                                 + "\"columnMappings\":[{\"queryColumn\":\"a0\",\"outputColumn\":\"EXPR$0\"}]"
                                 + "}]"
                               : "[{"
                                 + "\"query\":{\"queryType\":\"timeseries\","
                                 + "\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},"
                                 + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                                 + "\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"v0\",\"expression\":\"substring(\\\"dim1\\\", 0, 1)\",\"outputType\":\"STRING\"}],"
                                 + "\"filter\":{\"type\":\"and\",\"fields\":[{\"type\":\"equals\",\"column\":\"dim2\",\"matchValueType\":\"STRING\",\"matchValue\":\"a\"},{\"type\":\"not\",\"field\":{\"type\":\"equals\",\"column\":\"v0\",\"matchValueType\":\"STRING\",\"matchValue\":\"z\"}}]},"
                                 + "\"granularity\":{\"type\":\"all\"},"
                                 + "\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],"
                                 + "\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}},"
                                 + "\"signature\":[{\"name\":\"a0\",\"type\":\"LONG\"}],"
                                 + "\"columnMappings\":[{\"queryColumn\":\"a0\",\"outputColumn\":\"EXPR$0\"}]"
                                 + "}]";
    final String resources = "[{\"name\":\"aview\",\"type\":\"VIEW\"}]";
    final String attributes = "{\"statementType\":\"SELECT\"}";

    testQuery(
        PLANNER_CONFIG_LEGACY_QUERY_EXPLAIN,
        query,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{legacyExplanation, resources, attributes}
        )
    );
    testQuery(
        PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN,
        query,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{explanation, resources, attributes}
        )
    );
  }

  @Test
  public void testExplainInformationSchemaColumns()
  {
    final String explanation =
        "BindableProject(COLUMN_NAME=[$3], DATA_TYPE=[$7])\n"
        + "  BindableFilter(condition=[AND(=($1, 'druid'), =($2, 'foo'))])\n"
        + "    BindableTableScan(table=[[INFORMATION_SCHEMA, COLUMNS]])\n";

    final String resources = "[]";
    final String attributes = "{\"statementType\":\"SELECT\"}";

    testQuery(
        "EXPLAIN PLAN FOR\n"
        + "SELECT COLUMN_NAME, DATA_TYPE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{explanation, resources, attributes}
        )
    );
  }

  @Test
  public void testExplainExactCountDistinctOfSemiJoinResult()
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    final String query = "EXPLAIN PLAN FOR SELECT COUNT(*)\n"
                         + "FROM (\n"
                         + "  SELECT DISTINCT dim2\n"
                         + "  FROM druid.foo\n"
                         + "  WHERE SUBSTRING(dim2, 1, 1) IN (\n"
                         + "    SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo WHERE dim1 IS NOT NULL\n"
                         + "  )\n"
                         + ")";
    final String legacyExplanation = NullHandling.replaceWithDefault()
                                     ?
                                     "DruidOuterQueryRel(query=[{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"__subquery__\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}}], signature=[{a0:LONG}])\n"
                                     + "  DruidJoinQueryRel(condition=[=(SUBSTRING($2, 1, 1), $8)], joinType=[inner], query=[{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"table\",\"name\":\"__join__\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"default\",\"dimension\":\"dim2\",\"outputName\":\"d0\",\"outputType\":\"STRING\"}],\"limitSpec\":{\"type\":\"NoopLimitSpec\"},\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}}], signature=[{d0:STRING}])\n"
                                     + "    DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"__time\",\"cnt\",\"dim1\",\"dim2\",\"dim3\",\"m1\",\"m2\",\"unique_dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"LONG\",\"LONG\",\"STRING\",\"STRING\",\"STRING\",\"FLOAT\",\"DOUBLE\",\"COMPLEX<hyperUnique>\"],\"granularity\":{\"type\":\"all\"}}], signature=[{__time:LONG, dim1:STRING, dim2:STRING, dim3:STRING, cnt:LONG, m1:FLOAT, m2:DOUBLE, unique_dim1:COMPLEX<hyperUnique>}])\n"
                                     + "    DruidQueryRel(query=[{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"filter\":{\"type\":\"not\",\"field\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":null}},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"extraction\",\"dimension\":\"dim1\",\"outputName\":\"d0\",\"outputType\":\"STRING\",\"extractionFn\":{\"type\":\"substring\",\"index\":0,\"length\":1}}],\"limitSpec\":{\"type\":\"NoopLimitSpec\"},\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}}], signature=[{d0:STRING}])\n"
                                     :
                                     "DruidOuterQueryRel(query=[{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"__subquery__\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}}], signature=[{a0:LONG}])\n"
                                     + "  DruidJoinQueryRel(condition=[=(SUBSTRING($2, 1, 1), $8)], joinType=[inner], query=[{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"table\",\"name\":\"__join__\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"default\",\"dimension\":\"dim2\",\"outputName\":\"d0\",\"outputType\":\"STRING\"}],\"limitSpec\":{\"type\":\"NoopLimitSpec\"},\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}}], signature=[{d0:STRING}])\n"
                                     + "    DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"__time\",\"cnt\",\"dim1\",\"dim2\",\"dim3\",\"m1\",\"m2\",\"unique_dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"LONG\",\"LONG\",\"STRING\",\"STRING\",\"STRING\",\"FLOAT\",\"DOUBLE\",\"COMPLEX<hyperUnique>\"],\"granularity\":{\"type\":\"all\"}}], signature=[{__time:LONG, dim1:STRING, dim2:STRING, dim3:STRING, cnt:LONG, m1:FLOAT, m2:DOUBLE, unique_dim1:COMPLEX<hyperUnique>}])\n"
                                     + "    DruidQueryRel(query=[{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"filter\":{\"type\":\"not\",\"field\":{\"type\":\"null\",\"column\":\"dim1\"}},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"extraction\",\"dimension\":\"dim1\",\"outputName\":\"d0\",\"outputType\":\"STRING\",\"extractionFn\":{\"type\":\"substring\",\"index\":0,\"length\":1}}],\"limitSpec\":{\"type\":\"NoopLimitSpec\"},\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}}], signature=[{d0:STRING}])\n";
    final String explanation = NullHandling.replaceWithDefault() ?
                               "["
                               + "{\"query\":{\"queryType\":\"groupBy\","
                               + "\"dataSource\":{\"type\":\"query\",\"query\":{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"join\",\"left\":{\"type\":\"table\",\"name\":\"foo\"},\"right\":{\"type\":\"query\",\"query\":{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"filter\":{\"type\":\"not\",\"field\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":null}},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"extraction\",\"dimension\":\"dim1\",\"outputName\":\"d0\",\"outputType\":\"STRING\",\"extractionFn\":{\"type\":\"substring\",\"index\":0,\"length\":1}}],\"limitSpec\":{\"type\":\"NoopLimitSpec\"},\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}}},\"rightPrefix\":\"j0.\",\"condition\":\"(substring(\\\"dim2\\\", 0, 1) == \\\"j0.d0\\\")\",\"joinType\":\"INNER\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"default\",\"dimension\":\"dim2\",\"outputName\":\"d0\",\"outputType\":\"STRING\"}],\"limitSpec\":{\"type\":\"NoopLimitSpec\"},\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}}},"
                               + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                               + "\"granularity\":{\"type\":\"all\"},"
                               + "\"dimensions\":[],"
                               + "\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],"
                               + "\"limitSpec\":{\"type\":\"NoopLimitSpec\"},"
                               + "\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}},"
                               + "\"signature\":[{\"name\":\"a0\",\"type\":\"LONG\"}],"
                               + "\"columnMappings\":[{\"queryColumn\":\"a0\",\"outputColumn\":\"EXPR$0\"}]"
                               + "}]"
                                                                 : "[{\"query\":{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"query\",\"query\":{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"join\",\"left\":{\"type\":\"table\",\"name\":\"foo\"},\"right\":{\"type\":\"query\",\"query\":{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"filter\":{\"type\":\"not\",\"field\":{\"type\":\"null\",\"column\":\"dim1\"}},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"extraction\",\"dimension\":\"dim1\",\"outputName\":\"d0\",\"outputType\":\"STRING\",\"extractionFn\":{\"type\":\"substring\",\"index\":0,\"length\":1}}],\"limitSpec\":{\"type\":\"NoopLimitSpec\"},\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}}},\"rightPrefix\":\"j0.\",\"condition\":\"(substring(\\\"dim2\\\", 0, 1) == \\\"j0.d0\\\")\",\"joinType\":\"INNER\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"default\",\"dimension\":\"dim2\",\"outputName\":\"d0\",\"outputType\":\"STRING\"}],\"limitSpec\":{\"type\":\"NoopLimitSpec\"},\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}}},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"granularity\":{\"type\":\"all\"},\"dimensions\":[],\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"limitSpec\":{\"type\":\"NoopLimitSpec\"},\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"}},\"signature\":[{\"name\":\"a0\",\"type\":\"LONG\"}],\"columnMappings\":[{\"queryColumn\":\"a0\",\"outputColumn\":\"EXPR$0\"}]}]";
    final String resources = "[{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]";
    final String attributes = "{\"statementType\":\"SELECT\"}";

    testQuery(
        query,
        ImmutableList.of(),
        ImmutableList.of(new Object[]{explanation, resources, attributes})
    );

    testQuery(
        PLANNER_CONFIG_LEGACY_QUERY_EXPLAIN,
        query,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(new Object[]{legacyExplanation, resources, attributes})
    );
  }

  // This testcase has been added here and not in CalciteSelectQueryTest since this checks if the overrides are working
  // properly when displaying the output of "EXPLAIN PLAN FOR ..." queries
  @Test
  public void testExplainSelectStarWithOverrides()
  {
    Map<String, Object> useRegularExplainContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    useRegularExplainContext.put(PlannerConfig.CTX_KEY_USE_NATIVE_QUERY_EXPLAIN, true);

    Map<String, Object> legacyExplainContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    legacyExplainContext.put(PlannerConfig.CTX_KEY_USE_NATIVE_QUERY_EXPLAIN, false);


    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();
    String legacyExplanationWithContext = "DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"__time\",\"cnt\",\"dim1\",\"dim2\",\"dim3\",\"m1\",\"m2\",\"unique_dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"useNativeQueryExplain\":false,\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"LONG\",\"LONG\",\"STRING\",\"STRING\",\"STRING\",\"FLOAT\",\"DOUBLE\",\"COMPLEX<hyperUnique>\"],\"granularity\":{\"type\":\"all\"}}], signature=[{__time:LONG, dim1:STRING, dim2:STRING, dim3:STRING, cnt:LONG, m1:FLOAT, m2:DOUBLE, unique_dim1:COMPLEX<hyperUnique>}])\n";
    String explanation = "[{"
                         + "\"query\":{\"queryType\":\"scan\","
                         + "\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},"
                         + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                         + "\"resultFormat\":\"compactedList\","
                         + "\"columns\":[\"__time\",\"cnt\",\"dim1\",\"dim2\",\"dim3\",\"m1\",\"m2\",\"unique_dim1\"],"
                         + "\"legacy\":false,"
                         + "\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},"
                         + "\"columnTypes\":[\"LONG\",\"LONG\",\"STRING\",\"STRING\",\"STRING\",\"FLOAT\",\"DOUBLE\",\"COMPLEX<hyperUnique>\"],\"granularity\":{\"type\":\"all\"}},"
                         + "\"signature\":[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"dim1\",\"type\":\"STRING\"},{\"name\":\"dim2\",\"type\":\"STRING\"},{\"name\":\"dim3\",\"type\":\"STRING\"},{\"name\":\"cnt\",\"type\":\"LONG\"},{\"name\":\"m1\",\"type\":\"FLOAT\"},{\"name\":\"m2\",\"type\":\"DOUBLE\"},{\"name\":\"unique_dim1\",\"type\":\"COMPLEX<hyperUnique>\"}],"
                         + "\"columnMappings\":[{\"queryColumn\":\"__time\",\"outputColumn\":\"__time\"},{\"queryColumn\":\"dim1\",\"outputColumn\":\"dim1\"},{\"queryColumn\":\"dim2\",\"outputColumn\":\"dim2\"},{\"queryColumn\":\"dim3\",\"outputColumn\":\"dim3\"},{\"queryColumn\":\"cnt\",\"outputColumn\":\"cnt\"},{\"queryColumn\":\"m1\",\"outputColumn\":\"m1\"},{\"queryColumn\":\"m2\",\"outputColumn\":\"m2\"},{\"queryColumn\":\"unique_dim1\",\"outputColumn\":\"unique_dim1\"}]"
                         + "}]";

    String explanationWithContext = "[{"
                                    + "\"query\":{\"queryType\":\"scan\","
                                    + "\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},"
                                    + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                                    + "\"resultFormat\":\"compactedList\","
                                    + "\"columns\":[\"__time\",\"cnt\",\"dim1\",\"dim2\",\"dim3\",\"m1\",\"m2\",\"unique_dim1\"],"
                                    + "\"legacy\":false,"
                                    + "\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"useNativeQueryExplain\":true,\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},"
                                    + "\"columnTypes\":[\"LONG\",\"LONG\",\"STRING\",\"STRING\",\"STRING\",\"FLOAT\",\"DOUBLE\",\"COMPLEX<hyperUnique>\"],\"granularity\":{\"type\":\"all\"}},"
                                    + "\"signature\":[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"dim1\",\"type\":\"STRING\"},{\"name\":\"dim2\",\"type\":\"STRING\"},{\"name\":\"dim3\",\"type\":\"STRING\"},{\"name\":\"cnt\",\"type\":\"LONG\"},{\"name\":\"m1\",\"type\":\"FLOAT\"},{\"name\":\"m2\",\"type\":\"DOUBLE\"},{\"name\":\"unique_dim1\",\"type\":\"COMPLEX<hyperUnique>\"}],"
                                    + "\"columnMappings\":[{\"queryColumn\":\"__time\",\"outputColumn\":\"__time\"},{\"queryColumn\":\"dim1\",\"outputColumn\":\"dim1\"},{\"queryColumn\":\"dim2\",\"outputColumn\":\"dim2\"},{\"queryColumn\":\"dim3\",\"outputColumn\":\"dim3\"},{\"queryColumn\":\"cnt\",\"outputColumn\":\"cnt\"},{\"queryColumn\":\"m1\",\"outputColumn\":\"m1\"},{\"queryColumn\":\"m2\",\"outputColumn\":\"m2\"},{\"queryColumn\":\"unique_dim1\",\"outputColumn\":\"unique_dim1\"}]"
                                    + "}]";
    String sql = "EXPLAIN PLAN FOR SELECT * FROM druid.foo";
    String resources = "[{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]";
    final String attributes = "{\"statementType\":\"SELECT\"}";

    // Test when default config and no overrides
    testQuery(sql, ImmutableList.of(), ImmutableList.of(new Object[]{explanation, resources, attributes}));

    // Test when default config and useNativeQueryExplain is overridden in the context
    testQuery(
        sql,
        legacyExplainContext,
        ImmutableList.of(),
        ImmutableList.of(new Object[]{legacyExplanationWithContext, resources, attributes})
    );

    // Test when useNativeQueryExplain enabled by default and no overrides
    testQuery(
        PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(new Object[]{explanation, resources, attributes})
    );

    // Test when useNativeQueryExplain enabled by default but is overriden in the context
    testQuery(
        PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN,
        useRegularExplainContext,
        sql,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(new Object[]{explanationWithContext, resources, attributes})
    );
  }

  @Test
  public void testExplainMultipleTopLevelUnionAllQueries()
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    final String query = "EXPLAIN PLAN FOR SELECT dim1 FROM druid.foo\n"
                         + "UNION ALL (SELECT dim1 FROM druid.foo WHERE dim1 = '42'\n"
                         + "UNION ALL SELECT dim1 FROM druid.foo WHERE dim1 = '44')";
    final String legacyExplanation = NullHandling.replaceWithDefault()
                                     ? "DruidUnionRel(limit=[-1])\n"
                                       + "  DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\"],\"granularity\":{\"type\":\"all\"}}], signature=[{dim1:STRING}])\n"
                                       + "  DruidUnionRel(limit=[-1])\n"
                                       + "    DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"filter\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":\"42\"},\"columns\":[\"dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\"],\"granularity\":{\"type\":\"all\"}}], signature=[{dim1:STRING}])\n"
                                       + "    DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"filter\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":\"44\"},\"columns\":[\"dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\"],\"granularity\":{\"type\":\"all\"}}], signature=[{dim1:STRING}])\n"
                                     : "DruidUnionRel(limit=[-1])\n"
                                       + "  DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\"],\"granularity\":{\"type\":\"all\"}}], signature=[{dim1:STRING}])\n"
                                       + "  DruidUnionRel(limit=[-1])\n"
                                       + "    DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"filter\":{\"type\":\"equals\",\"column\":\"dim1\",\"matchValueType\":\"STRING\",\"matchValue\":\"42\"},\"columns\":[\"dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\"],\"granularity\":{\"type\":\"all\"}}], signature=[{dim1:STRING}])\n"
                                       + "    DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"filter\":{\"type\":\"equals\",\"column\":\"dim1\",\"matchValueType\":\"STRING\",\"matchValue\":\"44\"},\"columns\":[\"dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\"],\"granularity\":{\"type\":\"all\"}}], signature=[{dim1:STRING}])\n";
    final String explanation = NullHandling.replaceWithDefault()
                               ? "["
                                 + "{"
                                 + "\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\"],\"granularity\":{\"type\":\"all\"}},"
                                 + "\"signature\":[{\"name\":\"dim1\",\"type\":\"STRING\"}],"
                                 + "\"columnMappings\":[{\"queryColumn\":\"dim1\",\"outputColumn\":\"dim1\"}]"
                                 + "},"
                                 + "{"
                                 + "\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"filter\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":\"42\"},\"columns\":[\"dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\"],\"granularity\":{\"type\":\"all\"}},"
                                 + "\"signature\":[{\"name\":\"dim1\",\"type\":\"STRING\"}],"
                                 + "\"columnMappings\":[{\"queryColumn\":\"dim1\",\"outputColumn\":\"dim1\"}]"
                                 + "},"
                                 + "{"
                                 + "\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"filter\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":\"44\"},\"columns\":[\"dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\"],\"granularity\":{\"type\":\"all\"}},"
                                 + "\"signature\":[{\"name\":\"dim1\",\"type\":\"STRING\"}],"
                                 + "\"columnMappings\":[{\"queryColumn\":\"dim1\",\"outputColumn\":\"dim1\"}]"
                                 + "}]"
                               : "[{\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\"],\"granularity\":{\"type\":\"all\"}},\"signature\":[{\"name\":\"dim1\",\"type\":\"STRING\"}],\"columnMappings\":[{\"queryColumn\":\"dim1\",\"outputColumn\":\"dim1\"}]},{\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"filter\":{\"type\":\"equals\",\"column\":\"dim1\",\"matchValueType\":\"STRING\",\"matchValue\":\"42\"},\"columns\":[\"dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\"],\"granularity\":{\"type\":\"all\"}},\"signature\":[{\"name\":\"dim1\",\"type\":\"STRING\"}],\"columnMappings\":[{\"queryColumn\":\"dim1\",\"outputColumn\":\"dim1\"}]},{\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"filter\":{\"type\":\"equals\",\"column\":\"dim1\",\"matchValueType\":\"STRING\",\"matchValue\":\"44\"},\"columns\":[\"dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"columnTypes\":[\"STRING\"],\"granularity\":{\"type\":\"all\"}},\"signature\":[{\"name\":\"dim1\",\"type\":\"STRING\"}],\"columnMappings\":[{\"queryColumn\":\"dim1\",\"outputColumn\":\"dim1\"}]}]";
    final String resources = "[{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]";
    final String attributes = "{\"statementType\":\"SELECT\"}";
    testQuery(
        PLANNER_CONFIG_LEGACY_QUERY_EXPLAIN,
        query,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{legacyExplanation, resources, attributes}
        )
    );
    testQuery(
        PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN,
        query,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{explanation, resources, attributes}
        )
    );
  }

  @Test
  public void testExplainSelectMvfilterExpressions()
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    final String explainSql = "EXPLAIN PLAN FOR SELECT"
                              + " MV_FILTER_ONLY(\"dim1\", ARRAY['true', 'false']),"
                              + " MV_FILTER_NONE(\"dim1\", ARRAY['true', 'false'])"
                              + " FROM druid.foo";

    // Test plan as default expressions
    final Map<String, Object> defaultExprContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    defaultExprContext.put(PlannerConfig.CTX_KEY_USE_NATIVE_QUERY_EXPLAIN, true);
    defaultExprContext.put(PlannerConfig.CTX_KEY_FORCE_EXPRESSION_VIRTUAL_COLUMNS, true);

    final String expectedPlanWithDefaultExpressions = "[{"
                                                      + "\"query\":{\"queryType\":\"scan\","
                                                      + "\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},"
                                                      + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                                                      + "\"virtualColumns\":["
                                                      + "{\"type\":\"expression\",\"name\":\"v0\",\"expression\":\"filter((x) -> array_contains(array('true','false'), x), \\\"dim1\\\")\",\"outputType\":\"STRING\"},"
                                                      + "{\"type\":\"expression\",\"name\":\"v1\",\"expression\":\"filter((x) -> !array_contains(array('true','false'), x), \\\"dim1\\\")\",\"outputType\":\"STRING\"}"
                                                      + "],"
                                                      + "\"resultFormat\":\"compactedList\","
                                                      + "\"columns\":[\"v0\",\"v1\"],"
                                                      + "\"legacy\":false,"
                                                      + "\"context\":{\"defaultTimeout\":300000,\"forceExpressionVirtualColumns\":true,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"useNativeQueryExplain\":true,\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},"
                                                      + "\"columnTypes\":[\"STRING\",\"STRING\"],"
                                                      + "\"granularity\":{\"type\":\"all\"}},"
                                                      + "\"signature\":[{\"name\":\"v0\",\"type\":\"STRING\"},{\"name\":\"v1\",\"type\":\"STRING\"}],"
                                                      + "\"columnMappings\":[{\"queryColumn\":\"v0\",\"outputColumn\":\"EXPR$0\"},{\"queryColumn\":\"v1\",\"outputColumn\":\"EXPR$1\"}]"
                                                      + "}]";
    final String expectedResources = "[{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]";
    final String expectedAttributes = "{\"statementType\":\"SELECT\"}";
    testQuery(
        explainSql,
        defaultExprContext,
        ImmutableList.of(),
        ImmutableList.of(new Object[]{expectedPlanWithDefaultExpressions, expectedResources, expectedAttributes})
    );

    // Test plan as mv-filtered virtual columns
    final String expectedPlanWithMvfiltered = "[{"
                                              + "\"query\":{\"queryType\":\"scan\","
                                              + "\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},"
                                              + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                                              + "\"virtualColumns\":["
                                              + "{\"type\":\"mv-filtered\",\"name\":\"v0\",\"delegate\":{\"type\":\"default\",\"dimension\":\"dim1\",\"outputName\":\"dim1\",\"outputType\":\"STRING\"},\"values\":[\"true\",\"false\"],\"isAllowList\":true},"
                                              + "{\"type\":\"mv-filtered\",\"name\":\"v1\",\"delegate\":{\"type\":\"default\",\"dimension\":\"dim1\",\"outputName\":\"dim1\",\"outputType\":\"STRING\"},\"values\":[\"true\",\"false\"],\"isAllowList\":false}"
                                              + "],"
                                              + "\"resultFormat\":\"compactedList\","
                                              + "\"columns\":[\"v0\",\"v1\"],"
                                              + "\"legacy\":false,"
                                              + "\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"useNativeQueryExplain\":true,\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},"
                                              + "\"columnTypes\":[\"STRING\",\"STRING\"],"
                                              + "\"granularity\":{\"type\":\"all\"}},"
                                              + "\"signature\":[{\"name\":\"v0\",\"type\":\"STRING\"},{\"name\":\"v1\",\"type\":\"STRING\"}],"
                                              + "\"columnMappings\":[{\"queryColumn\":\"v0\",\"outputColumn\":\"EXPR$0\"},{\"queryColumn\":\"v1\",\"outputColumn\":\"EXPR$1\"}]"
                                              + "}]";
    final Map<String, Object> mvFilteredContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    mvFilteredContext.put(PlannerConfig.CTX_KEY_USE_NATIVE_QUERY_EXPLAIN, true);

    testQuery(
        explainSql,
        mvFilteredContext,
        ImmutableList.of(),
        ImmutableList.of(new Object[]{expectedPlanWithMvfiltered, expectedResources, expectedAttributes})
    );
  }

  @Test
  public void testExplainSelectTimestampExpression()
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    final String explainSql = "EXPLAIN PLAN FOR SELECT"
                              + " TIME_PARSE(dim1)"
                              + " FROM druid.foo";

    final Map<String, Object> queryContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    queryContext.put(PlannerConfig.CTX_KEY_USE_NATIVE_QUERY_EXPLAIN, true);

    final String expectedPlan = "[{"
                                + "\"query\":{\"queryType\":\"scan\","
                                + "\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},"
                                + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                                + "\"virtualColumns\":["
                                + "{\"type\":\"expression\",\"name\":\"v0\",\"expression\":\"timestamp_parse(\\\"dim1\\\",null,'UTC')\",\"outputType\":\"LONG\"}"
                                + "],"
                                + "\"resultFormat\":\"compactedList\","
                                + "\"columns\":[\"v0\"],"
                                + "\"legacy\":false,"
                                + "\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"useNativeQueryExplain\":true,\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},"
                                + "\"columnTypes\":[\"LONG\"],"
                                + "\"granularity\":{\"type\":\"all\"}},"
                                + "\"signature\":[{\"name\":\"v0\",\"type\":\"LONG\"}],"
                                + "\"columnMappings\":[{\"queryColumn\":\"v0\",\"outputColumn\":\"EXPR$0\"}]"
                                + "}]";
    final String expectedResources = "[{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]";
    final String expectedAttributes = "{\"statementType\":\"SELECT\"}";
    // Verify the query plan
    testQuery(
        explainSql,
        queryContext,
        ImmutableList.of(),
        ImmutableList.of(new Object[]{expectedPlan, expectedResources, expectedAttributes})
    );
  }

}
