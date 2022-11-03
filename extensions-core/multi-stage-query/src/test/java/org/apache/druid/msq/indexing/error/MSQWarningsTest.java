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

package org.apache.druid.msq.indexing.error;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.indexing.ColumnMapping;
import org.apache.druid.msq.indexing.ColumnMappings;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * unparseable.gz is a file containing 10 valid and 9 invalid records
 */
public class MSQWarningsTest extends MSQTestBase
{

  private File toRead;
  private RowSignature rowSignature;
  private String toReadFileNameAsJson;

  private Query<?> defaultQuery;
  private ColumnMappings defaultColumnMappings;

  @Before
  public void setUp3() throws IOException
  {
    toRead = getResourceAsTemporaryFile("/unparseable.gz");
    toReadFileNameAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    rowSignature = RowSignature.builder()
                               .add("__time", ColumnType.LONG)
                               .add("cnt", ColumnType.LONG)
                               .build();

    defaultQuery = GroupByQuery.builder()
                               .setDataSource(new ExternalDataSource(
                                   new LocalInputSource(
                                       null,
                                       null,
                                       ImmutableList.of(
                                           toRead.getAbsoluteFile()
                                       )
                                   ),
                                   new JsonInputFormat(null, null, null, null, null),
                                   RowSignature.builder()
                                               .add("timestamp", ColumnType.STRING)
                                               .add("page", ColumnType.STRING)
                                               .add("user", ColumnType.STRING)
                                               .build()
                               ))
                               .setInterval(querySegmentSpec(Filtration.eternity()))
                               .setGranularity(Granularities.ALL)
                               .setVirtualColumns(new ExpressionVirtualColumn(
                                   "v0",
                                   "timestamp_floor(timestamp_parse(\"timestamp\",null,'UTC'),'P1D',null,'UTC')",
                                   ColumnType.LONG,
                                   CalciteTests.createExprMacroTable()
                               ))
                               .setDimensions(dimensions(new DefaultDimensionSpec(
                                                             "v0",
                                                             "d0",
                                                             ColumnType.LONG
                                                         )
                               ))
                               .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                               .setContext(DEFAULT_MSQ_CONTEXT)
                               .build();

    defaultColumnMappings = new ColumnMappings(ImmutableList.of(
        new ColumnMapping("d0", "__time"),
        new ColumnMapping("a0", "cnt")
    )
    );
  }


  @Test
  public void testThrowExceptionWhenParseExceptionsExceedLimit()
  {
    testSelectQuery().setSql("SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [\"" + toRead.getAbsolutePath() + "\"],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1")
                     .setQueryContext(ImmutableMap.of(
                         MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 0
                     ))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .setExpectedMSQSpec(
                         MSQSpec.builder()
                                .query(defaultQuery)
                                .columnMappings(defaultColumnMappings)
                                .tuningConfig(MSQTuningConfig.defaultConfig())
                                .build())
                     .setExpectedMSQFaultClass(CannotParseExternalDataFault.class)
                     .verifyResults();
  }

  @Test
  public void testSuccessWhenNoLimitEnforced()
  {
    final Map<String, Object> userContext = ImmutableMap.of(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, -1);

    testSelectQuery().setSql("SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [\"" + toRead.getAbsolutePath() + "\"],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1")
                     .setQueryContext(
                         ImmutableMap.<String, Object>builder()
                                     .putAll(DEFAULT_MSQ_CONTEXT)
                                     .putAll(userContext)
                                     .build()
                     )
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
                     .setExpectedMSQSpec(
                         MSQSpec.builder()
                                .query(defaultQuery.withOverriddenContext(userContext))
                                .columnMappings(defaultColumnMappings)
                                .tuningConfig(MSQTuningConfig.defaultConfig())
                                .build())
                     .verifyResults();
  }

  @Test
  public void testInvalidMaxParseExceptionsPassed()
  {
    testSelectQuery().setSql("SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [\"" + toRead.getAbsolutePath() + "\"],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1")
                     .setQueryContext(ImmutableMap.of(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, -2))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
                     .setExpectedMSQSpec(
                         MSQSpec.builder()
                                .query(defaultQuery)
                                .columnMappings(defaultColumnMappings)
                                .tuningConfig(MSQTuningConfig.defaultConfig())
                                .build())
                     .setExpectedMSQFault(UnknownFault.forMessage(
                         "java.lang.IllegalArgumentException: "
                         + "Invalid limit of -2 supplied for warnings of type CannotParseExternalData. "
                         + "Limit can be greater than or equal to -1."))
                     .verifyResults();
  }

  @Test
  public void testFailureWhenParseExceptionsExceedPositiveLimit()
  {
    testSelectQuery().setSql("SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [\"" + toRead.getAbsolutePath() + "\"],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1")
                     .setQueryContext(ImmutableMap.of(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 4))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .setExpectedMSQSpec(
                         MSQSpec.builder()
                                .query(defaultQuery)
                                .columnMappings(defaultColumnMappings)
                                .tuningConfig(MSQTuningConfig.defaultConfig())
                                .build())
                     .setExpectedMSQFault(new TooManyWarningsFault(4, CannotParseExternalDataFault.CODE))
                     .verifyResults();
  }


  @Test
  public void testSuccessWhenParseExceptionsOnLimit()
  {
    final Map<String, Object> userContext = ImmutableMap.of(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 10);

    testSelectQuery().setSql("SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [\"" + toRead.getAbsolutePath() + "\"],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1")
                     .setQueryContext(
                         ImmutableMap.<String, Object>builder()
                                     .putAll(DEFAULT_MSQ_CONTEXT)
                                     .putAll(userContext)
                                     .build()
                     )
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
                     .setExpectedMSQSpec(
                         MSQSpec.builder()
                                .query(defaultQuery.withOverriddenContext(userContext))
                                .columnMappings(defaultColumnMappings)
                                .tuningConfig(MSQTuningConfig.defaultConfig())
                                .build())
                     .verifyResults();
  }

  @Test
  public void testSuccessInNonStrictMode()
  {
    final Map<String, Object> userContext = ImmutableMap.of(MultiStageQueryContext.CTX_MSQ_MODE, "nonStrict");

    testSelectQuery().setSql("SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [\"" + toRead.getAbsolutePath() + "\"],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1")
                     .setQueryContext(
                         ImmutableMap.<String, Object>builder()
                                     .putAll(DEFAULT_MSQ_CONTEXT)
                                     .putAll(userContext)
                                     .build()
                     )
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
                     .setExpectedMSQSpec(
                         MSQSpec.builder()
                                .query(defaultQuery.withOverriddenContext(userContext))
                                .columnMappings(defaultColumnMappings)
                                .tuningConfig(MSQTuningConfig.defaultConfig())
                                .build())
                     .verifyResults();
  }


  @Test
  public void testFailureInStrictMode()
  {
    testSelectQuery().setSql("SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [\"" + toRead.getAbsolutePath() + "\"],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1")
                     .setQueryContext(ImmutableMap.of(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, 0))
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
                     .setExpectedMSQSpec(
                         MSQSpec.builder()
                                .query(defaultQuery)
                                .columnMappings(defaultColumnMappings)
                                .tuningConfig(MSQTuningConfig.defaultConfig())
                                .build())
                     .setExpectedMSQFaultClass(CannotParseExternalDataFault.class)
                     .verifyResults();
  }

  @Test
  public void testDefaultStrictMode()
  {
    testIngestQuery().setSql(" insert into foo1 SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + toReadFileNameAsJson + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1  PARTITIONED by day ")
                     .setQueryContext(ROLLUP_CONTEXT)
                     .setExpectedRollUp(true)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .addExpectedAggregatorFactory(new LongSumAggregatorFactory("cnt", "cnt"))
                     .setExpectedMSQFaultClass(CannotParseExternalDataFault.class)
                     .verifyResults();
  }

  @Test
  public void testControllerTemporaryFileCleanup()
  {
    testIngestQuery().setSql(" insert into foo1 SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + toReadFileNameAsJson + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1  PARTITIONED by day ")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedMSQFaultClass(CannotParseExternalDataFault.class)
                     .verifyResults();

    // Temporary directory should not contain any controller-related folders
    Assert.assertEquals(0, localFileStorageDir.listFiles().length);
  }

  @Test
  public void testSuccessWhenModeIsOverridden()
  {
    final Map<String, Object> userContext =
        ImmutableMap.<String, Object>builder()
                    .put(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, -1)
                    .put(MultiStageQueryContext.CTX_MSQ_MODE, "strict")
                    .build();

    testSelectQuery().setSql("SELECT\n"
                             + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                             + "  count(*) as cnt\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [\"" + toRead.getAbsolutePath() + "\"],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                             + "  )\n"
                             + ") group by 1")
                     .setQueryContext(
                         ImmutableMap.<String, Object>builder()
                                     .putAll(DEFAULT_MSQ_CONTEXT)
                                     .putAll(userContext)
                                     .build()
                     )
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(ImmutableList.of(new Object[]{1566172800000L, 10L}))
                     .setExpectedMSQSpec(
                         MSQSpec.builder()
                                .query(defaultQuery.withOverriddenContext(userContext))
                                .columnMappings(defaultColumnMappings)
                                .tuningConfig(MSQTuningConfig.defaultConfig())
                                .build())
                     .verifyResults();
  }
}
