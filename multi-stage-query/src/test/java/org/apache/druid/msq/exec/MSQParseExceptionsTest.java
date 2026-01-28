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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.indexing.LegacyMSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.msq.indexing.error.InvalidNullByteFault;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MSQParseExceptionsTest extends MSQTestBase
{

  @Test
  public void testIngestWithNullByte() throws IOException
  {
    final File toRead = getResourceAsTemporaryFile("/unparseable-null-byte-string.csv");
    final String toReadAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("agent_category", ColumnType.STRING)
                                            .build();

    testIngestQuery()
        .setSql("INSERT INTO foo1\n"
                + "WITH\n"
                + "kttm_data AS (\n"
                + "SELECT * FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{ \"files\": [" + toReadAsJson + "],\"type\":\"local\"}',\n"
                + "    '{\"type\":\"csv\", \"findColumnsFromHeader\":true}',\n"
                + "    '[{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"agent_category\",\"type\":\"string\"},{\"name\":\"agent_type\",\"type\":\"string\"},{\"name\":\"browser\",\"type\":\"string\"}]'\n"
                + "  )\n"
                + "))\n"
                + "\n"
                + "SELECT\n"
                + "  FLOOR(TIME_PARSE(\"timestamp\") TO MINUTE) AS __time,\n"
                + "  \"agent_category\"\n"
                + "FROM kttm_data\n"
                + "PARTITIONED BY ALL")
        .setExpectedRowSignature(rowSignature)
        .setExpectedDataSource("foo1")
        .setExpectedMSQFault(
            new InvalidNullByteFault(
                StringUtils.format(
                    "external[LocalInputSource{baseDir=\"null\", filter=null, files=[%s]}]",
                    toRead.getAbsolutePath()
                ),
                1,
                "agent_category",
                "Personal computer\u0000",
                17
            )
        )
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .verifyResults();
  }

  @Test
  public void testIngestWithNullByteInSqlExpression()
  {

    RowSignature rowSignature = RowSignature.builder()
                                            .add("desc", ColumnType.STRING)
                                            .add("text", ColumnType.STRING)
                                            .build();

    testIngestQuery()
        .setSql(""
                + "WITH \"ext\" AS (SELECT *\n"
                + "FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{\"type\":\"inline\",\"data\":\"{\\\"desc\\\":\\\"Row with NULL\\\",\\\"text\\\":\\\"There is a null in\\\\u0000 here somewhere\\\"}\\n\"}',\n"
                + "    '{\"type\":\"json\"}'\n"
                + "  )\n"
                + ") EXTEND (\"desc\" VARCHAR, \"text\" VARCHAR))\n"
                + "SELECT\n"
                + "  \"desc\",\n"
                + "  REPLACE(\"text\", 'a', 'A') AS \"text\"\n"
                + "FROM \"ext\"\n"
                + "")
        .setExpectedRowSignature(rowSignature)
        .setExpectedDataSource("foo1")
        .setExpectedMSQFault(
            new InvalidNullByteFault(
                "external[InlineInputSource{data='{\"desc\":\"Row with NULL\",\"text\":\"There is a null in\\u0000 here somewhere\"}\n'}]",
                1,
                "text",
                "There is A null in\u0000 here somewhere",
                18
            )
        )
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .verifyResults();
  }

  @Test
  public void testIngestWithSanitizedNullByte() throws IOException
  {
    final File toRead = getResourceAsTemporaryFile("/unparseable-null-byte-string.csv");
    final String toReadAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("agent_category", ColumnType.STRING)
                                            .build();

    Map<String, Object> context = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    context.put("sqlInsertSegmentGranularity", "{\"type\":\"all\"}");

    final ScanQuery expectedQuery =
        newScanQueryBuilder()
            .dataSource(
                new ExternalDataSource(
                    new LocalInputSource(null, null, ImmutableList.of(toRead), SystemFields.none()),
                    new CsvInputFormat(null, null, null, true, 0, null),
                    RowSignature.builder()
                                .add("timestamp", ColumnType.STRING)
                                .add("agent_category", ColumnType.STRING)
                                .add("agent_type", ColumnType.STRING)
                                .add("browser", ColumnType.STRING)
                                .build()
                )
            )
            .intervals(querySegmentSpec(Filtration.eternity()))
            .virtualColumns(
                expressionVirtualColumn(
                    "v0",
                    "timestamp_floor(timestamp_parse(\"timestamp\",null,'UTC'),'PT1M',null,'UTC')",
                    ColumnType.LONG
                ),
                expressionVirtualColumn(
                    "v1",
                    "replace(\"agent_category\",'\\u0000','')",
                    ColumnType.STRING
                )
            )
            .columns("v0", "v1")
            .columnTypes(ColumnType.LONG, ColumnType.STRING)
            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
            .context(context)
            .build();


    testIngestQuery()
        .setSql("INSERT INTO foo1\n"
                + "WITH\n"
                + "kttm_data AS (\n"
                + "SELECT * FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{ \"files\": [" + toReadAsJson + "],\"type\":\"local\"}',\n"
                + "    '{\"type\":\"csv\", \"findColumnsFromHeader\":true}',\n"
                + "    '[{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"agent_category\",\"type\":\"string\"},{\"name\":\"agent_type\",\"type\":\"string\"},{\"name\":\"browser\",\"type\":\"string\"}]'\n"
                + "  )\n"
                + "))\n"
                + "\n"
                + "SELECT\n"
                + "  FLOOR(TIME_PARSE(\"timestamp\") TO MINUTE) AS __time,\n"
                + "  REPLACE(\"agent_category\", U&'\\0000', '') as \"agent_category\"\n"
                + "FROM kttm_data\n"
                + "PARTITIONED BY ALL")
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1566691200000L, "Personal computer"},
            new Object[]{1566691200000L, "Personal computer"},
            new Object[]{1566691200000L, "Smartphone"}
        ))
        .setExpectedDataSource("foo1")
        .setExpectedMSQSpec(
            LegacyMSQSpec
                .builder()
                .query(expectedQuery)
                .columnMappings(new ColumnMappings(
                    ImmutableList.of(
                        new ColumnMapping("v0", "__time"),
                        new ColumnMapping("v1", "agent_category")
                    )
                ))
                .destination(new DataSourceMSQDestination("foo1", Granularities.ALL, null, null, null, null, null))
                .tuningConfig(MSQTuningConfig.defaultConfig())
                .build())
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .verifyResults();
  }

  @Test
  public void testIngestWithSanitizedNullByteUsingContextParameter() throws IOException
  {
    final File toRead = getResourceAsTemporaryFile("/unparseable-null-byte-string.csv");
    final String toReadAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("agent_category", ColumnType.STRING)
                                            .build();

    Map<String, Object> context = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    context.put("sqlInsertSegmentGranularity", "{\"type\":\"all\"}");
    context.put(MultiStageQueryContext.CTX_REMOVE_NULL_BYTES, true);

    Map<String, Object> runtimeContext = new HashMap<>(DEFAULT_MSQ_CONTEXT);
    runtimeContext.put(MultiStageQueryContext.CTX_REMOVE_NULL_BYTES, true);

    final ScanQuery expectedQuery =
        newScanQueryBuilder()
            .dataSource(
                new ExternalDataSource(
                    new LocalInputSource(null, null, ImmutableList.of(toRead), SystemFields.none()),
                    new CsvInputFormat(null, null, null, true, 0, null),
                    RowSignature.builder()
                                .add("timestamp", ColumnType.STRING)
                                .add("agent_category", ColumnType.STRING)
                                .add("agent_type", ColumnType.STRING)
                                .add("browser", ColumnType.STRING)
                                .build()
                )
            )
            .intervals(querySegmentSpec(Filtration.eternity()))
            .virtualColumns(
                expressionVirtualColumn(
                    "v0",
                    "timestamp_floor(timestamp_parse(\"timestamp\",null,'UTC'),'PT1M',null,'UTC')",
                    ColumnType.LONG
                )
            )
            .columns("v0", "agent_category")
            .columnTypes(ColumnType.LONG, ColumnType.STRING)
            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
            .context(context)
            .build();


    testIngestQuery()
        .setSql("INSERT INTO foo1\n"
                + "WITH\n"
                + "kttm_data AS (\n"
                + "SELECT * FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{ \"files\": [" + toReadAsJson + "],\"type\":\"local\"}',\n"
                + "    '{\"type\":\"csv\", \"findColumnsFromHeader\":true}',\n"
                + "    '[{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"agent_category\",\"type\":\"string\"},{\"name\":\"agent_type\",\"type\":\"string\"},{\"name\":\"browser\",\"type\":\"string\"}]'\n"
                + "  )\n"
                + "))\n"
                + "\n"
                + "SELECT\n"
                + "  FLOOR(TIME_PARSE(\"timestamp\") TO MINUTE) AS __time,\n"
                + "  agent_category\n"
                + "FROM kttm_data\n"
                + "PARTITIONED BY ALL")
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1566691200000L, "Personal computer"},
            new Object[]{1566691200000L, "Personal computer"},
            new Object[]{1566691200000L, "Smartphone"}
        ))
        .setExpectedDataSource("foo1")
        .setExpectedMSQSpec(
            LegacyMSQSpec
                .builder()
                .query(expectedQuery)
                .columnMappings(new ColumnMappings(
                    ImmutableList.of(
                        new ColumnMapping("v0", "__time"),
                        new ColumnMapping("agent_category", "agent_category")
                    )
                ))
                .destination(new DataSourceMSQDestination("foo1", Granularities.ALL, null, null, null, null, null))
                .tuningConfig(MSQTuningConfig.defaultConfig())
                .build())
        .setQueryContext(runtimeContext)
        .verifyResults();
  }

  @Test
  public void testCannotParseJson() throws IOException
  {
    final File toRead = getResourceAsTemporaryFile("/not-json.txt");
    final String toReadAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("language", ColumnType.STRING_ARRAY)
                                            .build();

    final GroupByQuery expectedQuery =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("__time", "d0", ColumnType.LONG)))
                    .build();


    testSelectQuery()
        .setSql("WITH\n"
                + "ext AS (\n"
                + "SELECT * FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{ \"files\": [" + toReadAsJson + "],\"type\":\"local\"}',\n"
                + "    '{\"type\":\"json\"}',\n"
                + "    '[{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"thisRow\",\"type\":\"string\"}]'\n"
                + "  )\n"
                + "))\n"
                + "\n"
                + "SELECT\n"
                + "  TIME_PARSE(\"timestamp\") AS __time,\n"
                + "  thisRow\n"
                + "FROM ext")
        .setExpectedRowSignature(rowSignature)
        .setExpectedMSQSpec(
            LegacyMSQSpec
                .builder()
                .query(expectedQuery)
                .columnMappings(new ColumnMappings(
                    ImmutableList.of(
                        new ColumnMapping("d0", "__time"),
                        new ColumnMapping("a0", "cnt")
                    )
                ))
                .tuningConfig(MSQTuningConfig.defaultConfig())
                .build())
        .setExpectedMSQFault(
            new CannotParseExternalDataFault(
                "Unable to parse row [this row is not json] "
                + "(Path: file:" + toRead.getAbsolutePath() + ", Record: 3, Line: 3)"
            )
        )
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .verifyResults();
  }
}
