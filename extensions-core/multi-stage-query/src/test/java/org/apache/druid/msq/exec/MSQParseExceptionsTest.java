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
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.msq.indexing.error.InvalidNullByteFault;
import org.apache.druid.msq.querykit.scan.ExternalColumnSelectorFactory;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.SimpleAscendingOffset;
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
                    "external input source: LocalInputSource{baseDir=\"null\", filter=null, files=[%s]}",
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
                "external input source: InlineInputSource{data='{\"desc\":\"Row with NULL\",\"text\":\"There is a null in\\u0000 here somewhere\"}\n'}",
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
                    new CsvInputFormat(null, null, null, true, 0),
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
            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
            .context(defaultScanQueryContext(
                context,
                RowSignature.builder()
                            .add("v0", ColumnType.LONG)
                            .add("v1", ColumnType.STRING)
                            .build()
            ))
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
            MSQSpec
                .builder()
                .query(expectedQuery)
                .columnMappings(new ColumnMappings(
                    ImmutableList.of(
                        new ColumnMapping("v0", "__time"),
                        new ColumnMapping("v1", "agent_category")
                    )
                ))
                .destination(new DataSourceMSQDestination("foo1", Granularities.ALL, null, null))
                .tuningConfig(MSQTuningConfig.defaultConfig())
                .build())
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .verifyResults();
  }

  @Test
  public void testMultiValueStringWithIncorrectType() throws IOException
  {
    final File toRead = getResourceAsTemporaryFile("/unparseable-mv-string-array.json");
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
                + "kttm_data AS (\n"
                + "SELECT * FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{ \"files\": [" + toReadAsJson + "],\"type\":\"local\"}',\n"
                + "    '{\"type\":\"json\"}',\n"
                + "    '[{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"agent_category\",\"type\":\"string\"},{\"name\":\"agent_type\",\"type\":\"string\"},{\"name\":\"browser\",\"type\":\"string\"},{\"name\":\"browser_version\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"continent\",\"type\":\"string\"},{\"name\":\"country\",\"type\":\"string\"},{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"event_subtype\",\"type\":\"string\"},{\"name\":\"loaded_image\",\"type\":\"string\"},{\"name\":\"adblock_list\",\"type\":\"string\"},{\"name\":\"forwarded_for\",\"type\":\"string\"},{\"name\":\"language\",\"type\":\"string\"},{\"name\":\"number\",\"type\":\"long\"},{\"name\":\"os\",\"type\":\"string\"},{\"name\":\"path\",\"type\":\"string\"},{\"name\":\"platform\",\"type\":\"string\"},{\"name\":\"referrer\",\"type\":\"string\"},{\"name\":\"referrer_host\",\"type\":\"string\"},{\"name\":\"region\",\"type\":\"string\"},{\"name\":\"remote_address\",\"type\":\"string\"},{\"name\":\"screen\",\"type\":\"string\"},{\"name\":\"session\",\"type\":\"string\"},{\"name\":\"session_length\",\"type\":\"long\"},{\"name\":\"timezone\",\"type\":\"string\"},{\"name\":\"timezone_offset\",\"type\":\"long\"},{\"name\":\"window\",\"type\":\"string\"}]'\n"
                + "  )\n"
                + "))\n"
                + "\n"
                + "SELECT\n"
                + "  FLOOR(TIME_PARSE(\"timestamp\") TO MINUTE) AS __time,\n"
                + "  MV_TO_ARRAY(\"language\") AS \"language\"\n"
                + "FROM kttm_data")
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1566691200000L, ImmutableList.of("en")},
            new Object[]{1566691200000L, ImmutableList.of("en", "es", "es-419", "es-MX")},
            new Object[]{1566691200000L, ImmutableList.of("en", "es", "es-419", "es-US")}
        ))
        .setExpectedMSQSpec(
            MSQSpec
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
                ExternalColumnSelectorFactory
                    .createException(
                        new Exception("dummy"),
                        "v1",
                        new LocalInputSource(null, null, ImmutableList.of(toRead), SystemFields.none()),
                        new SimpleAscendingOffset(Integer.MAX_VALUE)
                    )
                    .getMessage()
            )
        )
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .verifyResults();
  }
}
