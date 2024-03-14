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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.Query;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CompressionUtils;
import org.hamcrest.CoreMatchers;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests INSERT and SELECT behaviour of MSQ with arrays and MVDs
 */
public class MSQArraysTest extends MSQTestBase
{
  private String dataFileNameJsonString;
  private String dataFileSignatureJsonString;
  private DataSource dataFileExternalDataSource;

  public static Collection<Object[]> data()
  {
    Object[][] data = new Object[][]{
        {DEFAULT, DEFAULT_MSQ_CONTEXT},
        {DURABLE_STORAGE, DURABLE_STORAGE_MSQ_CONTEXT},
        {FAULT_TOLERANCE, FAULT_TOLERANCE_MSQ_CONTEXT},
        {PARALLEL_MERGE, PARALLEL_MERGE_MSQ_CONTEXT}
    };
    return Arrays.asList(data);
  }

  @BeforeEach
  public void setup() throws IOException
  {
    // Read the file and make the name available to the tests
    File dataFile = newTempFile("dataFile");
    final InputStream resourceStream = NestedDataTestUtils.class.getClassLoader()
                                                                .getResourceAsStream(NestedDataTestUtils.ARRAY_TYPES_DATA_FILE);
    final InputStream decompressing = CompressionUtils.decompress(
        resourceStream,
        NestedDataTestUtils.ARRAY_TYPES_DATA_FILE
    );
    Files.copy(decompressing, dataFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    decompressing.close();

    dataFileNameJsonString = queryFramework().queryJsonMapper().writeValueAsString(dataFile);

    RowSignature dataFileSignature = RowSignature.builder()
                                                 .add("timestamp", ColumnType.STRING)
                                                 .add("arrayString", ColumnType.STRING_ARRAY)
                                                 .add("arrayStringNulls", ColumnType.STRING_ARRAY)
                                                 .add("arrayLong", ColumnType.LONG_ARRAY)
                                                 .add("arrayLongNulls", ColumnType.LONG_ARRAY)
                                                 .add("arrayDouble", ColumnType.DOUBLE_ARRAY)
                                                 .add("arrayDoubleNulls", ColumnType.DOUBLE_ARRAY)
                                                 .build();
    dataFileSignatureJsonString = queryFramework().queryJsonMapper().writeValueAsString(dataFileSignature);

    dataFileExternalDataSource = new ExternalDataSource(
        new LocalInputSource(null, null, ImmutableList.of(dataFile), SystemFields.none()),
        new JsonInputFormat(null, null, null, null, null),
        dataFileSignature
    );
  }

  /**
   * Tests the behaviour of INSERT query when arrayIngestMode is set to none (default) and the user tries to ingest
   * string arrays
   */
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testInsertStringArrayWithArrayIngestModeNone(String contextName, Map<String, Object> context)
  {

    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(MultiStageQueryContext.CTX_ARRAY_INGEST_MODE, "none");

    testIngestQuery().setSql(
                         "INSERT INTO foo1 SELECT MV_TO_ARRAY(dim3) AS dim3 FROM foo GROUP BY 1 PARTITIONED BY ALL TIME")
                     .setQueryContext(adjustedContext)
                     .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
                         CoreMatchers.instanceOf(ISE.class),
                         ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                             "String arrays can not be ingested when 'arrayIngestMode' is set to 'none'"))
                     ))
                     .verifyExecutionError();
  }

  /**
   * Tests the behaviour of INSERT query when arrayIngestMode is set to none (default) and the user tries to ingest
   * string arrays
   */
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testReplaceMvdWithStringArray(String contextName, Map<String, Object> context)
  {
    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(MultiStageQueryContext.CTX_ARRAY_INGEST_MODE, "array");

    testIngestQuery()
        .setSql(
            "REPLACE INTO foo OVERWRITE ALL\n"
            + "SELECT MV_TO_ARRAY(dim3) AS dim3 FROM foo\n"
            + "PARTITIONED BY ALL TIME"
        )
        .setQueryContext(adjustedContext)
        .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(DruidException.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                "Cannot write into field[dim3] using type[VARCHAR ARRAY] and arrayIngestMode[array], "
                + "since the existing type is[VARCHAR]"))
        ))
        .verifyExecutionError();
  }

  /**
   * Tests the behaviour of INSERT query when arrayIngestMode is set to none (default) and the user tries to ingest
   * string arrays
   */
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testReplaceStringArrayWithMvdInArrayMode(String contextName, Map<String, Object> context)
  {
    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(MultiStageQueryContext.CTX_ARRAY_INGEST_MODE, "array");

    testIngestQuery()
        .setSql(
            "REPLACE INTO arrays OVERWRITE ALL\n"
            + "SELECT ARRAY_TO_MV(arrayString) AS arrayString FROM arrays\n"
            + "PARTITIONED BY ALL TIME"
        )
        .setQueryContext(adjustedContext)
        .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(DruidException.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                "Cannot write into field[arrayString] using type[VARCHAR] and arrayIngestMode[array], since the "
                + "existing type is[VARCHAR ARRAY]. Try adjusting your query to make this column an ARRAY instead "
                + "of VARCHAR."))
        ))
        .verifyExecutionError();
  }

  /**
   * Tests the behaviour of INSERT query when arrayIngestMode is set to none (default) and the user tries to ingest
   * string arrays
   */
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testReplaceStringArrayWithMvdInMvdMode(String contextName, Map<String, Object> context)
  {
    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(MultiStageQueryContext.CTX_ARRAY_INGEST_MODE, "mvd");

    testIngestQuery()
        .setSql(
            "REPLACE INTO arrays OVERWRITE ALL\n"
            + "SELECT ARRAY_TO_MV(arrayString) AS arrayString FROM arrays\n"
            + "PARTITIONED BY ALL TIME"
        )
        .setQueryContext(adjustedContext)
        .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(DruidException.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                "Cannot write into field[arrayString] using type[VARCHAR] and arrayIngestMode[mvd], since the "
                + "existing type is[VARCHAR ARRAY]. Try setting arrayIngestMode to[array] and adjusting your query to "
                + "make this column an ARRAY instead of VARCHAR."))
        ))
        .verifyExecutionError();
  }

  /**
   * Tests the behaviour of INSERT query when arrayIngestMode is set to none (default) and the user tries to ingest
   * string arrays
   */
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testReplaceMvdWithStringArraySkipValidation(String contextName, Map<String, Object> context)
  {
    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(MultiStageQueryContext.CTX_ARRAY_INGEST_MODE, "array");
    adjustedContext.put(MultiStageQueryContext.CTX_SKIP_TYPE_VERIFICATION, "dim3");

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim3", ColumnType.STRING_ARRAY)
                                            .build();

    testIngestQuery()
        .setSql(
            "REPLACE INTO foo OVERWRITE ALL\n"
            + "SELECT MV_TO_ARRAY(dim3) AS dim3 FROM foo\n"
            + "PARTITIONED BY ALL TIME"
        )
        .setQueryContext(adjustedContext)
        .setExpectedDataSource("foo")
        .setExpectedRowSignature(rowSignature)
        .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo", Intervals.ETERNITY, "test", 0)))
        .setExpectedResultRows(
            NullHandling.sqlCompatible()
            ? ImmutableList.of(
                new Object[]{0L, null},
                new Object[]{0L, null},
                new Object[]{0L, new Object[]{"a", "b"}},
                new Object[]{0L, new Object[]{""}},
                new Object[]{0L, new Object[]{"b", "c"}},
                new Object[]{0L, new Object[]{"d"}}
            )
            : ImmutableList.of(
                new Object[]{0L, null},
                new Object[]{0L, null},
                new Object[]{0L, null},
                new Object[]{0L, new Object[]{"a", "b"}},
                new Object[]{0L, new Object[]{"b", "c"}},
                new Object[]{0L, new Object[]{"d"}}
            )
        )
        .verifyResults();
  }

  /**
   * Tests the behaviour of INSERT query when arrayIngestMode is set to none (default) and the user tries to ingest
   * string arrays
   */
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testReplaceMvdWithMvd(String contextName, Map<String, Object> context)
  {
    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(MultiStageQueryContext.CTX_ARRAY_INGEST_MODE, "array");

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim3", ColumnType.STRING)
                                            .build();

    testIngestQuery()
        .setSql(
            "REPLACE INTO foo OVERWRITE ALL\n"
            + "SELECT dim3 FROM foo\n"
            + "PARTITIONED BY ALL TIME"
        )
        .setQueryContext(adjustedContext)
        .setExpectedDataSource("foo")
        .setExpectedRowSignature(rowSignature)
        .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo", Intervals.ETERNITY, "test", 0)))
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{0L, null},
                new Object[]{0L, null},
                new Object[]{0L, NullHandling.sqlCompatible() ? "" : null},
                new Object[]{0L, ImmutableList.of("a", "b")},
                new Object[]{0L, ImmutableList.of("b", "c")},
                new Object[]{0L, "d"}
            )
        )
        .verifyResults();
  }

  /**
   * Tests the behaviour of INSERT query when arrayIngestMode is set to mvd (default) and the only array type to be
   * ingested is string array
   */
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testInsertOnFoo1WithMultiValueToArrayGroupByWithDefaultContext(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("dim3", ColumnType.STRING)
                                            .build();

    testIngestQuery().setSql(
                         "INSERT INTO foo1 SELECT MV_TO_ARRAY(dim3) AS dim3 FROM foo GROUP BY 1 PARTITIONED BY ALL TIME")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .setExpectedResultRows(expectedMultiValueFooRowsToArray())
                     .verifyResults();
  }

  /**
   * Tests the INSERT query when 'auto' type is set
   */
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testInsertArraysAutoType(String contextName, Map<String, Object> context)
  {
    List<Object[]> expectedRows = Arrays.asList(
        new Object[]{1672531200000L, null, null, null},
        new Object[]{1672531200000L, null, new Object[]{1L, 2L, 3L}, new Object[]{1.1, 2.2, 3.3}},
        new Object[]{1672531200000L, new Object[]{"d", "e"}, new Object[]{1L, 4L}, new Object[]{2.2, 3.3, 4.0}},
        new Object[]{1672531200000L, new Object[]{"a", "b"}, null, null},
        new Object[]{1672531200000L, new Object[]{"a", "b"}, new Object[]{1L, 2L, 3L}, new Object[]{1.1, 2.2, 3.3}},
        new Object[]{1672531200000L, new Object[]{"b", "c"}, new Object[]{1L, 2L, 3L, 4L}, new Object[]{1.1, 3.3}},
        new Object[]{1672531200000L, new Object[]{"a", "b", "c"}, new Object[]{2L, 3L}, new Object[]{3.3, 4.4, 5.5}},
        new Object[]{1672617600000L, null, null, null},
        new Object[]{1672617600000L, null, new Object[]{1L, 2L, 3L}, new Object[]{1.1, 2.2, 3.3}},
        new Object[]{1672617600000L, new Object[]{"d", "e"}, new Object[]{1L, 4L}, new Object[]{2.2, 3.3, 4.0}},
        new Object[]{1672617600000L, new Object[]{"a", "b"}, null, null},
        new Object[]{1672617600000L, new Object[]{"a", "b"}, new Object[]{1L, 2L, 3L}, new Object[]{1.1, 2.2, 3.3}},
        new Object[]{1672617600000L, new Object[]{"b", "c"}, new Object[]{1L, 2L, 3L, 4L}, new Object[]{1.1, 3.3}},
        new Object[]{1672617600000L, new Object[]{"a", "b", "c"}, new Object[]{2L, 3L}, new Object[]{3.3, 4.4, 5.5}}
    );

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("arrayString", ColumnType.STRING_ARRAY)
                                            .add("arrayLong", ColumnType.LONG_ARRAY)
                                            .add("arrayDouble", ColumnType.DOUBLE_ARRAY)
                                            .build();

    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(MultiStageQueryContext.CTX_USE_AUTO_SCHEMAS, true);

    testIngestQuery().setSql(" INSERT INTO foo1 SELECT\n"
                             + "  TIME_PARSE(\"timestamp\") as __time,\n"
                             + "  arrayString,\n"
                             + "  arrayLong,\n"
                             + "  arrayDouble\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"timestamp\", \"type\": \"STRING\"}, {\"name\": \"arrayString\", \"type\": \"COMPLEX<json>\"}, {\"name\": \"arrayLong\", \"type\": \"COMPLEX<json>\"}, {\"name\": \"arrayDouble\", \"type\": \"COMPLEX<json>\"}]'\n"
                             + "  )\n"
                             + ") PARTITIONED BY ALL")
                     .setQueryContext(adjustedContext)
                     .setExpectedResultRows(expectedRows)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .verifyResults();
  }

  /**
   * Tests the behaviour of INSERT query when arrayIngestMode is set to mvd and the user tries to ingest numeric array
   * types as well
   */
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testInsertArraysWithStringArraysAsMVDs(String contextName, Map<String, Object> context)
  {
    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(MultiStageQueryContext.CTX_ARRAY_INGEST_MODE, "mvd");

    testIngestQuery().setSql(" INSERT INTO foo1 SELECT\n"
                             + "  TIME_PARSE(\"timestamp\") as __time,\n"
                             + "  arrayString,\n"
                             + "  arrayStringNulls,\n"
                             + "  arrayLong,\n"
                             + "  arrayLongNulls,\n"
                             + "  arrayDouble,\n"
                             + "  arrayDoubleNulls\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '" + dataFileSignatureJsonString + "'\n"
                             + "  )\n"
                             + ") PARTITIONED BY ALL")
                     .setQueryContext(adjustedContext)
                     .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
                         CoreMatchers.instanceOf(ISE.class),
                         ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                             "Numeric arrays can only be ingested when"))
                     ))
                     .verifyExecutionError();
  }

  /**
   * Tests the behaviour of INSERT query when arrayIngestMode is set to array and the user tries to ingest all
   * array types
   */
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testInsertArraysAsArrays(String contextName, Map<String, Object> context)
  {
    final List<Object[]> expectedRows = Arrays.asList(
        new Object[]{
            1672531200000L,
            null,
            null,
            new Object[]{1L, 2L, 3L},
            new Object[]{},
            new Object[]{1.1d, 2.2d, 3.3d},
            null
        },
        new Object[]{
            1672531200000L,
            null,
            new Object[]{"a", "b"},
            null,
            new Object[]{2L, 3L},
            null,
            new Object[]{null}
        },
        new Object[]{
            1672531200000L,
            new Object[]{"d", "e"},
            new Object[]{"b", "b"},
            new Object[]{1L, 4L},
            new Object[]{1L},
            new Object[]{2.2d, 3.3d, 4.0d},
            null
        },
        new Object[]{
            1672531200000L,
            new Object[]{"a", "b"},
            null,
            null,
            new Object[]{null, 2L, 9L},
            null,
            new Object[]{999.0d, 5.5d, null}
        },
        new Object[]{
            1672531200000L,
            new Object[]{"a", "b"},
            new Object[]{"a", "b"},
            new Object[]{1L, 2L, 3L},
            new Object[]{1L, null, 3L},
            new Object[]{1.1d, 2.2d, 3.3d},
            new Object[]{1.1d, 2.2d, null}
        },
        new Object[]{
            1672531200000L,
            new Object[]{"b", "c"},
            new Object[]{"d", null, "b"},
            new Object[]{1L, 2L, 3L, 4L},
            new Object[]{1L, 2L, 3L},
            new Object[]{1.1d, 3.3d},
            new Object[]{null, 2.2d, null}
        },
        new Object[]{
            1672531200000L,
            new Object[]{"a", "b", "c"},
            new Object[]{null, "b"},
            new Object[]{2L, 3L},
            null,
            new Object[]{3.3d, 4.4d, 5.5d},
            new Object[]{999.0d, null, 5.5d}
        },
        new Object[]{
            1672617600000L,
            null,
            null,
            new Object[]{1L, 2L, 3L},
            null,
            new Object[]{1.1d, 2.2d, 3.3d},
            new Object[]{}
        },
        new Object[]{
            1672617600000L,
            null,
            new Object[]{"a", "b"},
            null,
            new Object[]{2L, 3L},
            null,
            new Object[]{null, 1.1d}
        },
        new Object[]{
            1672617600000L,
            new Object[]{"d", "e"},
            new Object[]{"b", "b"},
            new Object[]{1L, 4L},
            new Object[]{null},
            new Object[]{2.2d, 3.3d, 4.0},
            null
        },
        new Object[]{
            1672617600000L,
            new Object[]{"a", "b"},
            new Object[]{null},
            null,
            new Object[]{null, 2L, 9L},
            null,
            new Object[]{999.0d, 5.5d, null}
        },
        new Object[]{
            1672617600000L,
            new Object[]{"a", "b"},
            new Object[]{},
            new Object[]{1L, 2L, 3L},
            new Object[]{1L, null, 3L},
            new Object[]{1.1d, 2.2d, 3.3d},
            new Object[]{1.1d, 2.2d, null}
        },
        new Object[]{
            1672617600000L,
            new Object[]{"b", "c"},
            new Object[]{"d", null, "b"},
            new Object[]{1L, 2L, 3L, 4L},
            new Object[]{1L, 2L, 3L},
            new Object[]{1.1d, 3.3d},
            new Object[]{null, 2.2d, null}
        },
        new Object[]{
            1672617600000L,
            new Object[]{"a", "b", "c"},
            new Object[]{null, "b"},
            new Object[]{2L, 3L},
            null,
            new Object[]{3.3d, 4.4d, 5.5d},
            new Object[]{999.0d, null, 5.5d}
        }
    );

    RowSignature rowSignatureWithoutTimeColumn =
        RowSignature.builder()
                    .add("arrayString", ColumnType.STRING_ARRAY)
                    .add("arrayStringNulls", ColumnType.STRING_ARRAY)
                    .add("arrayLong", ColumnType.LONG_ARRAY)
                    .add("arrayLongNulls", ColumnType.LONG_ARRAY)
                    .add("arrayDouble", ColumnType.DOUBLE_ARRAY)
                    .add("arrayDoubleNulls", ColumnType.DOUBLE_ARRAY)
                    .build();

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .addAll(rowSignatureWithoutTimeColumn)
                                            .build();

    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(MultiStageQueryContext.CTX_ARRAY_INGEST_MODE, "array");

    testIngestQuery().setSql(" INSERT INTO foo1 SELECT\n"
                             + "  TIME_PARSE(\"timestamp\") as __time,\n"
                             + "  arrayString,\n"
                             + "  arrayStringNulls,\n"
                             + "  arrayLong,\n"
                             + "  arrayLongNulls,\n"
                             + "  arrayDouble,\n"
                             + "  arrayDoubleNulls\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '" + dataFileSignatureJsonString + "'\n"
                             + "  )\n"
                             + ") PARTITIONED BY ALL")
                     .setQueryContext(adjustedContext)
                     .setExpectedResultRows(expectedRows)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectOnArraysWithArrayIngestModeAsNone(String contextName, Map<String, Object> context)
  {
    testSelectOnArrays(contextName, context, "none");
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectOnArraysWithArrayIngestModeAsMVD(String contextName, Map<String, Object> context)
  {
    testSelectOnArrays(contextName, context, "mvd");
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectOnArraysWithArrayIngestModeAsArray(String contextName, Map<String, Object> context)
  {
    testSelectOnArrays(contextName, context, "array");
  }

  // Tests the behaviour of the select with the given arrayIngestMode. The expectation should be the same, since the
  // arrayIngestMode should only determine how the array gets ingested at the end.
  public void testSelectOnArrays(String contextName, Map<String, Object> context, String arrayIngestMode)
  {
    final List<Object[]> expectedRows = Arrays.asList(
        new Object[]{
            1672531200000L,
            Arrays.asList("a", "b"),
            Arrays.asList("a", "b"),
            Arrays.asList(1L, 2L, 3L),
            Arrays.asList(1L, null, 3L),
            Arrays.asList(1.1d, 2.2d, 3.3d),
            Arrays.asList(1.1d, 2.2d, null)
        },
        new Object[]{
            1672531200000L,
            Arrays.asList("a", "b", "c"),
            Arrays.asList(null, "b"),
            Arrays.asList(2L, 3L),
            null,
            Arrays.asList(3.3d, 4.4d, 5.5d),
            Arrays.asList(999.0d, null, 5.5d),
            },
        new Object[]{
            1672531200000L,
            Arrays.asList("b", "c"),
            Arrays.asList("d", null, "b"),
            Arrays.asList(1L, 2L, 3L, 4L),
            Arrays.asList(1L, 2L, 3L),
            Arrays.asList(1.1d, 3.3d),
            Arrays.asList(null, 2.2d, null)
        },
        new Object[]{
            1672531200000L,
            Arrays.asList("d", "e"),
            Arrays.asList("b", "b"),
            Arrays.asList(1L, 4L),
            Collections.singletonList(1L),
            Arrays.asList(2.2d, 3.3d, 4.0d),
            null
        },
        new Object[]{
            1672531200000L,
            null,
            null,
            Arrays.asList(1L, 2L, 3L),
            Collections.emptyList(),
            Arrays.asList(1.1d, 2.2d, 3.3d),
            null
        },
        new Object[]{
            1672531200000L,
            Arrays.asList("a", "b"),
            null,
            null,
            Arrays.asList(null, 2L, 9L),
            null,
            Arrays.asList(999.0d, 5.5d, null)
        },
        new Object[]{
            1672531200000L,
            null,
            Arrays.asList("a", "b"),
            null,
            Arrays.asList(2L, 3L),
            null,
            Collections.singletonList(null)
        },
        new Object[]{
            1672617600000L,
            Arrays.asList("a", "b"),
            Collections.emptyList(),
            Arrays.asList(1L, 2L, 3L),
            Arrays.asList(1L, null, 3L),
            Arrays.asList(1.1d, 2.2d, 3.3d),
            Arrays.asList(1.1d, 2.2d, null)
        },
        new Object[]{
            1672617600000L,
            Arrays.asList("a", "b", "c"),
            Arrays.asList(null, "b"),
            Arrays.asList(2L, 3L),
            null,
            Arrays.asList(3.3d, 4.4d, 5.5d),
            Arrays.asList(999.0d, null, 5.5d)
        },
        new Object[]{
            1672617600000L,
            Arrays.asList("b", "c"),
            Arrays.asList("d", null, "b"),
            Arrays.asList(1L, 2L, 3L, 4L),
            Arrays.asList(1L, 2L, 3L),
            Arrays.asList(1.1d, 3.3d),
            Arrays.asList(null, 2.2d, null)
        },
        new Object[]{
            1672617600000L,
            Arrays.asList("d", "e"),
            Arrays.asList("b", "b"),
            Arrays.asList(1L, 4L),
            Collections.singletonList(null),
            Arrays.asList(2.2d, 3.3d, 4.0),
            null
        },
        new Object[]{
            1672617600000L,
            null,
            null,
            Arrays.asList(1L, 2L, 3L),
            null,
            Arrays.asList(1.1d, 2.2d, 3.3d),
            Collections.emptyList()
        },
        new Object[]{
            1672617600000L,
            Arrays.asList("a", "b"),
            Collections.singletonList(null),
            null,
            Arrays.asList(null, 2L, 9L),
            null,
            Arrays.asList(999.0d, 5.5d, null)
        },
        new Object[]{
            1672617600000L,
            null,
            Arrays.asList("a", "b"),
            null,
            Arrays.asList(2L, 3L),
            null,
            Arrays.asList(null, 1.1d),
            }
    );

    RowSignature rowSignatureWithoutTimeColumn =
        RowSignature.builder()
                    .add("arrayString", ColumnType.STRING_ARRAY)
                    .add("arrayStringNulls", ColumnType.STRING_ARRAY)
                    .add("arrayLong", ColumnType.LONG_ARRAY)
                    .add("arrayLongNulls", ColumnType.LONG_ARRAY)
                    .add("arrayDouble", ColumnType.DOUBLE_ARRAY)
                    .add("arrayDoubleNulls", ColumnType.DOUBLE_ARRAY)
                    .build();

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .addAll(rowSignatureWithoutTimeColumn)
                                            .build();

    RowSignature scanSignature = RowSignature.builder()
                                             .add("arrayDouble", ColumnType.DOUBLE_ARRAY)
                                             .add("arrayDoubleNulls", ColumnType.DOUBLE_ARRAY)
                                             .add("arrayLong", ColumnType.LONG_ARRAY)
                                             .add("arrayLongNulls", ColumnType.LONG_ARRAY)
                                             .add("arrayString", ColumnType.STRING_ARRAY)
                                             .add("arrayStringNulls", ColumnType.STRING_ARRAY)
                                             .add("v0", ColumnType.LONG)
                                             .build();

    final Map<String, Object> adjustedContext = new HashMap<>(context);
    adjustedContext.put(MultiStageQueryContext.CTX_ARRAY_INGEST_MODE, arrayIngestMode);

    Query<?> expectedQuery = newScanQueryBuilder()
        .dataSource(dataFileExternalDataSource)
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns(
            "arrayDouble",
            "arrayDoubleNulls",
            "arrayLong",
            "arrayLongNulls",
            "arrayString",
            "arrayStringNulls",
            "v0"
        )
        .virtualColumns(new ExpressionVirtualColumn(
            "v0",
            "timestamp_parse(\"timestamp\",null,'UTC')",
            ColumnType.LONG,
            TestExprMacroTable.INSTANCE
        ))
        .context(defaultScanQueryContext(adjustedContext, scanSignature))
        .build();

    testSelectQuery().setSql("SELECT\n"
                             + "  TIME_PARSE(\"timestamp\") as __time,\n"
                             + "  arrayString,\n"
                             + "  arrayStringNulls,\n"
                             + "  arrayLong,\n"
                             + "  arrayLongNulls,\n"
                             + "  arrayDouble,\n"
                             + "  arrayDoubleNulls\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '" + dataFileSignatureJsonString + "'\n"
                             + "  )\n"
                             + ")")
                     .setQueryContext(adjustedContext)
                     .setExpectedMSQSpec(MSQSpec
                                             .builder()
                                             .query(expectedQuery)
                                             .columnMappings(new ColumnMappings(ImmutableList.of(
                                                 new ColumnMapping("v0", "__time"),
                                                 new ColumnMapping("arrayString", "arrayString"),
                                                 new ColumnMapping("arrayStringNulls", "arrayStringNulls"),
                                                 new ColumnMapping("arrayLong", "arrayLong"),
                                                 new ColumnMapping("arrayLongNulls", "arrayLongNulls"),
                                                 new ColumnMapping("arrayDouble", "arrayDouble"),
                                                 new ColumnMapping("arrayDoubleNulls", "arrayDoubleNulls")
                                             )))
                                             .tuningConfig(MSQTuningConfig.defaultConfig())
                                             .destination(TaskReportMSQDestination.INSTANCE)
                                             .build()
                     )
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedRows)
                     .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testScanWithOrderByOnStringArray(String contextName, Map<String, Object> context)
  {
    final List<Object[]> expectedRows = Arrays.asList(
        new Object[]{Arrays.asList("d", "e")},
        new Object[]{Arrays.asList("d", "e")},
        new Object[]{Arrays.asList("b", "c")},
        new Object[]{Arrays.asList("b", "c")},
        new Object[]{Arrays.asList("a", "b", "c")},
        new Object[]{Arrays.asList("a", "b", "c")},
        new Object[]{Arrays.asList("a", "b")},
        new Object[]{Arrays.asList("a", "b")},
        new Object[]{Arrays.asList("a", "b")},
        new Object[]{Arrays.asList("a", "b")},
        new Object[]{null},
        new Object[]{null},
        new Object[]{null},
        new Object[]{null}
    );


    RowSignature rowSignature = RowSignature.builder()
                                            .add("arrayString", ColumnType.STRING_ARRAY)
                                            .build();

    RowSignature scanSignature = RowSignature.builder()
                                             .add("arrayString", ColumnType.STRING_ARRAY)
                                             .build();

    Query<?> expectedQuery = newScanQueryBuilder()
        .dataSource(dataFileExternalDataSource)
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("arrayString")
        .orderBy(Collections.singletonList(new ScanQuery.OrderBy("arrayString", ScanQuery.Order.DESCENDING)))
        .context(defaultScanQueryContext(context, scanSignature))
        .build();

    testSelectQuery().setSql("SELECT\n"
                             + "  arrayString\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '" + dataFileSignatureJsonString + "'\n"
                             + "  )\n"
                             + ")\n"
                             + "ORDER BY arrayString DESC")
                     .setQueryContext(context)
                     .setExpectedMSQSpec(MSQSpec
                                             .builder()
                                             .query(expectedQuery)
                                             .columnMappings(new ColumnMappings(ImmutableList.of(
                                                 new ColumnMapping("arrayString", "arrayString")
                                             )))
                                             .tuningConfig(MSQTuningConfig.defaultConfig())
                                             .destination(TaskReportMSQDestination.INSTANCE)
                                             .build()
                     )
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedRows)
                     .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testScanWithOrderByOnLongArray(String contextName, Map<String, Object> context)
  {
    final List<Object[]> expectedRows = Arrays.asList(
        new Object[]{null},
        new Object[]{null},
        new Object[]{null},
        new Object[]{null},
        new Object[]{Arrays.asList(1L, 2L, 3L)},
        new Object[]{Arrays.asList(1L, 2L, 3L)},
        new Object[]{Arrays.asList(1L, 2L, 3L)},
        new Object[]{Arrays.asList(1L, 2L, 3L)},
        new Object[]{Arrays.asList(1L, 2L, 3L, 4L)},
        new Object[]{Arrays.asList(1L, 2L, 3L, 4L)},
        new Object[]{Arrays.asList(1L, 4L)},
        new Object[]{Arrays.asList(1L, 4L)},
        new Object[]{Arrays.asList(2L, 3L)},
        new Object[]{Arrays.asList(2L, 3L)}
    );

    RowSignature rowSignature = RowSignature.builder()
                                            .add("arrayLong", ColumnType.LONG_ARRAY)
                                            .build();

    RowSignature scanSignature = RowSignature.builder()
                                             .add("arrayLong", ColumnType.LONG_ARRAY)
                                             .build();

    Query<?> expectedQuery = newScanQueryBuilder()
        .dataSource(dataFileExternalDataSource)
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("arrayLong")
        .orderBy(Collections.singletonList(new ScanQuery.OrderBy("arrayLong", ScanQuery.Order.ASCENDING)))
        .context(defaultScanQueryContext(context, scanSignature))
        .build();

    testSelectQuery().setSql("SELECT\n"
                             + "  arrayLong\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '" + dataFileSignatureJsonString + "'\n"
                             + "  )\n"
                             + ")\n"
                             + "ORDER BY arrayLong")
                     .setQueryContext(context)
                     .setExpectedMSQSpec(MSQSpec
                                             .builder()
                                             .query(expectedQuery)
                                             .columnMappings(new ColumnMappings(ImmutableList.of(
                                                 new ColumnMapping("arrayLong", "arrayLong")
                                             )))
                                             .tuningConfig(MSQTuningConfig.defaultConfig())
                                             .destination(TaskReportMSQDestination.INSTANCE)
                                             .build()
                     )
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedRows)
                     .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testScanWithOrderByOnDoubleArray(String contextName, Map<String, Object> context)
  {
    final List<Object[]> expectedRows = Arrays.asList(
        new Object[]{null},
        new Object[]{null},
        new Object[]{null},
        new Object[]{null},
        new Object[]{Arrays.asList(1.1d, 2.2d, 3.3d)},
        new Object[]{Arrays.asList(1.1d, 2.2d, 3.3d)},
        new Object[]{Arrays.asList(1.1d, 2.2d, 3.3d)},
        new Object[]{Arrays.asList(1.1d, 2.2d, 3.3d)},
        new Object[]{Arrays.asList(1.1d, 3.3d)},
        new Object[]{Arrays.asList(1.1d, 3.3d)},
        new Object[]{Arrays.asList(2.2d, 3.3d, 4.0d)},
        new Object[]{Arrays.asList(2.2d, 3.3d, 4.0d)},
        new Object[]{Arrays.asList(3.3d, 4.4d, 5.5d)},
        new Object[]{Arrays.asList(3.3d, 4.4d, 5.5d)}
    );

    RowSignature rowSignature = RowSignature.builder()
                                            .add("arrayDouble", ColumnType.DOUBLE_ARRAY)
                                            .build();

    RowSignature scanSignature = RowSignature.builder()
                                             .add("arrayDouble", ColumnType.DOUBLE_ARRAY)
                                             .build();

    Query<?> expectedQuery = newScanQueryBuilder()
        .dataSource(dataFileExternalDataSource)
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("arrayDouble")
        .orderBy(Collections.singletonList(new ScanQuery.OrderBy("arrayDouble", ScanQuery.Order.ASCENDING)))
        .context(defaultScanQueryContext(context, scanSignature))
        .build();

    testSelectQuery().setSql("SELECT\n"
                             + "  arrayDouble\n"
                             + "FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{ \"files\": [" + dataFileNameJsonString + "],\"type\":\"local\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '" + dataFileSignatureJsonString + "'\n"
                             + "  )\n"
                             + ")\n"
                             + "ORDER BY arrayDouble")
                     .setQueryContext(context)
                     .setExpectedMSQSpec(MSQSpec
                                             .builder()
                                             .query(expectedQuery)
                                             .columnMappings(new ColumnMappings(ImmutableList.of(
                                                 new ColumnMapping("arrayDouble", "arrayDouble")
                                             )))
                                             .tuningConfig(MSQTuningConfig.defaultConfig())
                                             .destination(TaskReportMSQDestination.INSTANCE)
                                             .build()
                     )
                     .setExpectedRowSignature(rowSignature)
                     .setExpectedResultRows(expectedRows)
                     .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testScanExternBooleanArray(String contextName, Map<String, Object> context)
  {
    final List<Object[]> expectedRows = Collections.singletonList(
        new Object[]{Arrays.asList(1L, 0L, null)}
    );

    RowSignature scanSignature = RowSignature.builder()
                                             .add("a_bool", ColumnType.LONG_ARRAY)
                                             .build();

    Query<?> expectedQuery = newScanQueryBuilder()
        .dataSource(
            new ExternalDataSource(
                new InlineInputSource("{\"a_bool\":[true,false,null]}"),
                new JsonInputFormat(null, null, null, null, null),
                scanSignature
            )
        )
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("a_bool")
        .context(defaultScanQueryContext(context, scanSignature))
        .build();

    testSelectQuery().setSql("SELECT a_bool FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{\"type\": \"inline\", \"data\":\"{\\\"a_bool\\\":[true,false,null]}\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"a_bool\", \"type\": \"ARRAY<LONG>\"}]'\n"
                             + "  )\n"
                             + ")")
                     .setQueryContext(context)
                     .setExpectedMSQSpec(MSQSpec
                                             .builder()
                                             .query(expectedQuery)
                                             .columnMappings(new ColumnMappings(ImmutableList.of(
                                                 new ColumnMapping("a_bool", "a_bool")
                                             )))
                                             .tuningConfig(MSQTuningConfig.defaultConfig())
                                             .destination(TaskReportMSQDestination.INSTANCE)
                                             .build()
                     )
                     .setExpectedRowSignature(scanSignature)
                     .setExpectedResultRows(expectedRows)
                     .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testScanExternArrayWithNonConvertibleType(String contextName, Map<String, Object> context)
  {
    final List<Object[]> expectedRows = Collections.singletonList(
        new Object[]{Arrays.asList(null, null)}
    );

    RowSignature scanSignature = RowSignature.builder()
                                             .add("a_bool", ColumnType.LONG_ARRAY)
                                             .build();

    Query<?> expectedQuery = newScanQueryBuilder()
        .dataSource(
            new ExternalDataSource(
                new InlineInputSource("{\"a_bool\":[\"Test\",\"Test2\"]}"),
                new JsonInputFormat(null, null, null, null, null),
                scanSignature
            )
        )
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("a_bool")
        .context(defaultScanQueryContext(context, scanSignature))
        .build();

    testSelectQuery().setSql("SELECT a_bool FROM TABLE(\n"
                             + "  EXTERN(\n"
                             + "    '{\"type\": \"inline\", \"data\":\"{\\\"a_bool\\\":[\\\"Test\\\",\\\"Test2\\\"]}\"}',\n"
                             + "    '{\"type\": \"json\"}',\n"
                             + "    '[{\"name\": \"a_bool\", \"type\": \"ARRAY<LONG>\"}]'\n"
                             + "  )\n"
                             + ")")
                     .setQueryContext(context)
                     .setExpectedMSQSpec(MSQSpec
                                             .builder()
                                             .query(expectedQuery)
                                             .columnMappings(new ColumnMappings(ImmutableList.of(
                                                 new ColumnMapping("a_bool", "a_bool")
                                             )))
                                             .tuningConfig(MSQTuningConfig.defaultConfig())
                                             .destination(TaskReportMSQDestination.INSTANCE)
                                             .build()
                     )
                     .setExpectedRowSignature(scanSignature)
                     .setExpectedResultRows(expectedRows)
                     .verifyResults();
  }

  private List<Object[]> expectedMultiValueFooRowsToArray()
  {
    List<Object[]> expectedRows = new ArrayList<>();
    expectedRows.add(new Object[]{0L, null});
    if (!useDefault) {
      expectedRows.add(new Object[]{0L, ""});
    }

    expectedRows.addAll(ImmutableList.of(
        new Object[]{0L, ImmutableList.of("a", "b")},
        new Object[]{0L, ImmutableList.of("b", "c")},
        new Object[]{0L, "d"}
    ));
    return expectedRows;
  }
}
