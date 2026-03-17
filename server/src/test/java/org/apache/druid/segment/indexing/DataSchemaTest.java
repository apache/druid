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

package org.apache.druid.segment.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.druid.common.utils.IdUtilsTest;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.indexer.granularity.ArbitraryGranularitySpec;
import org.apache.druid.indexer.granularity.GranularitySpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

class DataSchemaTest extends InitializedNullHandlingTest
{
  private static ArbitraryGranularitySpec ARBITRARY_GRANULARITY = new ArbitraryGranularitySpec(
      Granularities.DAY,
      ImmutableList.of(Intervals.of("2014/2015"))
  );

  private final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  @Test
  void testDefaultExclusions()
  {
    DataSchema schema = DataSchema.builder()
                                  .withDataSource(IdUtilsTest.VALID_ID_CHARS)
                                  .withTimestamp(new TimestampSpec("time", "auto", null))
                                  .withDimensions(
                                      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("dimB", "dimA")))
                                  )
                                  .withAggregators(
                                      new DoubleSumAggregatorFactory("metric1", "col1"),
                                      new DoubleSumAggregatorFactory("metric2", "col2")
                                  )
                                  .withGranularity(ARBITRARY_GRANULARITY)
                                  .build();

    Assertions.assertEquals(
        ImmutableSet.of("__time", "time", "col1", "col2", "metric1", "metric2"),
        schema.getDimensionsSpec().getDimensionExclusions()
    );
  }

  @Test
  void testExplicitInclude()
  {
    DataSchema schema = DataSchema.builder()
                                  .withDataSource(IdUtilsTest.VALID_ID_CHARS)
                                  .withTimestamp(new TimestampSpec("time", "auto", null))
                                  .withDimensions(
                                      DimensionsSpec.builder()
                                                    .setDimensions(
                                                        DimensionsSpec.getDefaultSchemas(
                                                            List.of("time", "dimA", "dimB", "col2")
                                                        )
                                                    )
                                                    .setDimensionExclusions(ImmutableList.of("dimC"))
                                                    .build()
                                  )
                                  .withAggregators(
                                      new DoubleSumAggregatorFactory("metric1", "col1"),
                                      new DoubleSumAggregatorFactory("metric2", "col2")
                                  )
                                  .withGranularity(ARBITRARY_GRANULARITY)
                                  .build();

    Assertions.assertEquals(
        ImmutableSet.of("__time", "dimC", "col1", "metric1", "metric2"),
        schema.getDimensionsSpec().getDimensionExclusions()
    );
  }

  @Test
  void testOverlapMetricNameAndDim()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> DataSchema.builder()
                        .withDataSource(IdUtilsTest.VALID_ID_CHARS)
                        .withTimestamp(new TimestampSpec("time", "auto", null))
                        .withDimensions(
                            DimensionsSpec.builder()
                                          .setDimensions(
                                              DimensionsSpec.getDefaultSchemas(
                                                  List.of(
                                                      "time",
                                                      "dimA",
                                                      "dimB",
                                                      "metric1"
                                                  )
                                              )
                                          )
                                          .setDimensionExclusions(List.of("dimC"))
                                          .build()
                        )
                        .withAggregators(
                            new DoubleSumAggregatorFactory("metric1", "col1"),
                            new DoubleSumAggregatorFactory("metric2", "col2")
                        )
                        .withGranularity(ARBITRARY_GRANULARITY)
                        .build()
    );

    Assertions.assertEquals(
        "Cannot specify a column more than once: [metric1] seen in dimensions list, metricsSpec list",
        t.getMessage()
    );
  }

  @Test
  void testOverlapTimeAndDimPositionZero()
  {
    DataSchema schema = DataSchema.builder()
                                  .withDataSource(IdUtilsTest.VALID_ID_CHARS)
                                  .withTimestamp(new TimestampSpec("time", "auto", null))
                                  .withDimensions(
                                      DimensionsSpec.builder()
                                                    .setDimensions(
                                                        ImmutableList.of(
                                                            new LongDimensionSchema("__time"),
                                                            new StringDimensionSchema("dimA"),
                                                            new StringDimensionSchema("dimB")
                                                        )
                                                    )
                                                    .setDimensionExclusions(ImmutableList.of("dimC"))
                                                    .build()
                                  )
                                  .withGranularity(ARBITRARY_GRANULARITY)
                                  .build();

    Assertions.assertEquals(
        ImmutableList.of("__time", "dimA", "dimB"),
        schema.getDimensionsSpec().getDimensionNames()
    );

    Assertions.assertTrue(schema.getDimensionsSpec().isForceSegmentSortByTime());
  }

  @Test
  void testOverlapTimeAndDimPositionZeroWrongType()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> DataSchema.builder()
                        .withDataSource(IdUtilsTest.VALID_ID_CHARS)
                        .withTimestamp(new TimestampSpec("time", "auto", null))
                        .withDimensions(
                            DimensionsSpec.builder()
                                          .setDimensions(
                                              ImmutableList.of(
                                                  new StringDimensionSchema("__time"),
                                                  new StringDimensionSchema("dimA"),
                                                  new StringDimensionSchema("dimB")
                                              )
                                          )
                                          .setDimensionExclusions(ImmutableList.of("dimC"))
                                          .build()
                        )
                        .withGranularity(ARBITRARY_GRANULARITY)
                        .build()
    );

    Assertions.assertEquals(
        "Encountered dimension[__time] with incorrect type[STRING]. Type must be 'long'.",
        t.getMessage()
    );
  }

  @Test
  void testOverlapTimeAndDimPositionOne()
  {

    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> DataSchema.builder()
                        .withDataSource(IdUtilsTest.VALID_ID_CHARS)
                        .withTimestamp(new TimestampSpec("time", "auto", null))
                        .withDimensions(
                            DimensionsSpec.builder()
                                          .setDimensions(
                                              ImmutableList.of(
                                                  new StringDimensionSchema("dimA"),
                                                  new LongDimensionSchema("__time"),
                                                  new StringDimensionSchema("dimB")
                                              )
                                          )
                                          .setDimensionExclusions(ImmutableList.of("dimC"))
                                          .build()
                        )
                        .withGranularity(ARBITRARY_GRANULARITY)
                        .build()
    );

    Assertions.assertEquals(
        "Encountered dimension[__time] at position[1]. This is only supported when the dimensionsSpec "
        + "parameter[forceSegmentSortByTime] is set to[false]. "
        + DimensionsSpec.WARNING_NON_TIME_SORT_ORDER,
        t.getMessage()
    );
  }

  @Test
  void testOverlapTimeAndDimPositionOne_withExplicitSortOrder()
  {
    DataSchema schema =
        DataSchema.builder()
                  .withDataSource(IdUtilsTest.VALID_ID_CHARS)
                  .withTimestamp(new TimestampSpec("time", "auto", null))
                  .withDimensions(
                      DimensionsSpec.builder()
                                    .setDimensions(
                                        ImmutableList.of(
                                            new StringDimensionSchema("dimA"),
                                            new LongDimensionSchema("__time"),
                                            new StringDimensionSchema("dimB")
                                        )
                                    )
                                    .setDimensionExclusions(ImmutableList.of("dimC"))
                                    .setForceSegmentSortByTime(false)
                                    .build()
                  )
                  .withGranularity(ARBITRARY_GRANULARITY)
                  .build();

    Assertions.assertEquals(
        ImmutableList.of("dimA", "__time", "dimB"),
        schema.getDimensionsSpec().getDimensionNames()
    );

    Assertions.assertFalse(schema.getDimensionsSpec().isForceSegmentSortByTime());
  }

  @Test
  void testDuplicateAggregators()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> DataSchema.builder()
                        .withDataSource(IdUtilsTest.VALID_ID_CHARS)
                        .withTimestamp(new TimestampSpec("time", "auto", null))
                        .withDimensions(
                            DimensionsSpec.builder()
                                          .setDimensions(DimensionsSpec.getDefaultSchemas(ImmutableList.of("time")))
                                          .setDimensionExclusions(ImmutableList.of("dimC"))
                                          .build()
                        )
                        .withAggregators(
                            new DoubleSumAggregatorFactory("metric1", "col1"),
                            new DoubleSumAggregatorFactory("metric2", "col2"),
                            new DoubleSumAggregatorFactory("metric1", "col3"),
                            new DoubleSumAggregatorFactory("metric3", "col4"),
                            new DoubleSumAggregatorFactory("metric3", "col5")
                        )
                        .withGranularity(ARBITRARY_GRANULARITY)
                        .build()
    );

    Assertions.assertEquals(
        "Cannot specify a column more than once: [metric1] seen in metricsSpec list (2 occurrences); "
        + "[metric3] seen in metricsSpec list (2 occurrences)",
        t.getMessage()
    );
  }

  @Test
  void testEmptyDatasource()
  {
    DruidExceptionMatcher.ThrowingSupplier thrower =
        () -> DataSchema.builder()
                        .withDataSource("")
                        .withTimestamp(new TimestampSpec("time", "auto", null))
                        .withDimensions(
                            DimensionsSpec.builder()
                                          .setDimensions(
                                              DimensionsSpec.getDefaultSchemas(List.of("time", "dimA", "dimB", "col2"))
                                          )
                                          .setDimensionExclusions(List.of("dimC"))
                                          .build()
                        )
                        .withAggregators(
                            new DoubleSumAggregatorFactory("metric1", "col1"),
                            new DoubleSumAggregatorFactory("metric2", "col2")
                        )
                        .withGranularity(ARBITRARY_GRANULARITY)
                        .build();
    DruidExceptionMatcher.invalidInput()
                         .expectMessageIs("Invalid value for field [dataSource]: must not be null")
                         .assertThrowsAndMatches(thrower);
  }


  @Test
  void testInvalidWhitespaceDatasource()
  {
    Map<String, String> invalidCharToDataSourceName = ImmutableMap.of(
        "\\t", "\tab\t",
        "\\r", "\rcarriage\return\r",
        "\\n", "\nnew\nline\n"
    );

    for (Map.Entry<String, String> entry : invalidCharToDataSourceName.entrySet()) {
      String dataSource = entry.getValue();
      final String msg = StringUtils.format(
          "Invalid value for field [dataSource]: Value [%s] contains illegal whitespace characters.  Only space is allowed.",
          dataSource
      );
      DruidExceptionMatcher.invalidInput().expectMessageIs(msg).assertThrowsAndMatches(
          () -> DataSchema.builder()
                          .withDataSource(dataSource)
                          .build()
      );
    }
  }

  @Test
  void testSerde() throws Exception
  {
    // deserialize, then serialize, then deserialize of DataSchema.
    String jsonStr = "{"
                     + "\"dataSource\":\"" + StringEscapeUtils.escapeJson(IdUtilsTest.VALID_ID_CHARS) + "\","
                     + "\"timestampSpec\":{\"column\":\"xXx\", \"format\": \"auto\", \"missingValue\": null},"
                     + "\"dimensionsSpec\":{\"dimensions\":[], \"dimensionExclusions\":[]},"
                     + "\"flattenSpec\":{\"useFieldDiscovery\":true, \"fields\":[]},"
                     + "\"metricsSpec\":[{\"type\":\"doubleSum\",\"name\":\"metric1\",\"fieldName\":\"col1\"}],"
                     + "\"granularitySpec\":{"
                     + "\"type\":\"arbitrary\","
                     + "\"queryGranularity\":{\"type\":\"duration\",\"duration\":86400000,\"origin\":\"1970-01-01T00:00:00.000Z\"},"
                     + "\"intervals\":[\"2014-01-01T00:00:00.000Z/2015-01-01T00:00:00.000Z\"]}}";

    DataSchema actual = jsonMapper.readValue(
        jsonMapper.writeValueAsString(
            jsonMapper.readValue(jsonStr, DataSchema.class)
        ),
        DataSchema.class
    );

    Assertions.assertEquals(IdUtilsTest.VALID_ID_CHARS, actual.getDataSource());
    Assertions.assertEquals(
        new TimestampSpec("xXx", null, null),
        actual.getTimestampSpec()
    );
    Assertions.assertEquals(
        DimensionsSpec.builder().setDimensionExclusions(Arrays.asList("__time", "metric1", "xXx", "col1")).build(),
        actual.getDimensionsSpec()
    );
    Assertions.assertArrayEquals(
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1")
        },
        actual.getAggregators()
    );
    Assertions.assertEquals(
        new ArbitraryGranularitySpec(
            new DurationGranularity(86400000, null),
            ImmutableList.of(Intervals.of("2014/2015"))
        ),
        actual.getGranularitySpec()
    );
    Assertions.assertNull(actual.getProjections());
  }

  @Test
  public void testSerdeWithProjections() throws Exception
  {
    // serialize, then deserialize of DataSchema with projections.
    AggregateProjectionSpec projectionSpec =
        AggregateProjectionSpec.builder("ab_count_projection")
                               .groupingColumns(
                                   new StringDimensionSchema("a"),
                                   new LongDimensionSchema("b")
                               )
                               .aggregators(
                                   new CountAggregatorFactory("count")
                               )
                               .build();
    DataSchema original = DataSchema.builder()
                                    .withDataSource("datasource")
                                    .withTimestamp(new TimestampSpec(null, null, null))
                                    .withDimensions(DimensionsSpec.EMPTY)
                                    .withAggregators(new CountAggregatorFactory("rows"))
                                    .withProjections(ImmutableList.of(projectionSpec))
                                    .withGranularity(ARBITRARY_GRANULARITY)
                                    .build();
    DataSchema serdeResult = jsonMapper.readValue(jsonMapper.writeValueAsString(original), DataSchema.class);

    Assertions.assertEquals("datasource", serdeResult.getDataSource());
    Assertions.assertArrayEquals(
        new AggregatorFactory[]{new CountAggregatorFactory("rows")},
        serdeResult.getAggregators()
    );
    Assertions.assertEquals(ImmutableList.of(projectionSpec), serdeResult.getProjections());
    Assertions.assertEquals(ImmutableList.of("ab_count_projection"), serdeResult.getProjectionNames());
    Assertions.assertEquals(jsonMapper.writeValueAsString(original), jsonMapper.writeValueAsString(serdeResult));
  }

  @Test
  void testSerializeWithInvalidDataSourceName() throws Exception
  {
    // Escape backslashes to insert a tab character in the datasource name.
    Map<String, String> datasourceToErrorMsg = ImmutableMap.of(
        "",
        "Invalid value for field [dataSource]: must not be null",

        "../invalid",
        "Invalid value for field [dataSource]: Value [../invalid] cannot start with '.'.",

        "\tname",
        "Invalid value for field [dataSource]: Value [\tname] contains illegal whitespace characters.  Only space is allowed.",

        "name\t invalid",
        "Invalid value for field [dataSource]: Value [name\t invalid] contains illegal whitespace characters.  Only space is allowed."
    );
    for (Map.Entry<String, String> entry : datasourceToErrorMsg.entrySet()) {
      String jsonStr = "{"
                       + "\"dataSource\":\"" + StringEscapeUtils.escapeJson(entry.getKey()) + "\","
                       + "\"timestampSpec\":{\"column\":\"xXx\", \"format\": \"auto\", \"missingValue\": null},"
                       + "\"dimensionsSpec\":{\"dimensions\":[], \"dimensionExclusions\":[]},"
                       + "\"flattenSpec\":{\"useFieldDiscovery\":true, \"fields\":[]},"
                       + "\"metricsSpec\":[{\"type\":\"doubleSum\",\"name\":\"metric1\",\"fieldName\":\"col1\"}],"
                       + "\"granularitySpec\":{"
                       + "\"type\":\"arbitrary\","
                       + "\"queryGranularity\":{\"type\":\"duration\",\"duration\":86400000,\"origin\":\"1970-01-01T00:00:00.000Z\"},"
                       + "\"intervals\":[\"2014-01-01T00:00:00.000Z/2015-01-01T00:00:00.000Z\"]}}";
      try {
        jsonMapper.readValue(
            jsonMapper.writeValueAsString(
                jsonMapper.readValue(jsonStr, DataSchema.class)
            ),
            DataSchema.class
        );
      }
      catch (ValueInstantiationException e) {
        MatcherAssert.assertThat(
            entry.getKey(),
            e.getCause(),
            DruidExceptionMatcher.invalidInput().expectMessageIs(
                entry.getValue()
            )
        );
        continue;
      }
      Assertions.fail("Serialization of datasource " + entry.getKey() + " should have failed.");
    }
  }

  @Test
  void testSerdeWithUpdatedDataSchemaAddedField() throws IOException
  {
    DataSchema originalSchema = DataSchema.builder()
                                          .withDataSource(IdUtilsTest.VALID_ID_CHARS)
                                          .withTimestamp(new TimestampSpec("time", "auto", null))
                                          .withDimensions(
                                              new DimensionsSpec(
                                                  DimensionsSpec.getDefaultSchemas(List.of("dimB", "dimA"))
                                              )
                                          )
                                          .withAggregators(
                                              new DoubleSumAggregatorFactory("metric1", "col1"),
                                              new DoubleSumAggregatorFactory("metric2", "col2")
                                          )
                                          .withGranularity(ARBITRARY_GRANULARITY)
                                          .build();

    String serialized = jsonMapper.writeValueAsString(originalSchema);
    TestModifiedDataSchema deserialized = jsonMapper.readValue(serialized, TestModifiedDataSchema.class);

    Assertions.assertNull(deserialized.getExtra());
    Assertions.assertEquals(originalSchema.getDataSource(), deserialized.getDataSource());
    Assertions.assertEquals(originalSchema.getGranularitySpec(), deserialized.getGranularitySpec());
    Assertions.assertEquals(originalSchema.getTimestampSpec(), deserialized.getTimestampSpec());
    Assertions.assertEquals(originalSchema.getDimensionsSpec(), deserialized.getDimensionsSpec());
    Assertions.assertArrayEquals(originalSchema.getAggregators(), deserialized.getAggregators());
    Assertions.assertEquals(originalSchema.getTransformSpec(), deserialized.getTransformSpec());
  }

  @Test
  void testSerdeWithUpdatedDataSchemaRemovedField() throws IOException
  {
    TestModifiedDataSchema originalSchema = new TestModifiedDataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        new TimestampSpec("time", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dimB", "dimA"))),
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2")
        },
        ARBITRARY_GRANULARITY,
        null,
        "some arbitrary string"
    );

    String serialized = jsonMapper.writeValueAsString(originalSchema);
    DataSchema deserialized = jsonMapper.readValue(serialized, DataSchema.class);

    Assertions.assertEquals(originalSchema.getDataSource(), deserialized.getDataSource());
    Assertions.assertEquals(originalSchema.getGranularitySpec(), deserialized.getGranularitySpec());
    Assertions.assertEquals(originalSchema.getTimestampSpec(), deserialized.getTimestampSpec());
    Assertions.assertEquals(originalSchema.getDimensionsSpec(), deserialized.getDimensionsSpec());
    Assertions.assertArrayEquals(originalSchema.getAggregators(), deserialized.getAggregators());
    Assertions.assertEquals(originalSchema.getTransformSpec(), deserialized.getTransformSpec());
  }

  @Test
  void testWithDimensionSpec()
  {
    TimestampSpec tsSpec = Mockito.mock(TimestampSpec.class);
    GranularitySpec gSpec = Mockito.mock(GranularitySpec.class);
    DimensionsSpec oldDimSpec = Mockito.mock(DimensionsSpec.class);
    DimensionsSpec newDimSpec = Mockito.mock(DimensionsSpec.class);
    AggregatorFactory aggFactory = Mockito.mock(AggregatorFactory.class);
    Mockito.when(aggFactory.getName()).thenReturn("myAgg");
    TransformSpec transSpec = Mockito.mock(TransformSpec.class);
    Mockito.when(newDimSpec.withDimensionExclusions(ArgumentMatchers.any(Set.class))).thenReturn(newDimSpec);

    DataSchema oldSchema = DataSchema.builder()
                                     .withDataSource("dataSource")
                                     .withTimestamp(tsSpec)
                                     .withDimensions(oldDimSpec)
                                     .withAggregators(aggFactory)
                                     .withGranularity(gSpec)
                                     .withTransform(transSpec)
                                     .build();
    DataSchema newSchema = oldSchema.withDimensionsSpec(newDimSpec);
    Assertions.assertSame(oldSchema.getDataSource(), newSchema.getDataSource());
    Assertions.assertSame(oldSchema.getTimestampSpec(), newSchema.getTimestampSpec());
    Assertions.assertSame(newDimSpec, newSchema.getDimensionsSpec());
    Assertions.assertSame(oldSchema.getAggregators(), newSchema.getAggregators());
    Assertions.assertSame(oldSchema.getGranularitySpec(), newSchema.getGranularitySpec());
    Assertions.assertSame(oldSchema.getTransformSpec(), newSchema.getTransformSpec());

  }

  @Test
  void testCombinedDataSchemaSetsMultiValuedColumnsInfo()
  {
    Set<String> multiValuedDimensions = ImmutableSet.of("dimA");

    CombinedDataSchema schema = new CombinedDataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        new TimestampSpec("time", "auto", null),
        DimensionsSpec.builder()
                      .setDimensions(
                          DimensionsSpec.getDefaultSchemas(ImmutableList.of("dimA", "dimB", "metric1"))
                      )
                      .setDimensionExclusions(ImmutableList.of("dimC"))
                      .build(),
        null,
        ARBITRARY_GRANULARITY,
        null,
        null,
        multiValuedDimensions
    );
    Assertions.assertEquals(ImmutableSet.of("dimA"), schema.getMultiValuedDimensions());
  }

  @Test
  void testInvalidProjectionDupeNames()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> DataSchema.builder()
                        .withDataSource("dataSource")
                        .withGranularity(
                            new UniformGranularitySpec(
                                Granularities.HOUR,
                                Granularities.NONE,
                                false,
                                List.of(Intervals.of("2014/2015"))
                            )
                        )
                        .withProjections(
                            List.of(
                                AggregateProjectionSpec.builder("some projection")
                                                       .virtualColumns(
                                                           Granularities.toVirtualColumn(Granularities.HOUR, "g")
                                                       )
                                                       .groupingColumns(new LongDimensionSchema("g"))
                                                       .aggregators(new CountAggregatorFactory("count"))
                                                       .build(),
                                AggregateProjectionSpec.builder("some projection")
                                                       .virtualColumns(
                                                           Granularities.toVirtualColumn(Granularities.MINUTE, "g")
                                                       )
                                                       .groupingColumns(new LongDimensionSchema("g"))
                                                       .aggregators(new CountAggregatorFactory("count"))
                                                       .build()
                            )
                        )
                        .build()
    );

    Assertions.assertEquals(
        "projection[some projection] is already defined, projection names must be unique",
        t.getMessage()
    );
  }

  @Test
  void testInvalidProjectionGranularity()
  {
    AggregateProjectionSpec.Builder bob = AggregateProjectionSpec.builder()
                                                                 .groupingColumns(new LongDimensionSchema("g"))
                                                                 .aggregators(new CountAggregatorFactory("count"));
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> DataSchema.builder()
                        .withDataSource("dataSource")
                        .withGranularity(
                            new UniformGranularitySpec(
                                Granularities.HOUR,
                                Granularities.NONE,
                                false,
                                List.of(Intervals.of("2014/2015"))
                            )
                        )
                        .withProjections(
                            List.of(
                                bob.name("ok granularity")
                                   .virtualColumns(Granularities.toVirtualColumn(Granularities.HOUR, "g"))
                                   .build(),
                                bob.name("acceptable granularity")
                                   .virtualColumns(Granularities.toVirtualColumn(Granularities.MINUTE, "g"))
                                   .build(),
                                AggregateProjectionSpec.builder()
                                                       .name("not having a time column is ok too")
                                                       .aggregators(new CountAggregatorFactory("count"))
                                                       .build(),
                                bob.name("bad granularity")
                                   .virtualColumns(Granularities.toVirtualColumn(Granularities.DAY, "g"))
                                   .build()
                            )
                        )
                        .build()
    );

    Assertions.assertEquals(
        "projection[bad granularity] has granularity[{type=period, period=P1D, timeZone=UTC, origin=null}]"
        + " which must be finer than or equal to segment granularity[{type=period, period=PT1H, timeZone=UTC,"
        + " origin=null}]",
        t.getMessage()
    );
  }

  @Test
  void testInvalidProjectionDupeGroupingNames()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> DataSchema.builder()
                        .withDataSource("dataSource")
                        .withGranularity(
                            new UniformGranularitySpec(
                                Granularities.HOUR,
                                Granularities.NONE,
                                false,
                                List.of(Intervals.of("2014/2015"))
                            )
                        )
                        .withProjections(
                            List.of(
                                AggregateProjectionSpec.builder("some projection")
                                                       .virtualColumns(
                                                           Granularities.toVirtualColumn(Granularities.HOUR, "g")
                                                       )
                                                       .groupingColumns(
                                                           new LongDimensionSchema("g"),
                                                           new StringDimensionSchema("g")
                                                       )
                                                       .aggregators(new CountAggregatorFactory("count"))
                                                       .build()
                            )
                        )
                        .build()
    );

    Assertions.assertEquals(
        "Cannot specify a column more than once: [g] seen in projection[some projection] grouping column list (2 occurrences)",
        t.getMessage()
    );
  }

  @Test
  void testInvalidProjectionDupeAggNames()
  {
    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> DataSchema.builder()
                        .withDataSource("dataSource")
                        .withGranularity(
                            new UniformGranularitySpec(
                                Granularities.HOUR,
                                Granularities.NONE,
                                false,
                                List.of(Intervals.of("2014/2015"))
                            )
                        )
                        .withProjections(
                            List.of(
                                AggregateProjectionSpec.builder("some projection")
                                                       .virtualColumns(
                                                           Granularities.toVirtualColumn(Granularities.HOUR, "g")
                                                       )
                                                       .groupingColumns(new LongDimensionSchema("g"))
                                                       .aggregators(
                                                           new LongSumAggregatorFactory("a0", "added"),
                                                           new DoubleSumAggregatorFactory("a0", "added")
                                                       )
                                                       .build()
                            )
                        )
                        .build()
    );

    Assertions.assertEquals(
        "Cannot specify a column more than once: [a0] seen in projection[some projection] aggregators list (2 occurrences)",
        t.getMessage()
    );
  }

  @Test
  void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(DataSchema.class).usingGetClass().verify();
  }
}
