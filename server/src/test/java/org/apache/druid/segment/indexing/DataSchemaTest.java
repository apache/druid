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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.druid.common.utils.IdUtilsTest;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataSchemaTest extends InitializedNullHandlingTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  @Test
  public void testDefaultExclusions()
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dimB", "dimA")), null, null),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema schema = new DataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        parser,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
            },
        new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
        null,
        jsonMapper
    );

    Assert.assertEquals(
        ImmutableSet.of("time", "col1", "col2", "metric1", "metric2"),
        schema.getDimensionsSpec().getDimensionExclusions()
    );
  }

  @Test
  public void testExplicitInclude()
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(
                    DimensionsSpec.getDefaultSchemas(ImmutableList.of("time", "dimA", "dimB", "col2")),
                    ImmutableList.of("dimC"),
                    null
                ),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema schema = new DataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        parser,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
            },
        new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
        null,
        jsonMapper
    );

    Assert.assertEquals(
        ImmutableSet.of("dimC", "col1", "metric1", "metric2"),
        schema.getParser().getParseSpec().getDimensionsSpec().getDimensionExclusions()
    );
  }

  @Test
  public void testTransformSpec()
  {
    Map<String, Object> parserMap = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(
                    DimensionsSpec.getDefaultSchemas(ImmutableList.of("time", "dimA", "dimB", "col2")),
                    ImmutableList.of(),
                    null
                ),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema schema = new DataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        parserMap,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
            },
        new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
        new TransformSpec(
            new SelectorDimFilter("dimA", "foo", null),
            ImmutableList.of(
                new ExpressionTransform("expr", "concat(dimA,dimA)", TestExprMacroTable.INSTANCE)
            )
        ),
        jsonMapper
    );

    // Test hack that produces a StringInputRowParser.
    final StringInputRowParser parser = (StringInputRowParser) schema.getParser();

    final InputRow row1bb = parser.parseBatch(
        ByteBuffer.wrap("{\"time\":\"2000-01-01\",\"dimA\":\"foo\"}".getBytes(StandardCharsets.UTF_8))
    ).get(0);
    Assert.assertEquals(DateTimes.of("2000-01-01"), row1bb.getTimestamp());
    Assert.assertEquals("foo", row1bb.getRaw("dimA"));
    Assert.assertEquals("foofoo", row1bb.getRaw("expr"));

    final InputRow row1string = parser.parse("{\"time\":\"2000-01-01\",\"dimA\":\"foo\"}");
    Assert.assertEquals(DateTimes.of("2000-01-01"), row1string.getTimestamp());
    Assert.assertEquals("foo", row1string.getRaw("dimA"));
    Assert.assertEquals("foofoo", row1string.getRaw("expr"));

    final InputRow row2 = parser.parseBatch(
        ByteBuffer.wrap("{\"time\":\"2000-01-01\",\"dimA\":\"x\"}".getBytes(StandardCharsets.UTF_8))
    ).get(0);
    Assert.assertNull(row2);
  }

  @Test(expected = IAE.class)
  public void testOverlapMetricNameAndDim()
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of(
                    "time",
                    "dimA",
                    "dimB",
                    "metric1"
                )), ImmutableList.of("dimC"), null),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema schema = new DataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        parser,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
            },
        new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
        null,
        jsonMapper
    );
    schema.getParser();
  }

  @Test(expected = IAE.class)
  public void testDuplicateAggregators()
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(
                    DimensionsSpec.getDefaultSchemas(ImmutableList.of("time")),
                    ImmutableList.of("dimC"),
                    null
                ),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema schema = new DataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        parser,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
            new DoubleSumAggregatorFactory("metric1", "col3"),
            },
        new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
        null,
        jsonMapper
    );
    schema.getParser();
  }

  @Test
  public void testSerdeWithInvalidParserMap() throws Exception
  {
    String jsonStr = "{"
                     + "\"dataSource\":\"" + StringEscapeUtils.escapeJson(IdUtilsTest.VALID_ID_CHARS) + "\","
                     + "\"parser\":{\"type\":\"invalid\"},"
                     + "\"metricsSpec\":[{\"type\":\"doubleSum\",\"name\":\"metric1\",\"fieldName\":\"col1\"}],"
                     + "\"granularitySpec\":{"
                     + "\"type\":\"arbitrary\","
                     + "\"queryGranularity\":{\"type\":\"duration\",\"duration\":86400000,\"origin\":\"1970-01-01T00:00:00.000Z\"},"
                     + "\"intervals\":[\"2014-01-01T00:00:00.000Z/2015-01-01T00:00:00.000Z\"]}}";


    //no error on serde as parser is converted to InputRowParser lazily when really needed
    DataSchema schema = jsonMapper.readValue(
        jsonMapper.writeValueAsString(
            jsonMapper.readValue(jsonStr, DataSchema.class)
        ),
        DataSchema.class
    );

    expectedException.expect(CoreMatchers.instanceOf(IllegalArgumentException.class));
    expectedException.expectCause(CoreMatchers.instanceOf(JsonMappingException.class));
    expectedException.expectMessage(
        "Cannot construct instance of `org.apache.druid.data.input.impl.StringInputRowParser`, problem: parseSpec"
    );

    // Jackson creates a default type parser (StringInputRowParser) for an invalid type.
    schema.getParser();
  }

  @Test
  public void testEmptyDatasource()
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(
                    DimensionsSpec.getDefaultSchemas(ImmutableList.of("time", "dimA", "dimB", "col2")),
                    ImmutableList.of("dimC"),
                    null
                ),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    expectedException.expect(CoreMatchers.instanceOf(IllegalArgumentException.class));
    expectedException.expectMessage(
        "dataSource cannot be null or empty. Please provide a dataSource."
    );

    DataSchema schema = new DataSchema(
        "",
        parser,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
            },
        new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
        null,
        jsonMapper
    );
  }


  @Test
  public void testInvalidWhitespaceDatasource()
  {
    Map<String, String> invalidCharToDataSourceName = ImmutableMap.of(
        "\\t", "\tab\t",
        "\\r", "\rcarriage\return\r",
        "\\n", "\nnew\nline\n"
    );

    for (Map.Entry<String, String> entry : invalidCharToDataSourceName.entrySet()) {
      testInvalidWhitespaceDatasourceHelper(entry.getValue(), entry.getKey());
    }
  }

  private void testInvalidWhitespaceDatasourceHelper(String dataSource, String invalidChar)
  {
    String testFailMsg = "dataSource contain invalid whitespace character: " + invalidChar;
    try {
      DataSchema schema = new DataSchema(
          dataSource,
          Collections.emptyMap(),
          null,
          null,
          null,
          jsonMapper
      );
      Assert.fail(testFailMsg);
    }
    catch (IllegalArgumentException errorMsg) {
      String expectedMsg = "dataSource cannot contain whitespace character except space.";
      Assert.assertEquals(testFailMsg, expectedMsg, errorMsg.getMessage());
    }
  }

  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{"
                     + "\"dataSource\":\"" + StringEscapeUtils.escapeJson(IdUtilsTest.VALID_ID_CHARS) + "\","
                     + "\"parser\":{"
                     + "\"type\":\"string\","
                     + "\"parseSpec\":{"
                     + "\"format\":\"json\","
                     + "\"timestampSpec\":{\"column\":\"xXx\", \"format\": \"auto\", \"missingValue\": null},"
                     + "\"dimensionsSpec\":{\"dimensions\":[], \"dimensionExclusions\":[]},"
                     + "\"flattenSpec\":{\"useFieldDiscovery\":true, \"fields\":[]},"
                     + "\"featureSpec\":{}},"
                     + "\"encoding\":\"UTF-8\""
                     + "},"
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

    Assert.assertEquals(actual.getDataSource(), IdUtilsTest.VALID_ID_CHARS);
    Assert.assertEquals(
        actual.getParser().getParseSpec(),
        new JSONParseSpec(
            new TimestampSpec("xXx", null, null),
            new DimensionsSpec(null, Arrays.asList("metric1", "xXx", "col1"), null),
            null,
            null,
            null
        )
    );
    Assert.assertArrayEquals(
        actual.getAggregators(),
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1")
        }
    );
    Assert.assertEquals(
        actual.getGranularitySpec(),
        new ArbitraryGranularitySpec(
            new DurationGranularity(86400000, null),
            ImmutableList.of(Intervals.of("2014/2015"))
        )
    );
  }

  @Test
  public void testSerializeWithInvalidDataSourceName() throws Exception
  {
    // Escape backslashes to insert a tab character in the datasource name.
    List<String> datasources = ImmutableList.of("", "../invalid", "\tname", "name\t invalid");
    for (String datasource : datasources) {
      String jsonStr = "{"
                       + "\"dataSource\":\"" + StringEscapeUtils.escapeJson(datasource) + "\","
                       + "\"parser\":{"
                       + "\"type\":\"string\","
                       + "\"parseSpec\":{"
                       + "\"format\":\"json\","
                       + "\"timestampSpec\":{\"column\":\"xXx\", \"format\": \"auto\", \"missingValue\": null},"
                       + "\"dimensionsSpec\":{\"dimensions\":[], \"dimensionExclusions\":[]},"
                       + "\"flattenSpec\":{\"useFieldDiscovery\":true, \"fields\":[]},"
                       + "\"featureSpec\":{}},"
                       + "\"encoding\":\"UTF-8\""
                       + "},"
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
        Assert.assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        continue;
      }
      Assert.fail("Serialization of datasource " + datasource + " should have failed.");
    }
  }

  @Test
  public void testSerdeWithUpdatedDataSchemaAddedField() throws IOException
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dimB", "dimA")), null, null),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema originalSchema = new DataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        parser,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
            },
        new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
        null,
        jsonMapper
    );

    String serialized = jsonMapper.writeValueAsString(originalSchema);
    TestModifiedDataSchema deserialized = jsonMapper.readValue(serialized, TestModifiedDataSchema.class);

    Assert.assertEquals(null, deserialized.getExtra());
    Assert.assertEquals(originalSchema.getDataSource(), deserialized.getDataSource());
    Assert.assertEquals(originalSchema.getGranularitySpec(), deserialized.getGranularitySpec());
    Assert.assertEquals(originalSchema.getParser().getParseSpec(), deserialized.getParser().getParseSpec());
    Assert.assertArrayEquals(originalSchema.getAggregators(), deserialized.getAggregators());
    Assert.assertEquals(originalSchema.getTransformSpec(), deserialized.getTransformSpec());
    Assert.assertEquals(originalSchema.getParserMap(), deserialized.getParserMap());
  }

  @Test
  public void testSerdeWithUpdatedDataSchemaRemovedField() throws IOException
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dimB", "dimA")), null, null),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    TestModifiedDataSchema originalSchema = new TestModifiedDataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        null,
        null,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
            },
        new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
        null,
        parser,
        jsonMapper,
        "some arbitrary string"
    );

    String serialized = jsonMapper.writeValueAsString(originalSchema);
    DataSchema deserialized = jsonMapper.readValue(serialized, DataSchema.class);

    Assert.assertEquals(originalSchema.getDataSource(), deserialized.getDataSource());
    Assert.assertEquals(originalSchema.getGranularitySpec(), deserialized.getGranularitySpec());
    Assert.assertEquals(originalSchema.getParser().getParseSpec(), deserialized.getParser().getParseSpec());
    Assert.assertArrayEquals(originalSchema.getAggregators(), deserialized.getAggregators());
    Assert.assertEquals(originalSchema.getTransformSpec(), deserialized.getTransformSpec());
    Assert.assertEquals(originalSchema.getParserMap(), deserialized.getParserMap());
  }

  @Test
  public void testWithDimensionSpec()
  {
    TimestampSpec tsSpec = Mockito.mock(TimestampSpec.class);
    GranularitySpec gSpec = Mockito.mock(GranularitySpec.class);
    DimensionsSpec oldDimSpec = Mockito.mock(DimensionsSpec.class);
    DimensionsSpec newDimSpec = Mockito.mock(DimensionsSpec.class);
    AggregatorFactory aggFactory = Mockito.mock(AggregatorFactory.class);
    TransformSpec transSpec = Mockito.mock(TransformSpec.class);
    Map<String, Object> parserMap = Mockito.mock(Map.class);
    Mockito.when(newDimSpec.withDimensionExclusions(ArgumentMatchers.any(Set.class))).thenReturn(newDimSpec);

    DataSchema oldSchema = new DataSchema("dataSource", tsSpec, oldDimSpec,
                                          new AggregatorFactory[]{aggFactory}, gSpec,
                                          transSpec, parserMap, jsonMapper
    );
    DataSchema newSchema = oldSchema.withDimensionsSpec(newDimSpec);
    Assert.assertSame(oldSchema.getDataSource(), newSchema.getDataSource());
    Assert.assertSame(oldSchema.getTimestampSpec(), newSchema.getTimestampSpec());
    Assert.assertSame(newDimSpec, newSchema.getDimensionsSpec());
    Assert.assertSame(oldSchema.getAggregators(), newSchema.getAggregators());
    Assert.assertSame(oldSchema.getGranularitySpec(), newSchema.getGranularitySpec());
    Assert.assertSame(oldSchema.getTransformSpec(), newSchema.getTransformSpec());
    Assert.assertSame(oldSchema.getParserMap(), newSchema.getParserMap());

  }
}
