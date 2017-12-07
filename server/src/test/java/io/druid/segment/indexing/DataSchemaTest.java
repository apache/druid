/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.indexing;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.granularity.DurationGranularity;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.jackson.JacksonUtils;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.expression.TestExprMacroTable;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.TestHelper;
import io.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import io.druid.segment.transform.ExpressionTransform;
import io.druid.segment.transform.TransformSpec;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class DataSchemaTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final ObjectMapper jsonMapper = TestHelper.getJsonMapper();

  @Test
  public void testDefaultExclusions() throws Exception
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dimB", "dimA")), null, null),
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema schema = new DataSchema(
        "test",
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
        schema.getParser().getParseSpec().getDimensionsSpec().getDimensionExclusions()
    );
  }

  @Test
  public void testExplicitInclude() throws Exception
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
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema schema = new DataSchema(
        "test",
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
  public void testTransformSpec() throws Exception
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
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema schema = new DataSchema(
        "test",
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
        ByteBuffer.wrap("{\"time\":\"2000-01-01\",\"dimA\":\"foo\"}".getBytes(Charsets.UTF_8))
    ).get(0);
    Assert.assertEquals(DateTimes.of("2000-01-01"), row1bb.getTimestamp());
    Assert.assertEquals("foo", row1bb.getRaw("dimA"));
    Assert.assertEquals("foofoo", row1bb.getRaw("expr"));

    final InputRow row1string = parser.parse("{\"time\":\"2000-01-01\",\"dimA\":\"foo\"}");
    Assert.assertEquals(DateTimes.of("2000-01-01"), row1string.getTimestamp());
    Assert.assertEquals("foo", row1string.getRaw("dimA"));
    Assert.assertEquals("foofoo", row1string.getRaw("expr"));

    final InputRow row2 = parser.parseBatch(
        ByteBuffer.wrap("{\"time\":\"2000-01-01\",\"dimA\":\"x\"}".getBytes(Charsets.UTF_8))
    ).get(0);
    Assert.assertNull(row2);
  }

  @Test(expected = IAE.class)
  public void testOverlapMetricNameAndDim() throws Exception
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
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema schema = new DataSchema(
        "test",
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
  public void testDuplicateAggregators() throws Exception
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
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema schema = new DataSchema(
        "test",
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
                     + "\"dataSource\":\"test\","
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
        "Instantiation of [simple type, class io.druid.data.input.impl.StringInputRowParser] value failed: parseSpec"
    );

    // Jackson creates a default type parser (StringInputRowParser) for an invalid type.
    schema.getParser();
  }

  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{"
                     + "\"dataSource\":\"test\","
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

    Assert.assertEquals(actual.getDataSource(), "test");
    Assert.assertEquals(
        actual.getParser().getParseSpec(),
        new JSONParseSpec(
            new TimestampSpec("xXx", null, null),
            new DimensionsSpec(null, Arrays.asList("metric1", "xXx", "col1"), null),
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
}
