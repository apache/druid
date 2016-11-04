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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.IAE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class DataSchemaTest
{
  private final ObjectMapper jsonMapper;

  public DataSchemaTest()
  {
    jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, jsonMapper));
  }

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
        ), new TypeReference<Map<String, Object>>() {}
    );

    DataSchema schema = new DataSchema(
        "test",
        parser,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
        },
        new ArbitraryGranularitySpec(QueryGranularities.DAY, ImmutableList.of(Interval.parse("2014/2015"))),
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
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("time", "dimA", "dimB", "col2")), ImmutableList.of("dimC"), null),
                null,
                null
            ),
            null
        ), new TypeReference<Map<String, Object>>() {}
    );

    DataSchema schema = new DataSchema(
        "test",
        parser,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
        },
        new ArbitraryGranularitySpec(QueryGranularities.DAY, ImmutableList.of(Interval.parse("2014/2015"))),
        jsonMapper
    );

    Assert.assertEquals(
        ImmutableSet.of("dimC", "col1", "metric1", "metric2"),
        schema.getParser().getParseSpec().getDimensionsSpec().getDimensionExclusions()
    );
  }

  @Test(expected = IAE.class)
  public void testOverlapMetricNameAndDim() throws Exception
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("time", "dimA", "dimB", "metric1")), ImmutableList.of("dimC"), null),
                null,
                null
            ),
            null
        ), new TypeReference<Map<String, Object>>() {}
    );

    DataSchema schema = new DataSchema(
        "test",
        parser,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
        },
        new ArbitraryGranularitySpec(QueryGranularities.DAY, ImmutableList.of(Interval.parse("2014/2015"))),
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

    try {
      schema.getParser();
      Assert.fail("should've failed to get parser.");
    }
    catch (IllegalArgumentException ex) {

    }
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
    Assert.assertEquals(
        actual.getAggregators(),
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1")
        }
    );
    Assert.assertEquals(
        actual.getGranularitySpec(),
        new ArbitraryGranularitySpec(QueryGranularities.DAY, ImmutableList.of(Interval.parse("2014/2015")))
    );
  }
}
