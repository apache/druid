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

package org.apache.druid.indexing.overlord.sampler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.impl.DelimitedParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.TestFirehose;
import org.apache.druid.indexing.overlord.sampler.SamplerResponse.SamplerResponseRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class FirehoseSamplerTest
{
  private enum ParserType
  {
    MAP, STR_JSON, STR_CSV
  }

  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final boolean USE_DEFAULT_VALUE_FOR_NULL = Boolean.valueOf(System.getProperty(
      NullHandling.NULL_HANDLING_CONFIG_STRING,
      "true"
  ));

  private static final List<Object> MAP_ROWS = ImmutableList.of(
      ImmutableMap.of("t", "2019-04-22T12:00", "dim1", "foo", "met1", "1"),
      ImmutableMap.of("t", "2019-04-22T12:00", "dim1", "foo", "met1", "2"),
      ImmutableMap.of("t", "2019-04-22T12:01", "dim1", "foo", "met1", "3"),
      ImmutableMap.of("t", "2019-04-22T12:00", "dim1", "foo2", "met1", "4"),
      ImmutableMap.of("t", "2019-04-22T12:00", "dim1", "foo", "dim2", "bar", "met1", "5"),
      ImmutableMap.of("t", "bad_timestamp", "dim1", "foo", "met1", "6")
  );

  private static final List<Object> STR_JSON_ROWS = ImmutableList.of(
      "{ \"t\": \"2019-04-22T12:00\", \"dim1\": \"foo\", \"met1\": 1 }",
      "{ \"t\": \"2019-04-22T12:00\", \"dim1\": \"foo\", \"met1\": 2 }",
      "{ \"t\": \"2019-04-22T12:01\", \"dim1\": \"foo\", \"met1\": 3 }",
      "{ \"t\": \"2019-04-22T12:00\", \"dim1\": \"foo2\", \"met1\": 4 }",
      "{ \"t\": \"2019-04-22T12:00\", \"dim1\": \"foo\", \"dim2\": \"bar\", \"met1\": 5 }",
      "{ \"t\": \"bad_timestamp\", \"dim1\": \"foo\", \"met1\": 6 }"
  );

  private static final List<Object> STR_CSV_ROWS = ImmutableList.of(
      "2019-04-22T12:00,foo,,1",
      "2019-04-22T12:00,foo,,2",
      "2019-04-22T12:01,foo,,3",
      "2019-04-22T12:00,foo2,,4",
      "2019-04-22T12:00,foo,bar,5",
      "bad_timestamp,foo,,6"
  );

  private SamplerCache samplerCache;
  private FirehoseSampler firehoseSampler;
  private ParserType parserType;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Parameterized.Parameters(name = "parserType = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{ParserType.MAP},
        new Object[]{ParserType.STR_JSON},
        new Object[]{ParserType.STR_CSV}
    );
  }

  public FirehoseSamplerTest(ParserType parserType)
  {
    this.parserType = parserType;
  }

  @Before
  public void setupTest()
  {
    samplerCache = new SamplerCache(MapCache.create(100000));
    firehoseSampler = new FirehoseSampler(OBJECT_MAPPER, samplerCache);
  }

  @Test
  public void testNoParams()
  {
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("firehoseFactory required");

    firehoseSampler.sample(null, null, null);
  }

  @Test
  public void testNoDataSchema()
  {
    FirehoseFactory firehoseFactory = getFirehoseFactory(getTestRows());

    SamplerResponse response = firehoseSampler.sample(firehoseFactory, null, null);

    Assert.assertEquals(6, (int) response.getNumRowsRead());
    Assert.assertEquals(0, (int) response.getNumRowsIndexed());
    Assert.assertEquals(6, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    Assert.assertEquals(new SamplerResponseRow(getTestRows().get(0).toString(), null, true, null), data.get(0));
    Assert.assertEquals(new SamplerResponseRow(getTestRows().get(1).toString(), null, true, null), data.get(1));
    Assert.assertEquals(new SamplerResponseRow(getTestRows().get(2).toString(), null, true, null), data.get(2));
    Assert.assertEquals(new SamplerResponseRow(getTestRows().get(3).toString(), null, true, null), data.get(3));
    Assert.assertEquals(new SamplerResponseRow(getTestRows().get(4).toString(), null, true, null), data.get(4));
    Assert.assertEquals(new SamplerResponseRow(getTestRows().get(5).toString(), null, true, null), data.get(5));
  }

  @Test
  public void testNoDataSchemaNumRows()
  {
    FirehoseFactory firehoseFactory = getFirehoseFactory(getTestRows());

    SamplerResponse response = firehoseSampler.sample(firehoseFactory, null, new SamplerConfig(3, null, true, null));

    Assert.assertNull(response.getCacheKey());
    Assert.assertEquals(3, (int) response.getNumRowsRead());
    Assert.assertEquals(0, (int) response.getNumRowsIndexed());
    Assert.assertEquals(3, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    Assert.assertEquals(new SamplerResponseRow(getTestRows().get(0).toString(), null, true, null), data.get(0));
    Assert.assertEquals(new SamplerResponseRow(getTestRows().get(1).toString(), null, true, null), data.get(1));
    Assert.assertEquals(new SamplerResponseRow(getTestRows().get(2).toString(), null, true, null), data.get(2));
  }

  @Test
  public void testNoDataSchemaNumRowsCacheReplay()
  {
    FirehoseFactory firehoseFactory = getFirehoseFactory(getTestRows());

    SamplerResponse response = firehoseSampler.sample(firehoseFactory, null, new SamplerConfig(3, null, false, null));
    String cacheKey = response.getCacheKey();

    Assert.assertNotNull(cacheKey);
    Assert.assertEquals(3, (int) response.getNumRowsRead());
    Assert.assertEquals(0, (int) response.getNumRowsIndexed());
    Assert.assertEquals(3, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    Assert.assertEquals(new SamplerResponseRow(getTestRows().get(0).toString(), null, true, null), data.get(0));
    Assert.assertEquals(new SamplerResponseRow(getTestRows().get(1).toString(), null, true, null), data.get(1));
    Assert.assertEquals(new SamplerResponseRow(getTestRows().get(2).toString(), null, true, null), data.get(2));

    response = firehoseSampler.sample(firehoseFactory, null, new SamplerConfig(3, cacheKey, false, null));

    Assert.assertTrue(!isCacheable() || cacheKey.equals(response.getCacheKey()));
    Assert.assertEquals(3, (int) response.getNumRowsRead());
    Assert.assertEquals(0, (int) response.getNumRowsIndexed());
    Assert.assertEquals(3, response.getData().size());
    Assert.assertEquals(data, response.getData());
  }

  @Test
  public void testMissingValueTimestampSpec()
  {
    FirehoseFactory firehoseFactory = getFirehoseFactory(getTestRows());

    ParseSpec parseSpec = getParseSpec(new TimestampSpec(null, null, DateTimes.of("1970")), new DimensionsSpec(null));
    DataSchema dataSchema = new DataSchema("sampler", getParser(parseSpec), null, null, null, OBJECT_MAPPER);

    SamplerResponse response = firehoseSampler.sample(firehoseFactory, dataSchema, null);

    Assert.assertEquals(6, (int) response.getNumRowsRead());
    Assert.assertEquals(6, (int) response.getNumRowsIndexed());
    Assert.assertEquals(6, response.getData().size());

    List<SamplerResponseRow> data = removeEmptyColumns(response.getData());

    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(0).toString(),
        ImmutableMap.of("__time", 0L, "t", "2019-04-22T12:00", "dim1", "foo", "met1", "1"),
        null,
        null
    ), data.get(0));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(1).toString(),
        ImmutableMap.of("__time", 0L, "t", "2019-04-22T12:00", "dim1", "foo", "met1", "2"),
        null,
        null
    ), data.get(1));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(2).toString(),
        ImmutableMap.of("__time", 0L, "t", "2019-04-22T12:01", "dim1", "foo", "met1", "3"),
        null,
        null
    ), data.get(2));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(3).toString(),
        ImmutableMap.of("__time", 0L, "t", "2019-04-22T12:00", "dim1", "foo2", "met1", "4"),
        null,
        null
    ), data.get(3));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(4).toString(),
        ImmutableMap.of("__time", 0L, "t", "2019-04-22T12:00", "dim1", "foo", "dim2", "bar", "met1", "5"),
        null,
        null
    ), data.get(4));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(5).toString(),
        ImmutableMap.of("__time", 0L, "t", "bad_timestamp", "dim1", "foo", "met1", "6"),
        null,
        null
    ), data.get(5));
  }

  @Test
  public void testWithTimestampSpec()
  {
    FirehoseFactory firehoseFactory = getFirehoseFactory(getTestRows());

    ParseSpec parseSpec = getParseSpec(new TimestampSpec("t", null, null), new DimensionsSpec(null));
    DataSchema dataSchema = new DataSchema("sampler", getParser(parseSpec), null, null, null, OBJECT_MAPPER);

    SamplerResponse response = firehoseSampler.sample(firehoseFactory, dataSchema, null);

    Assert.assertEquals(6, (int) response.getNumRowsRead());
    Assert.assertEquals(5, (int) response.getNumRowsIndexed());
    Assert.assertEquals(6, response.getData().size());

    List<SamplerResponseRow> data = removeEmptyColumns(response.getData());

    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(0).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "met1", "1"),
        null,
        null
    ), data.get(0));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(1).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "met1", "2"),
        null,
        null
    ), data.get(1));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(2).toString(),
        ImmutableMap.of("__time", 1555934460000L, "dim1", "foo", "met1", "3"),
        null,
        null
    ), data.get(2));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(3).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo2", "met1", "4"),
        null,
        null
    ), data.get(3));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(4).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "dim2", "bar", "met1", "5"),
        null,
        null
    ), data.get(4));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(5).toString(),
        null,
        true,
        getUnparseableTimestampString()
    ), data.get(5));
  }

  @Test
  public void testWithDimensionSpec()
  {
    FirehoseFactory firehoseFactory = getFirehoseFactory(getTestRows());

    ParseSpec parseSpec = getParseSpec(
        new TimestampSpec("t", null, null),
        new DimensionsSpec(ImmutableList.of(
            StringDimensionSchema.create("dim1"),
            StringDimensionSchema.create("met1")
        ))
    );
    DataSchema dataSchema = new DataSchema("sampler", getParser(parseSpec), null, null, null, OBJECT_MAPPER);

    SamplerResponse response = firehoseSampler.sample(firehoseFactory, dataSchema, null);

    Assert.assertEquals(6, (int) response.getNumRowsRead());
    Assert.assertEquals(5, (int) response.getNumRowsIndexed());
    Assert.assertEquals(6, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(0).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "met1", "1"),
        null,
        null
    ), data.get(0));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(1).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "met1", "2"),
        null,
        null
    ), data.get(1));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(2).toString(),
        ImmutableMap.of("__time", 1555934460000L, "dim1", "foo", "met1", "3"),
        null,
        null
    ), data.get(2));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(3).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo2", "met1", "4"),
        null,
        null
    ), data.get(3));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(4).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "met1", "5"),
        null,
        null
    ), data.get(4));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(5).toString(),
        null,
        true,
        getUnparseableTimestampString()
    ), data.get(5));
  }

  @Test
  public void testWithNoRollup()
  {
    FirehoseFactory firehoseFactory = getFirehoseFactory(getTestRows());

    ParseSpec parseSpec = getParseSpec(new TimestampSpec("t", null, null), new DimensionsSpec(null));
    AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    GranularitySpec granularitySpec = new UniformGranularitySpec(Granularities.DAY, Granularities.HOUR, false, null);
    DataSchema dataSchema = new DataSchema(
        "sampler",
        getParser(parseSpec),
        aggregatorFactories,
        granularitySpec,
        null,
        OBJECT_MAPPER
    );

    SamplerResponse response = firehoseSampler.sample(firehoseFactory, dataSchema, null);

    Assert.assertEquals(6, (int) response.getNumRowsRead());
    Assert.assertEquals(5, (int) response.getNumRowsIndexed());
    Assert.assertEquals(6, response.getData().size());

    List<SamplerResponseRow> data = removeEmptyColumns(response.getData());

    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(0).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "met1", 1L),
        null,
        null
    ), data.get(0));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(1).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "met1", 2L),
        null,
        null
    ), data.get(1));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(2).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "met1", 3L),
        null,
        null
    ), data.get(2));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(3).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo2", "met1", 4L),
        null,
        null
    ), data.get(3));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(4).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "dim2", "bar", "met1", 5L),
        null,
        null
    ), data.get(4));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(5).toString(),
        null,
        true,
        getUnparseableTimestampString()
    ), data.get(5));
  }

  @Test
  public void testWithRollup()
  {
    FirehoseFactory firehoseFactory = getFirehoseFactory(getTestRows());

    ParseSpec parseSpec = getParseSpec(new TimestampSpec("t", null, null), new DimensionsSpec(null));
    AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    GranularitySpec granularitySpec = new UniformGranularitySpec(Granularities.DAY, Granularities.HOUR, true, null);
    DataSchema dataSchema = new DataSchema(
        "sampler",
        getParser(parseSpec),
        aggregatorFactories,
        granularitySpec,
        null,
        OBJECT_MAPPER
    );

    SamplerResponse response = firehoseSampler.sample(firehoseFactory, dataSchema, null);

    Assert.assertEquals(6, (int) response.getNumRowsRead());
    Assert.assertEquals(5, (int) response.getNumRowsIndexed());
    Assert.assertEquals(4, response.getData().size());

    List<SamplerResponseRow> data = removeEmptyColumns(response.getData());

    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(0).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "met1", 6L),
        null,
        null
    ), data.get(0));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(3).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo2", "met1", 4L),
        null,
        null
    ), data.get(1));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(4).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "dim2", "bar", "met1", 5L),
        null,
        null
    ), data.get(2));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(5).toString(),
        null,
        true,
        getUnparseableTimestampString()
    ), data.get(3));
  }

  @Test
  public void testWithMoreRollup()
  {
    FirehoseFactory firehoseFactory = getFirehoseFactory(getTestRows());

    ParseSpec parseSpec = getParseSpec(
        new TimestampSpec("t", null, null),
        new DimensionsSpec(ImmutableList.of(StringDimensionSchema.create("dim1")))
    );
    AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    GranularitySpec granularitySpec = new UniformGranularitySpec(Granularities.DAY, Granularities.HOUR, true, null);
    DataSchema dataSchema = new DataSchema(
        "sampler",
        getParser(parseSpec),
        aggregatorFactories,
        granularitySpec,
        null,
        OBJECT_MAPPER
    );

    SamplerResponse response = firehoseSampler.sample(firehoseFactory, dataSchema, null);

    Assert.assertEquals(6, (int) response.getNumRowsRead());
    Assert.assertEquals(5, (int) response.getNumRowsIndexed());
    Assert.assertEquals(3, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(0).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "met1", 11L),
        null,
        null
    ), data.get(0));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(3).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo2", "met1", 4L),
        null,
        null
    ), data.get(1));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(5).toString(),
        null,
        true,
        getUnparseableTimestampString()
    ), data.get(2));
  }

  @Test
  public void testWithMoreRollupCacheReplay()
  {
    FirehoseFactory firehoseFactory = getFirehoseFactory(getTestRows());

    ParseSpec parseSpec = getParseSpec(
        new TimestampSpec("t", null, null),
        new DimensionsSpec(ImmutableList.of(StringDimensionSchema.create("dim1")))
    );
    AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    GranularitySpec granularitySpec = new UniformGranularitySpec(Granularities.DAY, Granularities.HOUR, true, null);
    DataSchema dataSchema = new DataSchema(
        "sampler",
        getParser(parseSpec),
        aggregatorFactories,
        granularitySpec,
        null,
        OBJECT_MAPPER
    );

    SamplerResponse response = firehoseSampler.sample(firehoseFactory, dataSchema, null);
    String cacheKey = response.getCacheKey();

    response = firehoseSampler.sample(firehoseFactory, dataSchema, new SamplerConfig(null, cacheKey, false, null));

    Assert.assertTrue(!isCacheable() || cacheKey.equals(response.getCacheKey()));

    Assert.assertEquals(6, (int) response.getNumRowsRead());
    Assert.assertEquals(5, (int) response.getNumRowsIndexed());
    Assert.assertEquals(3, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(0).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "met1", 11L),
        null,
        null
    ), data.get(0));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(3).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo2", "met1", 4L),
        null,
        null
    ), data.get(1));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(5).toString(),
        null,
        true,
        getUnparseableTimestampString()
    ), data.get(2));
  }

  @Test
  public void testWithTransformsAutoDimensions()
  {
    FirehoseFactory firehoseFactory = getFirehoseFactory(getTestRows());

    ParseSpec parseSpec = getParseSpec(
        new TimestampSpec("t", null, null),
        new DimensionsSpec(null)
    );
    AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    GranularitySpec granularitySpec = new UniformGranularitySpec(Granularities.DAY, Granularities.HOUR, true, null);
    TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(new ExpressionTransform("dim1PlusBar", "concat(dim1, 'bar')", TestExprMacroTable.INSTANCE))
    );

    DataSchema dataSchema = new DataSchema(
        "sampler",
        getParser(parseSpec),
        aggregatorFactories,
        granularitySpec,
        transformSpec,
        OBJECT_MAPPER
    );

    SamplerResponse response = firehoseSampler.sample(firehoseFactory, dataSchema, null);

    Assert.assertEquals(6, (int) response.getNumRowsRead());
    Assert.assertEquals(5, (int) response.getNumRowsIndexed());
    Assert.assertEquals(4, response.getData().size());

    List<SamplerResponseRow> data = removeEmptyColumns(response.getData());

    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(0).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "met1", 6L),
        null,
        null
    ), data.get(0));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(3).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo2", "met1", 4L),
        null,
        null
    ), data.get(1));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(4).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "dim2", "bar", "met1", 5L),
        null,
        null
    ), data.get(2));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(5).toString(),
        null,
        true,
        getUnparseableTimestampString()
    ), data.get(3));
  }

  @Test
  public void testWithTransformsDimensionsSpec()
  {
    // There's a bug in the CSV parser that does not allow a column added by a transform to be put in the dimensions
    // list if the 'columns' field is specified (it will complain that the dimensionName is not a valid column).
    if (ParserType.STR_CSV.equals(parserType)) {
      return;
    }

    FirehoseFactory firehoseFactory = getFirehoseFactory(getTestRows());

    ParseSpec parseSpec = getParseSpec(
        new TimestampSpec("t", null, null),
        new DimensionsSpec(ImmutableList.of(StringDimensionSchema.create("dim1PlusBar")))
    );
    AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    GranularitySpec granularitySpec = new UniformGranularitySpec(Granularities.DAY, Granularities.HOUR, true, null);
    TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(new ExpressionTransform("dim1PlusBar", "concat(dim1 + 'bar')", TestExprMacroTable.INSTANCE))
    );

    DataSchema dataSchema = new DataSchema(
        "sampler",
        getParser(parseSpec),
        aggregatorFactories,
        granularitySpec,
        transformSpec,
        OBJECT_MAPPER
    );

    SamplerResponse response = firehoseSampler.sample(firehoseFactory, dataSchema, null);

    Assert.assertEquals(6, (int) response.getNumRowsRead());
    Assert.assertEquals(5, (int) response.getNumRowsIndexed());
    Assert.assertEquals(3, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(0).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1PlusBar", "foobar", "met1", 11L),
        null,
        null
    ), data.get(0));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(3).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1PlusBar", "foo2bar", "met1", 4L),
        null,
        null
    ), data.get(1));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(5).toString(),
        null,
        true,
        getUnparseableTimestampString()
    ), data.get(2));
  }

  @Test
  public void testWithFilter()
  {
    FirehoseFactory firehoseFactory = getFirehoseFactory(getTestRows());

    ParseSpec parseSpec = getParseSpec(
        new TimestampSpec("t", null, null),
        new DimensionsSpec(null)
    );
    AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    GranularitySpec granularitySpec = new UniformGranularitySpec(Granularities.DAY, Granularities.HOUR, true, null);
    TransformSpec transformSpec = new TransformSpec(new SelectorDimFilter("dim1", "foo", null), null);
    DataSchema dataSchema = new DataSchema(
        "sampler",
        getParser(parseSpec),
        aggregatorFactories,
        granularitySpec,
        transformSpec,
        OBJECT_MAPPER
    );

    SamplerResponse response = firehoseSampler.sample(firehoseFactory, dataSchema, null);

    Assert.assertEquals(5, (int) response.getNumRowsRead());
    Assert.assertEquals(4, (int) response.getNumRowsIndexed());
    Assert.assertEquals(3, response.getData().size());

    List<SamplerResponseRow> data = removeEmptyColumns(response.getData());

    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(0).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "met1", 6L),
        null,
        null
    ), data.get(0));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(4).toString(),
        ImmutableMap.of("__time", 1555934400000L, "dim1", "foo", "dim2", "bar", "met1", 5L),
        null,
        null
    ), data.get(1));
    Assert.assertEquals(new SamplerResponseRow(
        getTestRows().get(5).toString(),
        null,
        true,
        getUnparseableTimestampString()
    ), data.get(2));
  }

  private Map<String, Object> getParser(ParseSpec parseSpec)
  {
    return OBJECT_MAPPER.convertValue(
        ParserType.MAP.equals(parserType)
        ? new MapInputRowParser(parseSpec)
        : new StringInputRowParser(parseSpec, StandardCharsets.UTF_8.name()),
        new TypeReference<Map<String, Object>>()
        {
        }
    );
  }

  private List<Object> getTestRows()
  {
    switch (parserType) {
      case MAP:
        return MAP_ROWS;
      case STR_JSON:
        return STR_JSON_ROWS;
      case STR_CSV:
        return STR_CSV_ROWS;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private FirehoseFactory<? extends InputRowParser> getFirehoseFactory(List<Object> seedRows)
  {
    return ParserType.MAP.equals(parserType)
           ? new TestFirehose.TestFirehoseFactory(false, seedRows)
           : new TestFirehose.TestAbstractTextFilesFirehoseFactory(false, seedRows);
  }

  private boolean isCacheable()
  {
    return !ParserType.MAP.equals(parserType);
  }

  private ParseSpec getParseSpec(TimestampSpec timestampSpec, DimensionsSpec dimensionsSpec)
  {
    return ParserType.STR_CSV.equals(parserType) ? new DelimitedParseSpec(
        timestampSpec,
        dimensionsSpec,
        ",",
        null,
        ImmutableList.of("t", "dim1", "dim2", "met1"),
        false,
        0
    ) : new JSONParseSpec(timestampSpec, dimensionsSpec, null, null);
  }

  private String getUnparseableTimestampString()
  {
    return ParserType.STR_CSV.equals(parserType)
           ? (USE_DEFAULT_VALUE_FOR_NULL
              ? "Unparseable timestamp found! Event: {t=bad_timestamp, dim1=foo, dim2=null, met1=6}"
              : "Unparseable timestamp found! Event: {t=bad_timestamp, dim1=foo, dim2=, met1=6}")
           : "Unparseable timestamp found! Event: {t=bad_timestamp, dim1=foo, met1=6}";
  }

  private List<SamplerResponseRow> removeEmptyColumns(List<SamplerResponseRow> rows)
  {
    return USE_DEFAULT_VALUE_FOR_NULL
           ? rows
           : rows.stream().map(x -> x.withParsed(removeEmptyValues(x.getParsed()))).collect(Collectors.toList());
  }

  @Nullable
  private Map<String, Object> removeEmptyValues(Map<String, Object> data)
  {
    return data == null
           ? null : data.entrySet()
                        .stream()
                        .filter(x -> !(x.getValue() instanceof String) || !((String) x.getValue()).isEmpty())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
