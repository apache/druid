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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import net.thisptr.jackson.jq.internal.misc.Lists;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.client.indexing.SamplerResponse.SamplerResponseRow;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DelimitedParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.seekablestream.RecordSupplierInputSource;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.collect.Utils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class InputSourceSamplerTest extends InitializedNullHandlingTest
{
  private enum ParserType
  {
    STR_JSON, STR_CSV
  }

  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  private static final List<String> STR_JSON_ROWS = ImmutableList.of(
      "{ \"t\": \"2019-04-22T12:00\", \"dim1\": \"foo\", \"met1\": 1 }",
      "{ \"t\": \"2019-04-22T12:00\", \"dim1\": \"foo\", \"met1\": 2 }",
      "{ \"t\": \"2019-04-22T12:01\", \"dim1\": \"foo\", \"met1\": 3 }",
      "{ \"t\": \"2019-04-22T12:00\", \"dim1\": \"foo2\", \"met1\": 4 }",
      "{ \"t\": \"2019-04-22T12:00\", \"dim1\": \"foo\", \"dim2\": \"bar\", \"met1\": 5 }",
      "{ \"t\": \"bad_timestamp\", \"dim1\": \"foo\", \"met1\": 6 }"
  );

  private static final List<String> STR_CSV_ROWS = ImmutableList.of(
      "2019-04-22T12:00,foo,,1",
      "2019-04-22T12:00,foo,,2",
      "2019-04-22T12:01,foo,,3",
      "2019-04-22T12:00,foo2,,4",
      "2019-04-22T12:00,foo,bar,5",
      "bad_timestamp,foo,,6"
  );


  private List<Map<String, Object>> mapOfRows;
  private InputSourceSampler inputSourceSampler;
  private ParserType parserType;
  private boolean useInputFormatApi;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Parameterized.Parameters(name = "parserType = {0}, useInputFormatApi={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    OBJECT_MAPPER.registerModules(new SamplerModule().getJacksonModules());
    return ImmutableList.of(
        new Object[]{ParserType.STR_JSON, false},
        new Object[]{ParserType.STR_JSON, true},
        new Object[]{ParserType.STR_CSV, false},
        new Object[]{ParserType.STR_CSV, true}
    );
  }

  public InputSourceSamplerTest(ParserType parserType, boolean useInputFormatApi)
  {
    this.parserType = parserType;
    this.useInputFormatApi = useInputFormatApi;
  }

  @Before
  public void setupTest()
  {
    inputSourceSampler = new InputSourceSampler(OBJECT_MAPPER);

    mapOfRows = new ArrayList<>();
    final List<String> columns = ImmutableList.of("t", "dim1", "dim2", "met1");
    for (String row : STR_CSV_ROWS) {
      final List<Object> values = new ArrayList<>();
      final String[] tokens = row.split(",");
      for (int i = 0; i < tokens.length; i++) {
        if (i < tokens.length - 1) {
          values.add("".equals(tokens[i]) ? null : tokens[i]);
        } else {
          values.add(Integer.parseInt(tokens[i]));
        }
      }
      mapOfRows.add(Utils.zipMapPartial(columns, values));
    }
  }

  @Test
  public void testNoParams()
  {
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("inputSource required");

    inputSourceSampler.sample(null, null, null, null);
  }

  @Test
  public void testNoDataSchema()
  {
    final InputSource inputSource = createInputSource(getTestRows());
    final SamplerResponse response = inputSourceSampler.sample(inputSource, createInputFormat(), null, null);

    Assert.assertEquals(6, response.getNumRowsRead());
    Assert.assertEquals(0, response.getNumRowsIndexed());
    Assert.assertEquals(6, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(0),
            null,
            true,
            unparseableTimestampErrorString(data.get(0).getInput(), 1)
        ),
        data.get(0)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(1),
            null,
            true,
            unparseableTimestampErrorString(data.get(1).getInput(), 2)
        ),
        data.get(1)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(2),
            null,
            true,
            unparseableTimestampErrorString(data.get(2).getInput(), 3)
        ),
        data.get(2)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(3),
            null,
            true,
            unparseableTimestampErrorString(data.get(3).getInput(), 4)
        ),
        data.get(3)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(4),
            null,
            true,
            unparseableTimestampErrorString(data.get(4).getInput(), 5)
        ),
        data.get(4)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(5),
            null,
            true,
            unparseableTimestampErrorString(data.get(5).getInput(), 6)
        ),
        data.get(5)
    );
  }

  @Test
  public void testNoDataSchemaNumRows()
  {
    final InputSource inputSource = createInputSource(getTestRows());
    final SamplerResponse response = inputSourceSampler.sample(
        inputSource,
        createInputFormat(),
        null,
        new SamplerConfig(3, null, null, null)
    );

    Assert.assertEquals(3, response.getNumRowsRead());
    Assert.assertEquals(0, response.getNumRowsIndexed());
    Assert.assertEquals(3, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(0),
            null,
            true,
            unparseableTimestampErrorString(data.get(0).getInput(), 1)
        ),
        data.get(0)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(1),
            null,
            true,
            unparseableTimestampErrorString(data.get(1).getInput(), 2)
        ),
        data.get(1)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(2),
            null,
            true,
            unparseableTimestampErrorString(data.get(2).getInput(), 3)
        ),
        data.get(2)
    );
  }

  @Test
  public void testMissingValueTimestampSpec() throws IOException
  {
    final TimestampSpec timestampSpec = new TimestampSpec(null, null, DateTimes.of("1970"));
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(null);
    final DataSchema dataSchema = createDataSchema(timestampSpec, dimensionsSpec, null, null, null);
    final InputSource inputSource = createInputSource(getTestRows());
    final InputFormat inputFormat = createInputFormat();

    SamplerResponse response = inputSourceSampler.sample(inputSource, inputFormat, dataSchema, null);

    Assert.assertEquals(6, response.getNumRowsRead());
    Assert.assertEquals(6, response.getNumRowsIndexed());
    Assert.assertEquals(6, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(0),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 0L)
                .put("t", "2019-04-22T12:00")
                .put("dim2", null)
                .put("dim1", "foo")
                .put("met1", "1")
                .build(),
            null,
            null
        ),
        data.get(0)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(1),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 0L)
                .put("t", "2019-04-22T12:00")
                .put("dim2", null)
                .put("dim1", "foo")
                .put("met1", "2")
                .build(),
            null,
            null
        ),
        data.get(1)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(2),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 0L)
                .put("t", "2019-04-22T12:01")
                .put("dim2", null)
                .put("dim1", "foo")
                .put("met1", "3")
                .build(),
            null,
            null
        ),
        data.get(2)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(3),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 0L)
                .put("t", "2019-04-22T12:00")
                .put("dim2", null)
                .put("dim1", "foo2")
                .put("met1", "4")
                .build(),
            null,
            null
        ),
        data.get(3)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(4),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 0L)
                .put("t", "2019-04-22T12:00")
                .put("dim2", "bar")
                .put("dim1", "foo")
                .put("met1", "5")
                .build(),
            null,
            null
        ),
        data.get(4)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(5),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 0L)
                .put("t", "bad_timestamp")
                .put("dim2", null)
                .put("dim1", "foo")
                .put("met1", "6")
                .build(),
            null,
            null
        ),
        data.get(5)
    );
  }

  @Test
  public void testWithTimestampSpec() throws IOException
  {
    final TimestampSpec timestampSpec = new TimestampSpec("t", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(null);
    final DataSchema dataSchema = createDataSchema(timestampSpec, dimensionsSpec, null, null, null);
    final InputSource inputSource = createInputSource(getTestRows());
    final InputFormat inputFormat = createInputFormat();

    SamplerResponse response = inputSourceSampler.sample(inputSource, inputFormat, dataSchema, null);

    Assert.assertEquals(6, response.getNumRowsRead());
    Assert.assertEquals(5, response.getNumRowsIndexed());
    Assert.assertEquals(6, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(0),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim2", null)
                .put("dim1", "foo")
                .put("met1", "1")
                .build(),
            null,
            null
        ),
        data.get(0)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(1),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim2", null)
                .put("dim1", "foo")
                .put("met1", "2")
                .build(),
            null,
            null
        ),
        data.get(1)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(2),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934460000L)
                .put("dim2", null)
                .put("dim1", "foo")
                .put("met1", "3")
                .build(),
            null,
            null
        ),
        data.get(2)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(3),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim2", null)
                .put("dim1", "foo2")
                .put("met1", "4")
                .build(),
            null,
            null
        ),
        data.get(3)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(4),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim2", "bar")
                .put("dim1", "foo")
                .put("met1", "5")
                .build(),
            null,
            null
        ),
        data.get(4)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(5),
            null,
            true,
            getUnparseableTimestampString()
        ),
        data.get(5)
    );
  }

  @Test
  public void testWithDimensionSpec() throws IOException
  {
    final TimestampSpec timestampSpec = new TimestampSpec("t", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        ImmutableList.of(StringDimensionSchema.create("dim1"), StringDimensionSchema.create("met1"))
    );
    final DataSchema dataSchema = createDataSchema(timestampSpec, dimensionsSpec, null, null, null);
    final InputSource inputSource = createInputSource(getTestRows());
    final InputFormat inputFormat = createInputFormat();

    SamplerResponse response = inputSourceSampler.sample(inputSource, inputFormat, dataSchema, null);

    Assert.assertEquals(6, response.getNumRowsRead());
    Assert.assertEquals(5, response.getNumRowsIndexed());
    Assert.assertEquals(6, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(0),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("met1", "1")
                .build(),
            null,
            null
        ),
        data.get(0)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(1),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("met1", "2")
                .build(),
            null,
            null
        ),
        data.get(1)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(2),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934460000L)
                .put("dim1", "foo")
                .put("met1", "3")
                .build(),
            null,
            null
        ),
        data.get(2)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(3),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo2")
                .put("met1", "4")
                .build(),
            null,
            null
        ),
        data.get(3)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(4),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("met1", "5")
                .build(),
            null,
            null
        ),
        data.get(4)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(5),
            null,
            true,
            getUnparseableTimestampString()
        ),
        data.get(5)
    );
  }

  @Test
  public void testWithNoRollup() throws IOException
  {
    final TimestampSpec timestampSpec = new TimestampSpec("t", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(null);
    final AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.HOUR,
        false,
        null
    );
    final DataSchema dataSchema = createDataSchema(
        timestampSpec,
        dimensionsSpec,
        aggregatorFactories,
        granularitySpec,
        null
    );
    final InputSource inputSource = createInputSource(getTestRows());
    final InputFormat inputFormat = createInputFormat();

    SamplerResponse response = inputSourceSampler.sample(inputSource, inputFormat, dataSchema, null);

    Assert.assertEquals(6, response.getNumRowsRead());
    Assert.assertEquals(5, response.getNumRowsIndexed());
    Assert.assertEquals(6, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(0),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("dim2", null)
                .put("met1", 1L)
                .build(),
            null,
            null
        ),
        data.get(0)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(1),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("dim2", null)
                .put("met1", 2L)
                .build(),
            null,
            null
        ),
        data.get(1)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(2),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("dim2", null)
                .put("met1", 3L)
                .build(),
            null,
            null
        ),
        data.get(2)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(3),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo2")
                .put("dim2", null)
                .put("met1", 4L)
                .build(),
            null,
            null
        ),
        data.get(3)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(4),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("dim2", "bar")
                .put("met1", 5L)
                .build(),
            null,
            null
        ),
        data.get(4)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(5),
            null,
            true,
            getUnparseableTimestampString()
        ),
        data.get(5)
    );
  }

  @Test
  public void testWithRollup() throws IOException
  {
    final TimestampSpec timestampSpec = new TimestampSpec("t", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(null);
    final AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.HOUR,
        true,
        null
    );
    final DataSchema dataSchema = createDataSchema(
        timestampSpec,
        dimensionsSpec,
        aggregatorFactories,
        granularitySpec,
        null
    );
    final InputSource inputSource = createInputSource(getTestRows());
    final InputFormat inputFormat = createInputFormat();

    SamplerResponse response = inputSourceSampler.sample(inputSource, inputFormat, dataSchema, null);

    Assert.assertEquals(6, response.getNumRowsRead());
    Assert.assertEquals(5, response.getNumRowsIndexed());
    Assert.assertEquals(4, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(0),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("dim2", null)
                .put("met1", 6L)
                .build(),
            null,
            null
        ),
        data.get(0)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(3),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo2")
                .put("dim2", null)
                .put("met1", 4L)
                .build(),
            null,
            null
        ),
        data.get(1)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(4),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("dim2", "bar")
                .put("met1", 5L)
                .build(),
            null,
            null
        ),
        data.get(2)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(5),
            null,
            true,
            getUnparseableTimestampString()
        ),
        data.get(3)
    );
  }

  @Test
  public void testWithMoreRollup() throws IOException
  {
    final TimestampSpec timestampSpec = new TimestampSpec("t", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(ImmutableList.of(StringDimensionSchema.create("dim1")));
    final AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.HOUR,
        true,
        null
    );
    final DataSchema dataSchema = createDataSchema(
        timestampSpec,
        dimensionsSpec,
        aggregatorFactories,
        granularitySpec,
        null
    );
    final InputSource inputSource = createInputSource(getTestRows());
    final InputFormat inputFormat = createInputFormat();

    SamplerResponse response = inputSourceSampler.sample(inputSource, inputFormat, dataSchema, null);

    Assert.assertEquals(6, response.getNumRowsRead());
    Assert.assertEquals(5, response.getNumRowsIndexed());
    Assert.assertEquals(3, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(0),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("met1", 11L)
                .build(),
            null,
            null
        ),
        data.get(0)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(3),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo2")
                .put("met1", 4L)
                .build(),
            null,
            null
        ),
        data.get(1)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(5),
            null,
            true,
            getUnparseableTimestampString()
        ),
        data.get(2)
    );
  }

  @Test
  public void testWithTransformsAutoDimensions() throws IOException
  {
    final TimestampSpec timestampSpec = new TimestampSpec("t", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(null);
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(new ExpressionTransform("dim1PlusBar", "concat(dim1, 'bar')", TestExprMacroTable.INSTANCE))
    );
    final AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.HOUR,
        true,
        null
    );
    final DataSchema dataSchema = createDataSchema(
        timestampSpec,
        dimensionsSpec,
        aggregatorFactories,
        granularitySpec,
        transformSpec
    );
    final InputSource inputSource = createInputSource(getTestRows());
    final InputFormat inputFormat = createInputFormat();

    SamplerResponse response = inputSourceSampler.sample(inputSource, inputFormat, dataSchema, null);

    Assert.assertEquals(6, response.getNumRowsRead());
    Assert.assertEquals(5, response.getNumRowsIndexed());
    Assert.assertEquals(4, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(0),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("dim2", null)
                .put("met1", 6L)
                .build(),
            null,
            null
        ),
        data.get(0)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(3),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo2")
                .put("dim2", null)
                .put("met1", 4L)
                .build(),
            null,
            null
        ),
        data.get(1)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(4),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("dim2", "bar")
                .put("met1", 5L)
                .build(),
            null,
            null
        ),
        data.get(2)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(5),
            null,
            true,
            getUnparseableTimestampString()
        ),
        data.get(3)
    );
  }

  @Test
  public void testWithTransformsDimensionsSpec() throws IOException
  {
    final TimestampSpec timestampSpec = new TimestampSpec("t", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        ImmutableList.of(StringDimensionSchema.create("dim1PlusBar"))
    );
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(new ExpressionTransform("dim1PlusBar", "concat(dim1 + 'bar')", TestExprMacroTable.INSTANCE))
    );
    final AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.HOUR,
        true,
        null
    );
    final DataSchema dataSchema = createDataSchema(
        timestampSpec,
        dimensionsSpec,
        aggregatorFactories,
        granularitySpec,
        transformSpec
    );
    final InputSource inputSource = createInputSource(getTestRows());
    final InputFormat inputFormat = createInputFormat();

    SamplerResponse response = inputSourceSampler.sample(inputSource, inputFormat, dataSchema, null);

    Assert.assertEquals(6, response.getNumRowsRead());
    Assert.assertEquals(5, response.getNumRowsIndexed());
    Assert.assertEquals(3, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(0),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1PlusBar", "foobar")
                .put("met1", 11L)
                .build(),
            null,
            null
        ),
        data.get(0)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(3),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1PlusBar", "foo2bar")
                .put("met1", 4L)
                .build(),
            null,
            null
        ),
        data.get(1)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(5),
            null,
            true,
            getUnparseableTimestampString()
        ),
        data.get(2)
    );
  }

  @Test
  public void testWithFilter() throws IOException
  {
    final TimestampSpec timestampSpec = new TimestampSpec("t", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(null);
    final TransformSpec transformSpec = new TransformSpec(new SelectorDimFilter("dim1", "foo", null), null);
    final AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.HOUR,
        true,
        null
    );
    final DataSchema dataSchema = createDataSchema(
        timestampSpec,
        dimensionsSpec,
        aggregatorFactories,
        granularitySpec,
        transformSpec
    );
    final InputSource inputSource = createInputSource(getTestRows());
    final InputFormat inputFormat = createInputFormat();

    SamplerResponse response = inputSourceSampler.sample(inputSource, inputFormat, dataSchema, null);

    Assert.assertEquals(5, response.getNumRowsRead());
    Assert.assertEquals(4, response.getNumRowsIndexed());
    Assert.assertEquals(3, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(0),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("dim2", null)
                .put("met1", 6L)
                .build(),
            null,
            null
        ),
        data.get(0)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(4),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1", "foo")
                .put("dim2", "bar")
                .put("met1", 5L)
                .build(),
            null,
            null
        ),
        data.get(1)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(5),
            null,
            true,
            getUnparseableTimestampString()
        ),
        data.get(2)
    );
  }

  @Test
  public void testIndexParseException() throws IOException
  {
    final TimestampSpec timestampSpec = new TimestampSpec("t", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        ImmutableList.of(StringDimensionSchema.create("dim1PlusBar"))
    );
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(new ExpressionTransform("dim1PlusBar", "concat(dim1 + 'bar')", TestExprMacroTable.INSTANCE))
    );
    final AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.HOUR,
        true,
        null
    );
    final DataSchema dataSchema = createDataSchema(
        timestampSpec,
        dimensionsSpec,
        aggregatorFactories,
        granularitySpec,
        transformSpec
    );

    //
    // add a invalid row to cause parse exception when indexing
    //
    Map<String, Object> rawColumns4ParseExceptionRow = ImmutableMap.of("t", "2019-04-22T12:00",
                                                                       "dim1", "foo2",
                                                                       "met1", "invalidNumber");
    final List<String> inputTestRows = Lists.newArrayList(getTestRows());
    inputTestRows.add(ParserType.STR_CSV.equals(parserType) ?
                      "2019-04-22T12:00,foo2,,invalidNumber" :
                      OBJECT_MAPPER.writeValueAsString(rawColumns4ParseExceptionRow));

    final InputSource inputSource = createInputSource(inputTestRows);
    final InputFormat inputFormat = createInputFormat();

    SamplerResponse response = inputSourceSampler.sample(inputSource, inputFormat, dataSchema, null);

    Assert.assertEquals(7, response.getNumRowsRead());
    Assert.assertEquals(5, response.getNumRowsIndexed());
    Assert.assertEquals(4, response.getData().size());

    List<SamplerResponseRow> data = response.getData();

    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(0),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1PlusBar", "foobar")
                .put("met1", 11L)
                .build(),
            null,
            null
        ),
        data.get(0)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(3),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1PlusBar", "foo2bar")
                .put("met1", 4L)
                .build(),
            null,
            null
        ),
        data.get(1)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            getRawColumns().get(5),
            null,
            true,
            getUnparseableTimestampString()
        ),
        data.get(2)
    );

    //
    // the last row has parse exception when indexing, check if rawColumns and exception message match the expected
    //
    String indexParseExceptioMessage = ParserType.STR_CSV.equals(parserType)
           ? "Found unparseable columns in row: [SamplerInputRow{row=TransformedInputRow{row={timestamp=2019-04-22T12:00:00.000Z, event={t=2019-04-22T12:00, dim1=foo2, dim2=null, met1=invalidNumber}, dimensions=[dim1PlusBar]}}}], exceptions: [Unable to parse value[invalidNumber] for field[met1]]"
           : "Found unparseable columns in row: [SamplerInputRow{row=TransformedInputRow{row={timestamp=2019-04-22T12:00:00.000Z, event={t=2019-04-22T12:00, dim1=foo2, met1=invalidNumber}, dimensions=[dim1PlusBar]}}}], exceptions: [Unable to parse value[invalidNumber] for field[met1]]";
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            rawColumns4ParseExceptionRow,
            null,
            true,
            indexParseExceptioMessage
        ),
        data.get(3)
    );
  }

  /**
   *
   * This case tests sampling for multiple json lines in one text block
   * Currently only RecordSupplierInputSource supports this kind of input, see https://github.com/apache/druid/pull/10383 for more information
   *
   * This test combines illegal json block and legal json block together to verify:
   * 1. all lines in the illegal json block should not be parsed
   * 2. the illegal json block should not affect the processing of the 2nd record
   * 3. all lines in legal json block should be parsed successfully
   *
   */
  @Test
  public void testMultipleJsonStringInOneBlock() throws IOException
  {
    if (!ParserType.STR_JSON.equals(parserType) || !useInputFormatApi) {
      return;
    }

    final TimestampSpec timestampSpec = new TimestampSpec("t", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        ImmutableList.of(StringDimensionSchema.create("dim1PlusBar"))
    );
    final TransformSpec transformSpec = new TransformSpec(
        null,
        ImmutableList.of(new ExpressionTransform("dim1PlusBar", "concat(dim1 + 'bar')", TestExprMacroTable.INSTANCE))
    );
    final AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.HOUR,
        true,
        null
    );
    final DataSchema dataSchema = createDataSchema(
        timestampSpec,
        dimensionsSpec,
        aggregatorFactories,
        granularitySpec,
        transformSpec
    );

    List<String> jsonBlockList = ImmutableList.of(
        // include the line which can't be parsed into JSON object to form a illegal json block
        String.join("", STR_JSON_ROWS),

        // exclude the last line to form a legal json block
        STR_JSON_ROWS.stream().limit(STR_JSON_ROWS.size() - 1).collect(Collectors.joining())
    );

    SamplerResponse response = inputSourceSampler.sample(
        new RecordSupplierInputSource("topicName", new TestRecordSupplier(jsonBlockList), true, 3000),
        createInputFormat(),
        dataSchema,
        new SamplerConfig(200, 3000/*default timeout is 10s, shorten it to speed up*/, null, null)
    );

    //
    // the 1st json block contains STR_JSON_ROWS.size() lines, and 2nd json block contains STR_JSON_ROWS.size()-1 lines
    // together there should STR_JSON_ROWS.size() * 2 - 1 lines
    //
    int illegalRows = STR_JSON_ROWS.size();
    int legalRows = STR_JSON_ROWS.size() - 1;
    Assert.assertEquals(illegalRows + legalRows, response.getNumRowsRead());
    Assert.assertEquals(legalRows, response.getNumRowsIndexed());
    Assert.assertEquals(illegalRows + 2, response.getData().size());

    List<SamplerResponseRow> data = response.getData();
    List<Map<String, Object>> rawColumnList = this.getRawColumns();
    int index = 0;

    //
    // first n rows are related to the first json block which fails to parse
    //
    String parseExceptionMessage;
    parseExceptionMessage = "Timestamp[bad_timestamp] is unparseable! Event: {t=bad_timestamp, dim1=foo, met1=6}";

    for (; index < illegalRows; index++) {
      assertEqualsSamplerResponseRow(
          new SamplerResponseRow(
              rawColumnList.get(index),
              null,
              true,
              parseExceptionMessage
          ),
          data.get(index)
      );
    }

    //
    // following are parsed rows for legal json block
    //
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            rawColumnList.get(0),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1PlusBar", "foobar")
                .put("met1", 11L)
                .build(),
            null,
            null
        ),
        data.get(index++)
    );
    assertEqualsSamplerResponseRow(
        new SamplerResponseRow(
            rawColumnList.get(3),
            new SamplerTestUtils.MapAllowingNullValuesBuilder<String, Object>()
                .put("__time", 1555934400000L)
                .put("dim1PlusBar", "foo2bar")
                .put("met1", 4L)
                .build(),
            null,
            null
        ),
        data.get(index)
    );
  }

  @Test(expected = SamplerException.class)
  public void testReaderCreationException()
  {
    InputSource failingReaderInputSource = new InputSource()
    {
      @Override
      public boolean isSplittable()
      {
        return false;
      }

      @Override
      public boolean needsFormat()
      {
        return false;
      }

      @Override
      public InputSourceReader reader(
          InputRowSchema inputRowSchema,
          @Nullable InputFormat inputFormat,
          File temporaryDirectory
      )
      {
        throw new RuntimeException();
      }
    };
    inputSourceSampler.sample(failingReaderInputSource, null, null, null);
  }

  @Test
  public void testRowLimiting() throws IOException
  {
    final TimestampSpec timestampSpec = new TimestampSpec("t", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(null);
    final AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.HOUR,
        true,
        null
    );
    final DataSchema dataSchema = createDataSchema(
        timestampSpec,
        dimensionsSpec,
        aggregatorFactories,
        granularitySpec,
        null
    );
    final InputSource inputSource = createInputSource(getTestRows());
    final InputFormat inputFormat = createInputFormat();

    SamplerResponse response = inputSourceSampler.sample(
        inputSource,
        inputFormat,
        dataSchema,
        new SamplerConfig(4, null, null, null)
    );

    Assert.assertEquals(4, response.getNumRowsRead());
    Assert.assertEquals(4, response.getNumRowsIndexed());
    Assert.assertEquals(2, response.getData().size());

  }

  @Test
  public void testMaxBytesInMemoryLimiting() throws IOException
  {
    final TimestampSpec timestampSpec = new TimestampSpec("t", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(null);
    final AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.HOUR,
        true,
        null
    );
    final DataSchema dataSchema = createDataSchema(
        timestampSpec,
        dimensionsSpec,
        aggregatorFactories,
        granularitySpec,
        null
    );
    final InputSource inputSource = createInputSource(getTestRows());
    final InputFormat inputFormat = createInputFormat();

    SamplerResponse response = inputSourceSampler.sample(
        inputSource,
        inputFormat,
        dataSchema,
        new SamplerConfig(null, null, HumanReadableBytes.valueOf(256), null)
    );

    Assert.assertEquals(4, response.getNumRowsRead());
    Assert.assertEquals(4, response.getNumRowsIndexed());
    Assert.assertEquals(2, response.getData().size());
  }

  @Test
  public void testMaxClientResponseBytesLimiting() throws IOException
  {
    final TimestampSpec timestampSpec = new TimestampSpec("t", null, null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(null);
    final AggregatorFactory[] aggregatorFactories = {new LongSumAggregatorFactory("met1", "met1")};
    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularities.DAY,
        Granularities.HOUR,
        true,
        null
    );
    final DataSchema dataSchema = createDataSchema(
        timestampSpec,
        dimensionsSpec,
        aggregatorFactories,
        granularitySpec,
        null
    );
    final InputSource inputSource = createInputSource(getTestRows());
    final InputFormat inputFormat = createInputFormat();

    SamplerResponse response = inputSourceSampler.sample(
        inputSource,
        inputFormat,
        dataSchema,
        new SamplerConfig(null, null, null, HumanReadableBytes.valueOf(300))
    );

    Assert.assertEquals(4, response.getNumRowsRead());
    Assert.assertEquals(4, response.getNumRowsIndexed());
    Assert.assertEquals(2, response.getData().size());
  }

  private List<String> getTestRows()
  {
    switch (parserType) {
      case STR_JSON:
        return STR_JSON_ROWS;
      case STR_CSV:
        return STR_CSV_ROWS;
      default:
        throw new IAE("Unknown parser type: %s", parserType);
    }
  }

  private List<Map<String, Object>> getRawColumns()
  {
    switch (parserType) {
      case STR_JSON:
        return mapOfRows.stream().map(this::removeEmptyValues).collect(Collectors.toList());
      case STR_CSV:
        return mapOfRows;
      default:
        throw new IAE("Unknown parser type: %s", parserType);
    }
  }

  private InputFormat createInputFormat()
  {
    switch (parserType) {
      case STR_JSON:
        return new JsonInputFormat(null, null, null, null, null);
      case STR_CSV:
        return new CsvInputFormat(ImmutableList.of("t", "dim1", "dim2", "met1"), null, null, false, 0);
      default:
        throw new IAE("Unknown parser type: %s", parserType);
    }
  }

  private InputRowParser createInputRowParser(TimestampSpec timestampSpec, DimensionsSpec dimensionsSpec)
  {
    switch (parserType) {
      case STR_JSON:
        return new StringInputRowParser(new JSONParseSpec(timestampSpec, dimensionsSpec, null, null, null));
      case STR_CSV:
        return new StringInputRowParser(
            new DelimitedParseSpec(
                timestampSpec,
                dimensionsSpec,
                ",",
                null,
                ImmutableList.of("t", "dim1", "dim2", "met1"),
                false,
                0
            )
        );
      default:
        throw new IAE("Unknown parser type: %s", parserType);
    }
  }

  private DataSchema createDataSchema(
      @Nullable TimestampSpec timestampSpec,
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable AggregatorFactory[] aggregators,
      @Nullable GranularitySpec granularitySpec,
      @Nullable TransformSpec transformSpec
  ) throws IOException
  {
    if (useInputFormatApi) {
      return new DataSchema(
          "sampler",
          timestampSpec,
          dimensionsSpec,
          aggregators,
          granularitySpec,
          transformSpec
      );
    } else {
      final Map<String, Object> parserMap = getParserMap(createInputRowParser(timestampSpec, dimensionsSpec));
      return new DataSchema(
          "sampler",
          parserMap,
          aggregators,
          granularitySpec,
          transformSpec,
          OBJECT_MAPPER
      );
    }
  }

  private Map<String, Object> getParserMap(InputRowParser parser) throws IOException
  {
    if (useInputFormatApi) {
      throw new RuntimeException("Don't call this if useInputFormatApi = true");
    }
    return OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsBytes(parser), Map.class);
  }

  private InputSource createInputSource(List<String> rows)
  {
    final String data = String.join("\n", rows);
    return new InlineInputSource(data);
  }

  private String getUnparseableTimestampString()
  {
    return ParserType.STR_CSV.equals(parserType)
           ? "Timestamp[bad_timestamp] is unparseable! Event: {t=bad_timestamp, dim1=foo, dim2=null, met1=6} (Line: 6)"
           : "Timestamp[bad_timestamp] is unparseable! Event: {t=bad_timestamp, dim1=foo, met1=6} (Line: 6)";
  }

  private String unparseableTimestampErrorString(Map<String, Object> rawColumns, int line)
  {
    return StringUtils.format("Timestamp[null] is unparseable! Event: %s (Line: %d)", rawColumns, line);
  }

  @Nullable
  private Map<String, Object> removeEmptyValues(Map<String, Object> data)
  {
    return data == null
           ? null : data.entrySet()
                        .stream()
                        .filter(x -> x.getValue() != null)
                        .filter(x -> !(x.getValue() instanceof String) || !((String) x.getValue()).isEmpty())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static void assertEqualsSamplerResponseRow(SamplerResponseRow row1, SamplerResponseRow row2)
  {
    Assert.assertTrue(equalsIgnoringType(row1.getInput(), row2.getInput()));
    Assert.assertEquals(row1.getParsed(), row2.getParsed());
    Assert.assertEquals(row1.getError(), row2.getError());
    Assert.assertEquals(row1.isUnparseable(), row2.isUnparseable());
  }

  private static boolean equalsIgnoringType(Map<String, Object> map1, Map<String, Object> map2)
  {
    for (Entry<String, Object> entry1 : map1.entrySet()) {
      final Object val1 = entry1.getValue();
      final Object val2 = map2.get(entry1.getKey());
      if (!equalsStringOrInteger(val1, val2)) {
        return false;
      }
    }
    return true;
  }

  private static boolean equalsStringOrInteger(Object val1, Object val2)
  {
    if (val1 == null || val2 == null) {
      return val1 == val2;
    } else if (val1.equals(val2)) {
      return true;
    } else {
      if (val1 instanceof Number || val2 instanceof Number) {
        final Integer int1, int2;
        if (val1 instanceof String) {
          int1 = Integer.parseInt((String) val1);
        } else if (val1 instanceof Number) {
          int1 = ((Number) val1).intValue();
        } else {
          int1 = null;
        }

        if (val2 instanceof String) {
          int2 = Integer.parseInt((String) val2);
        } else if (val2 instanceof Number) {
          int2 = ((Number) val2).intValue();
        } else {
          int2 = null;
        }

        return Objects.equals(int1, int2);
      }
    }

    return false;
  }

  private static class TestRecordSupplier implements RecordSupplier<Integer, Long, ByteEntity>
  {
    private final List<String> jsonList;
    private final Set<Integer> partitions;
    private boolean polled;

    public TestRecordSupplier(List<String> jsonList)
    {
      this.jsonList = jsonList;
      partitions = ImmutableSet.of(5);
      polled = false;
    }

    @Override
    public void assign(Set<StreamPartition<Integer>> set)
    {
    }

    @Override
    public void seek(StreamPartition<Integer> partition, Long sequenceNumber)
    {
    }

    @Override
    public void seekToEarliest(Set<StreamPartition<Integer>> set)
    {
    }

    @Override
    public void seekToLatest(Set<StreamPartition<Integer>> set)
    {
    }

    @Override
    public Collection<StreamPartition<Integer>> getAssignment()
    {
      return null;
    }

    @Nonnull
    @Override
    public List<OrderedPartitionableRecord<Integer, Long, ByteEntity>> poll(long timeout)
    {
      if (polled) {
        try {
          Thread.sleep(timeout);
        }
        catch (InterruptedException e) {
        }
        return Collections.emptyList();
      }

      polled = true;
      return jsonList.stream()
                     .map(jsonText -> new OrderedPartitionableRecord<>(
                         "topic",
                         0,
                         0L,
                         Collections.singletonList(new ByteEntity(StringUtils.toUtf8(jsonText)))
                     ))
                     .collect(Collectors.toList());
    }

    @Nullable
    @Override
    public Long getLatestSequenceNumber(StreamPartition<Integer> partition)
    {
      return null;
    }

    @Nullable
    @Override
    public Long getEarliestSequenceNumber(StreamPartition<Integer> partition)
    {
      return null;
    }

    @Override
    public boolean isOffsetAvailable(StreamPartition<Integer> partition, OrderedSequenceNumber<Long> offset)
    {
      return true;
    }

    @Override
    public Long getPosition(StreamPartition<Integer> partition)
    {
      return null;
    }

    @Override
    public Set<Integer> getPartitionIds(String stream)
    {
      return partitions;
    }

    @Override
    public void close()
    {
    }
  }
}
