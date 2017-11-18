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

package io.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexer.path.StaticPathSpec;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.jackson.JacksonUtils;
import io.druid.java.util.common.parsers.JSONPathSpec;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.TestHelper;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.transform.ExpressionTransform;
import io.druid.segment.transform.TransformSpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HadoopDruidIndexerMapperTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.getJsonMapper();
  private static final DataSchema DATA_SCHEMA = new DataSchema(
      "test_ds",
      JSON_MAPPER.convertValue(
          new HadoopyStringInputRowParser(
              new JSONParseSpec(
                  new TimestampSpec("t", "auto", null),
                  new DimensionsSpec(
                      DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim1t", "dim2")),
                      null,
                      null
                  ),
                  new JSONPathSpec(true, ImmutableList.of()),
                  ImmutableMap.of()
              )
          ),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      ),
      new AggregatorFactory[]{new CountAggregatorFactory("rows")},
      new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
      null,
      JSON_MAPPER
  );

  private static final HadoopIOConfig IO_CONFIG = new HadoopIOConfig(
      JSON_MAPPER.convertValue(
          new StaticPathSpec("dummyPath", null),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      ),
      null,
      "dummyOutputPath"
  );

  private static final HadoopTuningConfig TUNING_CONFIG = HadoopTuningConfig
      .makeDefaultTuningConfig()
      .withWorkingPath("dummyWorkingPath");

  @Test
  public void testHadoopyStringParser() throws Exception
  {
    final HadoopDruidIndexerConfig config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(DATA_SCHEMA, IO_CONFIG, TUNING_CONFIG)
    );

    final MyMapper mapper = new MyMapper();
    final Configuration hadoopConfig = new Configuration();
    hadoopConfig.set(
        HadoopDruidIndexerConfig.CONFIG_PROPERTY,
        HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(config)
    );
    final Mapper.Context mapContext = EasyMock.mock(Mapper.Context.class);
    EasyMock.expect(mapContext.getConfiguration()).andReturn(hadoopConfig).once();
    EasyMock.replay(mapContext);
    mapper.setup(mapContext);
    final List<Map<String, Object>> rows = ImmutableList.of(
        ImmutableMap.of("t", "2000-01-01T00:00:00.000Z", "dim1", "x", "m1", 1.0),
        ImmutableMap.of("t", "2000-01-01T00:00:00.000Z", "dim2", "y", "m1", 1.0)
    );
    for (Map<String, Object> row : rows) {
      mapper.map(NullWritable.get(), new Text(JSON_MAPPER.writeValueAsString(row)), mapContext);
    }
    assertRowListEquals(rows, mapper.getRows());
  }

  @Test
  public void testHadoopyStringParserWithTransformSpec() throws Exception
  {
    final HadoopDruidIndexerConfig config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            DATA_SCHEMA.withTransformSpec(
                new TransformSpec(
                    new SelectorDimFilter("dim1", "foo", null),
                    ImmutableList.of(
                        new ExpressionTransform("dim1t", "concat(dim1,dim1)", ExprMacroTable.nil())
                    )
                )
            ),
            IO_CONFIG,
            TUNING_CONFIG
        )
    );

    final MyMapper mapper = new MyMapper();
    final Configuration hadoopConfig = new Configuration();
    hadoopConfig.set(
        HadoopDruidIndexerConfig.CONFIG_PROPERTY,
        HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(config)
    );
    final Mapper.Context mapContext = EasyMock.mock(Mapper.Context.class);
    EasyMock.expect(mapContext.getConfiguration()).andReturn(hadoopConfig).once();
    EasyMock.replay(mapContext);
    mapper.setup(mapContext);
    final List<Map<String, Object>> rows = ImmutableList.of(
        ImmutableMap.of("t", "2000-01-01T00:00:00.000Z", "dim1", "foo", "dim2", "x", "m1", 1.0),
        ImmutableMap.of("t", "2000-01-01T00:00:00.000Z", "dim1", "bar", "dim2", "y", "m1", 1.0),
        ImmutableMap.of("t", "2000-01-01T00:00:00.000Z", "dim1", "foo", "dim2", "z", "m1", 1.0)
    );
    for (Map<String, Object> row : rows) {
      mapper.map(NullWritable.get(), new Text(JSON_MAPPER.writeValueAsString(row)), mapContext);
    }
    assertRowListEquals(
        ImmutableList.of(
            ImmutableMap.of("t", "2000-01-01T00:00:00.000Z", "dim1", "foo", "dim1t", "foofoo", "dim2", "x", "m1", 1.0),
            ImmutableMap.of("t", "2000-01-01T00:00:00.000Z", "dim1", "foo", "dim1t", "foofoo", "dim2", "z", "m1", 1.0)
        ),
        mapper.getRows()
    );
  }

  private static void assertRowListEquals(final List<Map<String, Object>> expected, final List<InputRow> actual)
  {
    Assert.assertEquals(
        expected,
        actual.stream().map(HadoopDruidIndexerMapperTest::rowToMap).collect(Collectors.toList())
    );
  }

  private static Map<String, Object> rowToMap(final InputRow row)
  {
    // Normalize input row for the purposes of testing.
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
        .put("t", row.getTimestamp().toString());

    for (String dim : row.getDimensions()) {
      final Object val = row.getRaw(dim);
      if (val != null) {
        builder.put(dim, val);
      }
    }

    // other, non-dimension fields are not self describing so much be specified individually
    builder.put("m1", row.getRaw("m1"));
    return builder.build();
  }

  public static class MyMapper extends HadoopDruidIndexerMapper
  {
    private final List<InputRow> rows = new ArrayList<>();

    @Override
    protected void innerMap(
        final InputRow inputRow,
        final Context context,
        final boolean reportParseExceptions
    ) throws IOException, InterruptedException
    {
      rows.add(inputRow);
    }

    public List<InputRow> getRows()
    {
      return rows;
    }
  }
}
