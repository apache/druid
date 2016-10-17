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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class IndexGeneratorCombinerTest
{
  private AggregatorFactory[] aggregators;
  private IndexGeneratorJob.IndexGeneratorCombiner combiner;

  @Before
  public void setUp() throws Exception
  {
    HadoopDruidIndexerConfig config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            new DataSchema(
                "website",
                HadoopDruidIndexerConfig.JSON_MAPPER.convertValue(
                    new StringInputRowParser(
                        new TimeAndDimsParseSpec(
                            new TimestampSpec("timestamp", "yyyyMMddHH", null),
                            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host", "keywords")), null, null)
                        ),
                        null
                    ),
                    Map.class
                ),
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("visited_sum", "visited"),
                    new HyperUniquesAggregatorFactory("unique_hosts", "host")
                },
                new UniformGranularitySpec(
                    Granularity.DAY, QueryGranularities.NONE, ImmutableList.of(Interval.parse("2010/2011"))
                ),
                HadoopDruidIndexerConfig.JSON_MAPPER
            ),
            new HadoopIOConfig(
                ImmutableMap.<String, Object>of(
                    "paths",
                    "/tmp/dummy",
                    "type",
                    "static"
                ),
                null,
                "/tmp/dummy"
            ),
            HadoopTuningConfig.makeDefaultTuningConfig().withWorkingPath("/tmp/work").withVersion("ver")
        )
    );
    Configuration hadoopConfig = new Configuration();
    hadoopConfig.set(
        HadoopDruidIndexerConfig.CONFIG_PROPERTY,
        HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(config)
    );

    Reducer.Context context = EasyMock.createMock(Reducer.Context.class);
    EasyMock.expect(context.getConfiguration()).andReturn(hadoopConfig);
    EasyMock.replay(context);

    aggregators = config.getSchema().getDataSchema().getAggregators();

    combiner = new IndexGeneratorJob.IndexGeneratorCombiner();
    combiner.setup(context);
  }

  @Test
  public void testSingleRowNoMergePassThrough() throws Exception
  {
    Reducer.Context context = EasyMock.createMock(Reducer.Context.class);
    Capture<BytesWritable> captureKey = Capture.newInstance();
    Capture<BytesWritable> captureVal = Capture.newInstance();
    context.write(EasyMock.capture(captureKey), EasyMock.capture(captureVal));
    EasyMock.replay(context);

    BytesWritable key = new BytesWritable("dummy_key".getBytes());
    BytesWritable val = new BytesWritable("dummy_row".getBytes());

    combiner.reduce(key, Lists.newArrayList(val), context);

    Assert.assertTrue(captureKey.getValue() == key);
    Assert.assertTrue(captureVal.getValue() == val);
  }

  @Test
  public void testMultipleRowsMerged() throws Exception
  {
    long timestamp = System.currentTimeMillis();

    Bucket bucket = new Bucket(0, new DateTime(timestamp), 0);
    SortableBytes keySortableBytes = new SortableBytes(
        bucket.toGroupKey(),
        new byte[0]
    );
    BytesWritable key = keySortableBytes.toBytesWritable();

    InputRow row1 = new MapBasedInputRow(
        timestamp,
        ImmutableList.<String>of("keywords"),
        ImmutableMap.<String, Object>of(
            "host", "host1",
            "keywords", Arrays.asList("foo", "bar"),
            "visited", 10
        )
    );
    InputRow row2 = new MapBasedInputRow(
        timestamp,
        ImmutableList.<String>of("keywords"),
        ImmutableMap.<String, Object>of(
            "host", "host2",
            "keywords", Arrays.asList("foo", "bar"),
            "visited", 5
        )
    );
    List<BytesWritable> rows = Lists.newArrayList(
        new BytesWritable(InputRowSerde.toBytes(row1, aggregators, true)),
        new BytesWritable(InputRowSerde.toBytes(row2, aggregators, true))
    );

    Reducer.Context context = EasyMock.createNiceMock(Reducer.Context.class);
    Capture<BytesWritable> captureKey = Capture.newInstance();
    Capture<BytesWritable> captureVal = Capture.newInstance();
    context.write(EasyMock.capture(captureKey), EasyMock.capture(captureVal));
    EasyMock.replay(context);

    combiner.reduce(
        key,
        rows,
        context
    );

    EasyMock.verify(context);

    Assert.assertTrue(captureKey.getValue() == key);

    InputRow capturedRow = InputRowSerde.fromBytes(captureVal.getValue().getBytes(), aggregators);
    Assert.assertEquals(Arrays.asList("host", "keywords"), capturedRow.getDimensions());
    Assert.assertEquals(ImmutableList.of(), capturedRow.getDimension("host"));
    Assert.assertEquals(Arrays.asList("bar", "foo"), capturedRow.getDimension("keywords"));
    Assert.assertEquals(15, capturedRow.getLongMetric("visited_sum"));
    Assert.assertEquals(2.0, (Double)HyperUniquesAggregatorFactory.estimateCardinality(capturedRow.getRaw("unique_hosts")), 0.001);
  }

  @Test
  public void testMultipleRowsNotMerged() throws Exception
  {
    long timestamp = System.currentTimeMillis();

    Bucket bucket = new Bucket(0, new DateTime(timestamp), 0);
    SortableBytes keySortableBytes = new SortableBytes(
        bucket.toGroupKey(),
        new byte[0]
    );
    BytesWritable key = keySortableBytes.toBytesWritable();

    InputRow row1 = new MapBasedInputRow(
        timestamp,
        ImmutableList.<String>of("host", "keywords"),
        ImmutableMap.<String, Object>of(
            "host", "host1",
            "keywords", Arrays.asList("foo", "bar"),
            "visited", 10
        )
    );
    InputRow row2 = new MapBasedInputRow(
        timestamp,
        ImmutableList.<String>of("host", "keywords"),
        ImmutableMap.<String, Object>of(
            "host", "host2",
            "keywords", Arrays.asList("foo", "bar"),
            "visited", 5
        )
    );
    List<BytesWritable> rows = Lists.newArrayList(
        new BytesWritable(InputRowSerde.toBytes(row1, aggregators, true)),
        new BytesWritable(InputRowSerde.toBytes(row2, aggregators, true))
    );

    Reducer.Context context = EasyMock.createNiceMock(Reducer.Context.class);
    Capture<BytesWritable> captureKey1 = Capture.newInstance();
    Capture<BytesWritable> captureVal1 = Capture.newInstance();
    Capture<BytesWritable> captureKey2 = Capture.newInstance();
    Capture<BytesWritable> captureVal2 = Capture.newInstance();
    context.write(EasyMock.capture(captureKey1), EasyMock.capture(captureVal1));
    context.write(EasyMock.capture(captureKey2), EasyMock.capture(captureVal2));
    EasyMock.replay(context);

    combiner.reduce(
        key,
        rows,
        context
    );

    EasyMock.verify(context);

    Assert.assertTrue(captureKey1.getValue() == key);
    Assert.assertTrue(captureKey2.getValue() == key);

    InputRow capturedRow1 = InputRowSerde.fromBytes(captureVal1.getValue().getBytes(), aggregators);
    Assert.assertEquals(Arrays.asList("host", "keywords"), capturedRow1.getDimensions());
    Assert.assertEquals(Arrays.asList("host1"), capturedRow1.getDimension("host"));
    Assert.assertEquals(Arrays.asList("bar", "foo"), capturedRow1.getDimension("keywords"));
    Assert.assertEquals(10, capturedRow1.getLongMetric("visited_sum"));
    Assert.assertEquals(1.0, (Double)HyperUniquesAggregatorFactory.estimateCardinality(capturedRow1.getRaw("unique_hosts")), 0.001);

    InputRow capturedRow2 = InputRowSerde.fromBytes(captureVal2.getValue().getBytes(), aggregators);
    Assert.assertEquals(Arrays.asList("host", "keywords"), capturedRow2.getDimensions());
    Assert.assertEquals(Arrays.asList("host2"), capturedRow2.getDimension("host"));
    Assert.assertEquals(Arrays.asList("bar", "foo"), capturedRow2.getDimension("keywords"));
    Assert.assertEquals(5, capturedRow2.getLongMetric("visited_sum"));
    Assert.assertEquals(1.0, (Double)HyperUniquesAggregatorFactory.estimateCardinality(capturedRow2.getRaw("unique_hosts")), 0.001);
  }
}
