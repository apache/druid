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
import com.metamx.common.Granularity;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
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
                        new CSVParseSpec(
                            new TimestampSpec("timestamp", "yyyyMMddHH", null),
                            new DimensionsSpec(ImmutableList.of("host"), null, null),
                            null,
                            ImmutableList.of("timestamp", "host", "visited")
                        )
                    ),
                    Map.class
                ),
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("visited_sum", "visited"),
                    new HyperUniquesAggregatorFactory("unique_hosts", "host")
                },
                new UniformGranularitySpec(
                    Granularity.DAY, QueryGranularity.NONE, ImmutableList.of(Interval.parse("2010/2011"))
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
        ImmutableList.<String>of(),
        ImmutableMap.<String, Object>of(
            "host", "host1",
            "visited", 10
        )
    );
    InputRow row2 = new MapBasedInputRow(
        timestamp,
        ImmutableList.<String>of(),
        ImmutableMap.<String, Object>of(
            "host", "host2",
            "visited", 5
        )
    );
    List<BytesWritable> rows = Lists.newArrayList(
        new BytesWritable(InputRowSerde.toBytes(row1, aggregators)),
        new BytesWritable(InputRowSerde.toBytes(row2, aggregators))
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

    Assert.assertTrue(captureKey.getValue() == key);

    InputRow capturedRow = InputRowSerde.fromBytes(captureVal.getValue().getBytes(), aggregators);
    Assert.assertEquals(15, capturedRow.getLongMetric("visited_sum"));
    Assert.assertEquals(2.0, (Double)HyperUniquesAggregatorFactory.estimateCardinality(capturedRow.getRaw("unique_hosts")), 0.001);
  }
}
