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

package io.druid.indexer.path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.HadoopIOConfig;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.HadoopTuningConfig;
import io.druid.indexer.hadoop.DatasourceIngestionSpec;
import io.druid.indexer.hadoop.DatasourceInputFormat;
import io.druid.indexer.hadoop.WindowedDataSegment;
import io.druid.initialization.Initialization;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.TestHelper;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.server.DruidNode;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
public class DatasourcePathSpecTest
{
  private DatasourceIngestionSpec ingestionSpec1;
  private DatasourceIngestionSpec ingestionSpec2;
  private List<WindowedDataSegment> segments1;
  private List<WindowedDataSegment> segments2;

  public DatasourcePathSpecTest()
  {
    this.ingestionSpec1 = new DatasourceIngestionSpec(
        "test",
        Intervals.of("2000/3000"),
        null,
        null,
        null,
        null,
        null,
        false,
        null
    );

    this.ingestionSpec2 = new DatasourceIngestionSpec(
        "test2",
        Intervals.of("2000/3000"),
        null,
        null,
        null,
        null,
        null,
        false,
        null
    );

    segments1 = ImmutableList.of(
        WindowedDataSegment.of(
            new DataSegment(
                ingestionSpec1.getDataSource(),
                Intervals.of("2000/3000"),
                "ver",
                ImmutableMap.<String, Object>of(
                    "type", "local",
                    "path", "/tmp/index.zip"
                ),
                ImmutableList.of("product"),
                ImmutableList.of("visited_sum", "unique_hosts"),
                NoneShardSpec.instance(),
                9,
                12334
            )
        ),
        WindowedDataSegment.of(
            new DataSegment(
                ingestionSpec1.getDataSource(),
                Intervals.of("2050/3000"),
                "ver",
                ImmutableMap.<String, Object>of(
                    "type", "hdfs",
                    "path", "/tmp/index.zip"
                ),
                ImmutableList.of("product"),
                ImmutableList.of("visited_sum", "unique_hosts"),
                NoneShardSpec.instance(),
                9,
                12335
            )
        )
    );

    segments2 = ImmutableList.of(
        WindowedDataSegment.of(
            new DataSegment(
                ingestionSpec2.getDataSource(),
                Intervals.of("2000/3000"),
                "ver",
                ImmutableMap.<String, Object>of(
                    "type", "local",
                    "path", "/tmp2/index.zip"
                ),
                ImmutableList.of("product2"),
                ImmutableList.of("visited_sum2", "unique_hosts2"),
                NoneShardSpec.instance(),
                9,
                12334
            )
        )
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    final UsedSegmentLister segmentList = EasyMock.createMock(
        UsedSegmentLister.class
    );

    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bind(UsedSegmentLister.class).toInstance(segmentList);
                JsonConfigProvider.bindInstance(
                    binder,
                    Key.get(DruidNode.class, Self.class),
                    new DruidNode("dummy-node", null, null, null, true, false)
                );
              }
            }
        )
    );

    ObjectMapper jsonMapper = injector.getInstance(ObjectMapper.class);

    DatasourcePathSpec expected = new DatasourcePathSpec(
        jsonMapper,
        null,
        ingestionSpec1,
        Long.valueOf(10),
        false
    );
    PathSpec actual = jsonMapper.readValue(jsonMapper.writeValueAsString(expected), PathSpec.class);
    Assert.assertEquals(expected, actual);

    expected = new DatasourcePathSpec(
        jsonMapper,
        null,
        ingestionSpec1,
        null,
        false
    );
    actual = jsonMapper.readValue(jsonMapper.writeValueAsString(expected), PathSpec.class);
    Assert.assertEquals(expected, actual);

    expected = new DatasourcePathSpec(
        jsonMapper,
        segments1,
        ingestionSpec1,
        null,
        false
    );
    actual = jsonMapper.readValue(jsonMapper.writeValueAsString(expected), PathSpec.class);
    Assert.assertEquals(expected, actual);

    expected = new DatasourcePathSpec(
        jsonMapper,
        segments1,
        ingestionSpec1,
        null,
        true
    );
    actual = jsonMapper.readValue(jsonMapper.writeValueAsString(expected), PathSpec.class);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testAddInputPaths() throws Exception
  {
    HadoopDruidIndexerConfig hadoopIndexerConfig = makeHadoopDruidIndexerConfig();

    ObjectMapper mapper = TestHelper.makeJsonMapper();

    DatasourcePathSpec pathSpec1 = new DatasourcePathSpec(
        mapper,
        segments1,
        ingestionSpec1,
        null,
        false
    );

    DatasourcePathSpec pathSpec2 = new DatasourcePathSpec(
        mapper,
        segments2,
        ingestionSpec2,
        null,
        false
    );

    Configuration config = new Configuration();
    Job job = EasyMock.createNiceMock(Job.class);
    EasyMock.expect(job.getConfiguration()).andReturn(config).anyTimes();
    EasyMock.replay(job);

    pathSpec1.addInputPaths(hadoopIndexerConfig, job);
    pathSpec2.addInputPaths(hadoopIndexerConfig, job);

    Assert.assertEquals(
        ImmutableList.of(ingestionSpec1.getDataSource(), ingestionSpec2.getDataSource()),
        DatasourceInputFormat.getDataSources(config)
    );

    Assert.assertEquals(segments1, DatasourceInputFormat.getSegments(config, ingestionSpec1.getDataSource()));
    Assert.assertEquals(segments2, DatasourceInputFormat.getSegments(config, ingestionSpec2.getDataSource()));

    Assert.assertEquals(
        ingestionSpec1
            .withDimensions(ImmutableList.of("product"))
            .withMetrics(ImmutableList.of("visited_sum")),
        DatasourceInputFormat.getIngestionSpec(config, ingestionSpec1.getDataSource())
    );

    Assert.assertEquals(
        ingestionSpec2
            .withDimensions(ImmutableList.of("product2"))
            .withMetrics(ImmutableList.of("visited_sum")),
        DatasourceInputFormat.getIngestionSpec(config, ingestionSpec2.getDataSource())
    );
  }

  @Test
  public void testAddInputPathsWithNoSegments() throws Exception
  {
    HadoopDruidIndexerConfig hadoopIndexerConfig = makeHadoopDruidIndexerConfig();

    ObjectMapper mapper = new DefaultObjectMapper();

    DatasourcePathSpec pathSpec = new DatasourcePathSpec(
        mapper,
        null,
        ingestionSpec1,
        null,
        false
    );

    Configuration config = new Configuration();
    Job job = EasyMock.createNiceMock(Job.class);
    EasyMock.expect(job.getConfiguration()).andReturn(config).anyTimes();
    EasyMock.replay(job);

    try {
      pathSpec.addInputPaths(hadoopIndexerConfig, job);
      Assert.fail("should've been ISE");
    }
    catch (ISE ex) {
      //OK
    }

    //now with ignoreWhenNoSegments flag set
    pathSpec = new DatasourcePathSpec(
        mapper,
        null,
        ingestionSpec1.withIgnoreWhenNoSegments(true),
        null,
        false
    );
    pathSpec.addInputPaths(hadoopIndexerConfig, job);

    Assert.assertEquals(Collections.emptyList(), DatasourceInputFormat.getDataSources(config));
  }

  @SuppressWarnings("unchecked")
  private HadoopDruidIndexerConfig makeHadoopDruidIndexerConfig()
  {
    return new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            new DataSchema(
                ingestionSpec1.getDataSource(),
                HadoopDruidIndexerConfig.JSON_MAPPER.convertValue(
                    new StringInputRowParser(
                        new CSVParseSpec(
                            new TimestampSpec("timestamp", "yyyyMMddHH", null),
                            new DimensionsSpec(null, null, null),
                            null,
                            ImmutableList.of("timestamp", "host", "visited"),
                            false,
                            0
                        ),
                        null
                    ),
                    Map.class
                ),
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("visited_sum", "visited")
                },
                new UniformGranularitySpec(
                    Granularities.DAY, Granularities.NONE, ImmutableList.of(Intervals.of("2000/3000"))
                ),
                null,
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
  }
}
