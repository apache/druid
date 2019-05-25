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

package org.apache.druid.indexer.path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.HadoopIOConfig;
import org.apache.druid.indexer.HadoopIngestionSpec;
import org.apache.druid.indexer.HadoopTuningConfig;
import org.apache.druid.indexer.hadoop.DatasourceIngestionSpec;
import org.apache.druid.indexer.hadoop.DatasourceInputFormat;
import org.apache.druid.indexer.hadoop.WindowedDataSegment;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.server.DruidNode;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
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
                ImmutableMap.of(
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
                ImmutableMap.of(
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
                ImmutableMap.of(
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
                    new DruidNode("dummy-node", null, false, null, null, true, false)
                );
              }
            }
        )
    );

    ObjectMapper jsonMapper = injector.getInstance(ObjectMapper.class);

    DatasourcePathSpec expected = new DatasourcePathSpec(
        null,
        ingestionSpec1,
        Long.valueOf(10),
        false
    );
    PathSpec actual = jsonMapper.readValue(jsonMapper.writeValueAsString(expected), PathSpec.class);
    Assert.assertEquals(expected, actual);

    expected = new DatasourcePathSpec(
        null,
        ingestionSpec1,
        null,
        false
    );
    actual = jsonMapper.readValue(jsonMapper.writeValueAsString(expected), PathSpec.class);
    Assert.assertEquals(expected, actual);

    expected = new DatasourcePathSpec(
        segments1,
        ingestionSpec1,
        null,
        false
    );
    actual = jsonMapper.readValue(jsonMapper.writeValueAsString(expected), PathSpec.class);
    Assert.assertEquals(expected, actual);

    expected = new DatasourcePathSpec(
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

    DatasourcePathSpec pathSpec1 = new DatasourcePathSpec(
        segments1,
        ingestionSpec1,
        null,
        false
    );

    DatasourcePathSpec pathSpec2 = new DatasourcePathSpec(
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

    DatasourcePathSpec pathSpec = new DatasourcePathSpec(
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
                    Granularities.DAY,
                    Granularities.NONE,
                    ImmutableList.of(Intervals.of("2000/3000"))
                ),
                null,
                HadoopDruidIndexerConfig.JSON_MAPPER
            ),
            new HadoopIOConfig(
                ImmutableMap.of(
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
