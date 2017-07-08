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

import com.fasterxml.jackson.core.type.TypeReference;
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
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ServerConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 */
public class DatasourcePathSpecTest
{
  private DatasourceIngestionSpec ingestionSpec;
  private List<WindowedDataSegment> segments;

  public DatasourcePathSpecTest()
  {
    this.ingestionSpec = new DatasourceIngestionSpec(
        "test",
        Interval.parse("2000/3000"),
        null,
        null,
        null,
        null,
        null,
        false
    );

    segments = ImmutableList.of(
        WindowedDataSegment.of(
            new DataSegment(
                ingestionSpec.getDataSource(),
                Interval.parse("2000/3000"),
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
                ingestionSpec.getDataSource(),
                Interval.parse("2050/3000"),
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
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("dummy-node", null, null, null, new ServerConfig())
                );
              }
            }
        )
    );

    ObjectMapper jsonMapper = injector.getInstance(ObjectMapper.class);

    DatasourcePathSpec expected = new DatasourcePathSpec(
        jsonMapper,
        null,
        ingestionSpec,
        Long.valueOf(10)
    );
    PathSpec actual = jsonMapper.readValue(jsonMapper.writeValueAsString(expected), PathSpec.class);
    Assert.assertEquals(expected, actual);

    expected = new DatasourcePathSpec(
        jsonMapper,
        null,
        ingestionSpec,
        null
    );
    actual = jsonMapper.readValue(jsonMapper.writeValueAsString(expected), PathSpec.class);
    Assert.assertEquals(expected, actual);

    expected = new DatasourcePathSpec(
        jsonMapper,
        segments,
        ingestionSpec,
        null
    );
    actual = jsonMapper.readValue(jsonMapper.writeValueAsString(expected), PathSpec.class);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testAddInputPaths() throws Exception
  {
    HadoopDruidIndexerConfig hadoopIndexerConfig = makeHadoopDruidIndexerConfig();

    ObjectMapper mapper = new DefaultObjectMapper();

    DatasourcePathSpec pathSpec = new DatasourcePathSpec(
        mapper,
        segments,
        ingestionSpec,
        null
    );

    Configuration config = new Configuration();
    Job job = EasyMock.createNiceMock(Job.class);
    EasyMock.expect(job.getConfiguration()).andReturn(config).anyTimes();
    EasyMock.replay(job);

    pathSpec.addInputPaths(hadoopIndexerConfig, job);
    List<WindowedDataSegment> actualSegments = mapper.readValue(
        config.get(DatasourceInputFormat.CONF_INPUT_SEGMENTS),
        new TypeReference<List<WindowedDataSegment>>()
        {
        }
    );

    Assert.assertEquals(segments, actualSegments);

    DatasourceIngestionSpec actualIngestionSpec = mapper.readValue(
        config.get(DatasourceInputFormat.CONF_DRUID_SCHEMA),
        DatasourceIngestionSpec.class
    );
    Assert.assertEquals(
        ingestionSpec
            .withDimensions(ImmutableList.of("product"))
            .withMetrics(ImmutableList.of("visited_sum")),
        actualIngestionSpec
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
        ingestionSpec,
        null
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
        ingestionSpec.withIgnoreWhenNoSegments(true),
        null
    );
    pathSpec.addInputPaths(hadoopIndexerConfig, job);

    Assert.assertNull(config.get(DatasourceInputFormat.CONF_INPUT_SEGMENTS));
    Assert.assertNull(config.get(DatasourceInputFormat.CONF_DRUID_SCHEMA));
  }

  private HadoopDruidIndexerConfig makeHadoopDruidIndexerConfig()
  {
    return new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            new DataSchema(
                ingestionSpec.getDataSource(),
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
                    Granularities.DAY, Granularities.NONE, ImmutableList.of(Interval.parse("2000/3000"))
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
  }
}
