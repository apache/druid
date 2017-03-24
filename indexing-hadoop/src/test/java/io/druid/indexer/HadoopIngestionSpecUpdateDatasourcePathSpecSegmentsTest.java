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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.indexer.hadoop.DatasourceIngestionSpec;
import io.druid.indexer.hadoop.WindowedDataSegment;
import io.druid.indexer.path.DatasourcePathSpec;
import io.druid.indexer.path.MultiplePathSpec;
import io.druid.indexer.path.PathSpec;
import io.druid.indexer.path.StaticPathSpec;
import io.druid.indexer.path.UsedSegmentLister;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 */
public class HadoopIngestionSpecUpdateDatasourcePathSpecSegmentsTest
{
  private final String testDatasource = "test";
  private final Interval testDatasourceInterval = new Interval("1970/3000");
  private final Interval testDatasourceIntervalPartial = new Interval("2050/3000");
  private final ObjectMapper jsonMapper;

  public HadoopIngestionSpecUpdateDatasourcePathSpecSegmentsTest()
  {
    jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(
        new InjectableValues.Std().addValue(ObjectMapper.class, jsonMapper)
    );
  }

  private static final DataSegment SEGMENT = new DataSegment(
      "test1",
      Interval.parse("2000/3000"),
      "ver",
      ImmutableMap.<String, Object>of(
          "type", "local",
          "path", "/tmp/index1.zip"
      ),
      ImmutableList.of("host"),
      ImmutableList.of("visited_sum", "unique_hosts"),
      NoneShardSpec.instance(),
      9,
      2
  );

  @Test
  public void testupdateSegmentListIfDatasourcePathSpecIsUsedWithNoDatasourcePathSpec() throws Exception
  {
    PathSpec pathSpec = new StaticPathSpec("/xyz", null);
    HadoopDruidIndexerConfig config = testRunUpdateSegmentListIfDatasourcePathSpecIsUsed(pathSpec, null);
    Assert.assertTrue(config.getPathSpec() instanceof StaticPathSpec);
  }

  @Test
  public void testupdateSegmentListIfDatasourcePathSpecIsUsedWithJustDatasourcePathSpec() throws Exception
  {
    PathSpec pathSpec = new DatasourcePathSpec(
        jsonMapper,
        null,
        new DatasourceIngestionSpec(testDatasource, testDatasourceInterval, null, null, null, null, null, false),
        null
    );
    HadoopDruidIndexerConfig config = testRunUpdateSegmentListIfDatasourcePathSpecIsUsed(
        pathSpec,
        testDatasourceInterval
    );
    Assert.assertEquals(
        ImmutableList.of(WindowedDataSegment.of(SEGMENT)),
        ((DatasourcePathSpec) config.getPathSpec()).getSegments()
    );
  }

  @Test
  public void testupdateSegmentListIfDatasourcePathSpecWithMatchingUserSegments() throws Exception
  {
    PathSpec pathSpec = new DatasourcePathSpec(
        jsonMapper,
        null,
        new DatasourceIngestionSpec(
            testDatasource,
            testDatasourceInterval,
            null,
            ImmutableList.<DataSegment>of(SEGMENT),
            null,
            null,
            null,
            false
        ),
        null
    );
    HadoopDruidIndexerConfig config = testRunUpdateSegmentListIfDatasourcePathSpecIsUsed(
        pathSpec,
        testDatasourceInterval
    );
    Assert.assertEquals(
        ImmutableList.of(WindowedDataSegment.of(SEGMENT)),
        ((DatasourcePathSpec) config.getPathSpec()).getSegments()
    );
  }

  @Test(expected = IOException.class)
  public void testupdateSegmentListThrowsExceptionWithUserSegmentsMismatch() throws Exception
  {
    PathSpec pathSpec = new DatasourcePathSpec(
        jsonMapper,
        null,
        new DatasourceIngestionSpec(
            testDatasource,
            testDatasourceInterval,
            null,
            ImmutableList.<DataSegment>of(SEGMENT.withVersion("v2")),
            null,
            null,
            null,
            false
        ),
        null
    );
    testRunUpdateSegmentListIfDatasourcePathSpecIsUsed(
        pathSpec,
        testDatasourceInterval
    );
  }

  @Test
  public void testupdateSegmentListIfDatasourcePathSpecIsUsedWithJustDatasourcePathSpecAndPartialInterval()
      throws Exception
  {
    PathSpec pathSpec = new DatasourcePathSpec(
        jsonMapper,
        null,
        new DatasourceIngestionSpec(
            testDatasource,
            testDatasourceIntervalPartial,
            null,
            null,
            null,
            null,
            null,
            false
        ),
        null
    );
    HadoopDruidIndexerConfig config = testRunUpdateSegmentListIfDatasourcePathSpecIsUsed(
        pathSpec,
        testDatasourceIntervalPartial
    );
    Assert.assertEquals(
        ImmutableList.of(new WindowedDataSegment(SEGMENT, testDatasourceIntervalPartial)),
        ((DatasourcePathSpec) config.getPathSpec()).getSegments()
    );
  }

  @Test
  public void testupdateSegmentListIfDatasourcePathSpecIsUsedWithMultiplePathSpec() throws Exception
  {
    PathSpec pathSpec = new MultiplePathSpec(
        ImmutableList.of(
            new StaticPathSpec("/xyz", null),
            new DatasourcePathSpec(
                jsonMapper,
                null,
                new DatasourceIngestionSpec(
                    testDatasource,
                    testDatasourceInterval,
                    null,
                    null,
                    null,
                    null,
                    null,
                    false
                ),
                null
            )
        )
    );
    HadoopDruidIndexerConfig config = testRunUpdateSegmentListIfDatasourcePathSpecIsUsed(
        pathSpec,
        testDatasourceInterval
    );
    Assert.assertEquals(
        ImmutableList.of(WindowedDataSegment.of(SEGMENT)),
        ((DatasourcePathSpec) ((MultiplePathSpec) config.getPathSpec()).getChildren().get(1)).getSegments()
    );
  }

  private HadoopDruidIndexerConfig testRunUpdateSegmentListIfDatasourcePathSpecIsUsed(
      PathSpec datasourcePathSpec,
      Interval jobInterval
  )
      throws Exception
  {
    HadoopIngestionSpec spec = new HadoopIngestionSpec(
        new DataSchema(
            "foo",
            null,
            new AggregatorFactory[0],
            new UniformGranularitySpec(
                Granularities.DAY,
                null,
                ImmutableList.of(
                    new Interval("2010-01-01/P1D")
                )
            ),
            jsonMapper
        ),
        new HadoopIOConfig(
            jsonMapper.convertValue(datasourcePathSpec, Map.class),
            null,
            null
        ),
        null
    );

    spec = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        HadoopIngestionSpec.class
    );

    UsedSegmentLister segmentLister = EasyMock.createMock(UsedSegmentLister.class);
    EasyMock.expect(
        segmentLister.getUsedSegmentsForIntervals(testDatasource, Lists.newArrayList(jobInterval))
    ).andReturn(ImmutableList.of(SEGMENT));
    EasyMock.replay(segmentLister);

    spec = HadoopIngestionSpec.updateSegmentListIfDatasourcePathSpecIsUsed(spec, jsonMapper, segmentLister);
    return HadoopDruidIndexerConfig.fromString(jsonMapper.writeValueAsString(spec));
  }
}
