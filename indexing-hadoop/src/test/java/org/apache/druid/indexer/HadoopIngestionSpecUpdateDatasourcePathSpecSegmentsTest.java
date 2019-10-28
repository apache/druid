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

package org.apache.druid.indexer;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.hadoop.DatasourceIngestionSpec;
import org.apache.druid.indexer.hadoop.WindowedDataSegment;
import org.apache.druid.indexer.path.DatasourcePathSpec;
import org.apache.druid.indexer.path.MultiplePathSpec;
import org.apache.druid.indexer.path.PathSpec;
import org.apache.druid.indexer.path.StaticPathSpec;
import org.apache.druid.indexer.path.UsedSegmentLister;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 */
public class HadoopIngestionSpecUpdateDatasourcePathSpecSegmentsTest
{
  private static final String TEST_DATA_SOURCE = "test";
  private static final String TEST_DATA_SOURCE2 = "test2";
  private static final Interval TEST_DATA_SOURCE_INTERVAL = Intervals.of("1970/3000");
  private static final Interval TEST_DATA_SOURCE_INTERVAL2 = Intervals.of("2000/2001");
  private static final Interval TEST_DATA_SOURCE_INTERVAL_PARTIAL = Intervals.of("2050/3000");

  private final ObjectMapper jsonMapper;

  public HadoopIngestionSpecUpdateDatasourcePathSpecSegmentsTest()
  {
    jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ObjectMapper.class, jsonMapper)
            .addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT)
    );
  }

  private static final DataSegment SEGMENT = new DataSegment(
      TEST_DATA_SOURCE,
      Intervals.of("2000/3000"),
      "ver",
      ImmutableMap.of(
          "type", "local",
          "path", "/tmp/index1.zip"
      ),
      ImmutableList.of("host"),
      ImmutableList.of("visited_sum", "unique_hosts"),
      NoneShardSpec.instance(),
      9,
      2
  );

  private static final DataSegment SEGMENT2 = new DataSegment(
      TEST_DATA_SOURCE2,
      Intervals.of("2000/3000"),
      "ver2",
      ImmutableMap.of(
          "type", "local",
          "path", "/tmp/index2.zip"
      ),
      ImmutableList.of("host2"),
      ImmutableList.of("visited_sum", "unique_hosts"),
      NoneShardSpec.instance(),
      9,
      2
  );

  @Test
  public void testUpdateSegmentListIfDatasourcePathSpecIsUsedWithNoDatasourcePathSpec() throws Exception
  {
    PathSpec pathSpec = new StaticPathSpec("/xyz", null);
    HadoopDruidIndexerConfig config = testRunUpdateSegmentListIfDatasourcePathSpecIsUsed(pathSpec, null);
    Assert.assertTrue(config.getPathSpec() instanceof StaticPathSpec);
  }

  @Test
  public void testUpdateSegmentListIfDatasourcePathSpecIsUsedWithJustDatasourcePathSpec() throws Exception
  {
    PathSpec pathSpec = new DatasourcePathSpec(
        null,
        new DatasourceIngestionSpec(TEST_DATA_SOURCE, TEST_DATA_SOURCE_INTERVAL, null, null, null, null, null, false, null),
        null,
        false
    );
    HadoopDruidIndexerConfig config = testRunUpdateSegmentListIfDatasourcePathSpecIsUsed(
        pathSpec,
        TEST_DATA_SOURCE_INTERVAL
    );
    Assert.assertEquals(
        ImmutableList.of(WindowedDataSegment.of(SEGMENT)),
        ((DatasourcePathSpec) config.getPathSpec()).getSegments()
    );
  }

  @Test
  public void testUpdateSegmentListIfDatasourcePathSpecWithMatchingUserSegments() throws Exception
  {
    PathSpec pathSpec = new DatasourcePathSpec(
        null,
        new DatasourceIngestionSpec(
            TEST_DATA_SOURCE,
            TEST_DATA_SOURCE_INTERVAL,
            null,
            ImmutableList.of(SEGMENT),
            null,
            null,
            null,
            false,
            null
        ),
        null,
        false
    );
    HadoopDruidIndexerConfig config = testRunUpdateSegmentListIfDatasourcePathSpecIsUsed(
        pathSpec,
        TEST_DATA_SOURCE_INTERVAL
    );
    Assert.assertEquals(
        ImmutableList.of(WindowedDataSegment.of(SEGMENT)),
        ((DatasourcePathSpec) config.getPathSpec()).getSegments()
    );
  }

  @Test(expected = IOException.class)
  public void testUpdateSegmentListThrowsExceptionWithUserSegmentsMismatch() throws Exception
  {
    PathSpec pathSpec = new DatasourcePathSpec(
        null,
        new DatasourceIngestionSpec(
            TEST_DATA_SOURCE,
            TEST_DATA_SOURCE_INTERVAL,
            null,
            ImmutableList.of(SEGMENT.withVersion("v2")),
            null,
            null,
            null,
            false,
            null
        ),
        null,
        false
    );
    testRunUpdateSegmentListIfDatasourcePathSpecIsUsed(
        pathSpec,
        TEST_DATA_SOURCE_INTERVAL
    );
  }

  @Test
  public void testUpdateSegmentListIfDatasourcePathSpecIsUsedWithJustDatasourcePathSpecAndPartialInterval()
      throws Exception
  {
    PathSpec pathSpec = new DatasourcePathSpec(
        null,
        new DatasourceIngestionSpec(
            TEST_DATA_SOURCE,
            TEST_DATA_SOURCE_INTERVAL_PARTIAL,
            null,
            null,
            null,
            null,
            null,
            false,
            null
        ),
        null,
        false
    );
    HadoopDruidIndexerConfig config = testRunUpdateSegmentListIfDatasourcePathSpecIsUsed(
        pathSpec,
        TEST_DATA_SOURCE_INTERVAL_PARTIAL
    );
    Assert.assertEquals(
        ImmutableList.of(new WindowedDataSegment(SEGMENT, TEST_DATA_SOURCE_INTERVAL_PARTIAL)),
        ((DatasourcePathSpec) config.getPathSpec()).getSegments()
    );
  }

  @Test
  public void testUpdateSegmentListIfDatasourcePathSpecIsUsedWithMultiplePathSpec() throws Exception
  {
    PathSpec pathSpec = new MultiplePathSpec(
        ImmutableList.of(
            new StaticPathSpec("/xyz", null),
            new DatasourcePathSpec(
                null,
                new DatasourceIngestionSpec(
                    TEST_DATA_SOURCE,
                    TEST_DATA_SOURCE_INTERVAL,
                    null,
                    null,
                    null,
                    null,
                    null,
                    false,
                    null
                ),
                null,
                false
            ),
            new DatasourcePathSpec(
                null,
                new DatasourceIngestionSpec(
                    TEST_DATA_SOURCE2,
                    TEST_DATA_SOURCE_INTERVAL2,
                    null,
                    null,
                    null,
                    null,
                    null,
                    false,
                    null
                ),
                null,
                false
            )
        )
    );
    HadoopDruidIndexerConfig config = testRunUpdateSegmentListIfDatasourcePathSpecIsUsed(
        pathSpec,
        TEST_DATA_SOURCE_INTERVAL
    );
    Assert.assertEquals(
        ImmutableList.of(WindowedDataSegment.of(SEGMENT)),
        ((DatasourcePathSpec) ((MultiplePathSpec) config.getPathSpec()).getChildren().get(1)).getSegments()
    );
    Assert.assertEquals(
        ImmutableList.of(new WindowedDataSegment(SEGMENT2, TEST_DATA_SOURCE_INTERVAL2)),
        ((DatasourcePathSpec) ((MultiplePathSpec) config.getPathSpec()).getChildren().get(2)).getSegments()
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
                ImmutableList.of(Intervals.of("2010-01-01/P1D"))
            ),
            null,
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
        segmentLister.getUsedSegmentsForIntervals(
            TEST_DATA_SOURCE,
            Collections.singletonList(jobInterval != null ? jobInterval.overlap(TEST_DATA_SOURCE_INTERVAL) : null)
        )
    ).andReturn(ImmutableList.of(SEGMENT));

    EasyMock.expect(
        segmentLister.getUsedSegmentsForIntervals(
            TEST_DATA_SOURCE2,
            Collections.singletonList(jobInterval != null ? jobInterval.overlap(TEST_DATA_SOURCE_INTERVAL2) : null)
        )
    ).andReturn(ImmutableList.of(SEGMENT2));

    EasyMock.replay(segmentLister);

    spec = HadoopIngestionSpec.updateSegmentListIfDatasourcePathSpecIsUsed(spec, jsonMapper, segmentLister);
    return HadoopDruidIndexerConfig.fromString(jsonMapper.writeValueAsString(spec));
  }
}
