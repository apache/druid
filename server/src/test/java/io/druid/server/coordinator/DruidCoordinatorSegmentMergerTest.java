/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.coordinator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import junit.framework.Assert;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class DruidCoordinatorSegmentMergerTest
{
  private static final long mergeBytesLimit = 100;
  private static final int mergeSegmentsLimit = 8;

  @Test
  public void testNoMerges()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(), merge(segments)
    );
  }

  @Test
  public void testMergeAtStart()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(90).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(0), segments.get(1))
        ), merge(segments)
    );
  }

  @Test
  public void testMergeAtEnd()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(20).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(2), segments.get(3))
        ), merge(segments)
    );
  }

  @Test
  public void testMergeInMiddle()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(10).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(20).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(1), segments.get(2))
        ), merge(segments)
    );
  }

  @Test
  public void testMergeNoncontiguous()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(10).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(10).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(10).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(0), segments.get(1), segments.get(2))
        ), merge(segments)
    );
  }

  @Test
  public void testMergeSeriesByteLimited()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(40).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(40).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(40).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(40).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("2").size(40).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(40).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(0), segments.get(1)),
            ImmutableList.of(segments.get(2), segments.get(3)),
            ImmutableList.of(segments.get(4), segments.get(5))
        ), merge(segments)
    );
  }

  @Test
  public void testMergeSeriesSegmentLimited()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("2").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-07/P1D")).version("2").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-08/P1D")).version("2").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-09/P1D")).version("2").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-10/P1D")).version("2").size(1).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(
                segments.get(0),
                segments.get(1),
                segments.get(2),
                segments.get(3),
                segments.get(4),
                segments.get(5),
                segments.get(6),
                segments.get(7)
            ),
            ImmutableList.of(segments.get(8), segments.get(9))
        ), merge(segments)
    );
  }

  @Test
  public void testOverlappingMergeWithBacktracking()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P4D")).version("2").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("3").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("4").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("3").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-07/P1D")).version("2").size(20).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(0), segments.get(1)),
            ImmutableList.of(segments.get(2), segments.get(3), segments.get(4), segments.get(5), segments.get(6))
        ), merge(segments)
    );
  }

  @Test
  public void testOverlappingMergeWithGapsAlignedStart()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P8D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-09/P1D")).version("3").size(8).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(1), segments.get(0), segments.get(2))
        ), merge(segments)
    );
  }

  @Test
  public void testOverlappingMergeWithGapsNonalignedStart()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P8D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-09/P1D")).version("3").size(8).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(0), segments.get(1), segments.get(2))
        ), merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge1()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("1").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(), merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge2()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(15).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("4").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(2), segments.get(3), segments.get(4))
        ), merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge3()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("1").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(1), segments.get(2), segments.get(4))
        ), merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge4()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("4").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(1), segments.get(2), segments.get(3), segments.get(4))
        ), merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge5()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("1").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(), merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge6()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("4").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(25).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(2), segments.get(3), segments.get(4))
        ), merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge7()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(120).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("4").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(2), segments.get(3), segments.get(4), segments.get(5))
        ), merge(segments)
    );
  }

  @Test
  public void testOverlappingMerge8()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(120).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("1").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(ImmutableList.of(ImmutableList.of(segments.get(4), segments.get(5))), merge(segments));
  }

  @Test
  public void testMergeLinearShardSpecs()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(1))
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-02/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(7))
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(1500))
                   .build()
    );

    Assert.assertEquals(
        ImmutableList.of(),
        merge(segments)
    );
  }

  @Test
  public void testMergeMixedShardSpecs()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-02/P1D"))
                   .version("1")
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(1500))
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-04/P1D"))
                   .version("1")
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-05/P1D"))
                   .version("1")
                   .build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableList.of(segments.get(0), segments.get(1)),
            ImmutableList.of(segments.get(3), segments.get(4))
        ),
        merge(segments)
    );
  }

  /**
   * Runs DruidCoordinatorSegmentMerger on a particular set of segments and returns the list of requested merges.
   */
  private static List<List<DataSegment>> merge(final Collection<DataSegment> segments)
  {
    final List<List<DataSegment>> retVal = Lists.newArrayList();
    final IndexingServiceClient indexingServiceClient = new IndexingServiceClient(null, null, null)
    {
      @Override
      public void mergeSegments(List<DataSegment> segmentsToMerge)
      {
        retVal.add(segmentsToMerge);
      }
    };

    final AtomicReference<DatasourceWhitelist> whitelistRef = new AtomicReference<DatasourceWhitelist>(null);
    final DruidCoordinatorSegmentMerger merger = new DruidCoordinatorSegmentMerger(indexingServiceClient, whitelistRef);
    final DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams.newBuilder()
                                                                    .withAvailableSegments(ImmutableSet.copyOf(segments))
                                                                    .withDynamicConfigs(
                                                                        new CoordinatorDynamicConfig.Builder().withMergeBytesLimit(
                                                                            mergeBytesLimit
                                                                        ).withMergeSegmentsLimit(
                                                                            mergeSegmentsLimit
                                                                        )
                                                                                                         .build()
                                                                    )
                                                                    .build();
    merger.run(params);
    return retVal;
  }
}
