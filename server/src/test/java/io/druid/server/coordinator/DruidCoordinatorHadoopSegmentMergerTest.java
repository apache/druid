/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metamx.common.Pair;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.server.coordinator.helper.DruidCoordinatorHadoopSegmentMerger;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.SingleDimensionShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DruidCoordinatorHadoopSegmentMergerTest
{
  private static final long mergeBytesLimit = 100;

  private static DruidCoordinatorConfig config;

  @Before
  public void setUp() throws Exception
  {
    config = new TestDruidCoordinatorConfig();
  }

  @Test
  public void testNoMerges()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(100).build()
    );

    Assert.assertEquals(
        ImmutableList.of(),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(),
        merge(segments, false, false)
    );
  }

  @Test
  public void testMergeAtStart()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(100).build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-01/P2D")))),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-01/P2D")))),
        merge(segments, false, false)
    );
  }

  @Test
  public void testMergeAtEnd()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(20).build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-03/P2D")))),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-03/P2D")))),
        merge(segments, false, false)
    );
  }

  @Test
  public void testMergeInMiddle()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(100).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(100).build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-02/P2D")))),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-02/P2D")))),
        merge(segments, false, false)
    );
  }

  @Test
  public void testMergeNoncontiguous()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(10).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(30).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(70).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("2").size(30).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-07/P1D")).version("2").size(150).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(
                    new Interval("2012-01-01/2012-01-05"),
                    new Interval("2012-01-05/2012-01-08")
                )
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-04/2012-01-06")))),
        merge(segments, false, false)
    );
  }

  @Test
  public void testMergeNoncontiguousWithKeepGap()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(10).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(70).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("2").size(30).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-07/P1D")).version("2").size(150).build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-03/2012-01-06")))),
        merge(segments, true, true)
    );
    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-04/2012-01-06")))),
        merge(segments, true, false)
    );
  }

  @Test
  public void testMergeSeries()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("2").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("2").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("2").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(50).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(
                    new Interval("2012-01-01/P2D"),
                    new Interval("2012-01-03/P2D"),
                    new Interval("2012-01-05/P2d")
                )
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(
                    new Interval("2012-01-05/P2d"),
                    new Interval("2012-01-03/P2D"),
                    new Interval("2012-01-01/P2D")
                )
            )
        ),
        merge(segments, false, false)
    );
  }

  @Test
  public void testOverlappingMerge1()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P4D")).version("2").size(30).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("3").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("4").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("3").size(20).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-07/P1D")).version("2").size(40).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(
                    new Interval("2012-01-01/2012-01-04"),
                    new Interval("2012-01-04/2012-01-08")
                )
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-04/P4D"), new Interval("2012-01-01/P3D"))
            )
        ), merge(segments, false, false)
    );
  }

  @Test
  public void testOverlappingMerge2()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P8D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-09/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-10/P1D")).version("3").size(8).build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-01/2012-01-09")))),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-04/2012-01-11")))),
        merge(segments, false, false)
    );
  }

  @Test
  public void testOverlappingMerge3()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P8D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-09/P1D")).version("3").size(8).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-10/P1D")).version("3").size(8).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-01/2012-01-04"), new Interval("2012-01-04/2012-01-11"))
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-04/2012-01-11"), new Interval("2012-01-01/2012-01-04"))
            )
        ),
        merge(segments, false, false)
    );
  }

  @Test
  public void testOverlappingMerge4()
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
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(
                    new Interval("2012-01-01/2012-01-03"),
                    new Interval("2012-01-03/2012-01-05"),
                    new Interval("2012-01-05/2012-01-07")
                )
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(
                    new Interval("2012-01-05/2012-01-07"),
                    new Interval("2012-01-03/2012-01-05"),
                    new Interval("2012-01-01/2012-01-03")
                )
            )
        ),
        merge(segments, false, false)
    );
  }

  @Test
  public void testOverlappingMerge5()
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
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-01/2012-01-04"), new Interval("2012-01-04/2012-01-07"))
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-05/2012-01-07"), new Interval("2012-01-02/2012-01-05"))
            )
        ),
        merge(segments, false, false)
    );
  }

  @Test
  public void testOverlappingMerge6()
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
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-01/2012-01-03"), new Interval("2012-01-03/2012-01-07"))
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-04/2012-01-07"), new Interval("2012-01-01/2012-01-04"))
            )
        ),
        merge(segments, false, false)
    );
  }

  @Test
  public void testOverlappingMerge7()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("4").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(97).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-01/2012-01-03"), new Interval("2012-01-03/2012-01-07"))
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-03/2012-01-07"), new Interval("2012-01-01/2012-01-03"))
            )
        ),
        merge(segments, false, false)
    );
  }

  @Test
  public void testOverlappingMerge8()
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
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-01/2012-01-04"), new Interval("2012-01-04/2012-01-06"))
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-05/2012-01-07"), new Interval("2012-01-03/2012-01-05"))
            )
        ),
        merge(segments, false, false)
    );
  }

  @Test
  public void testOverlappingMerge9()
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
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-01/2012-01-04"), new Interval("2012-01-04/2012-01-07"))
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-05/2012-01-07"), new Interval("2012-01-02/2012-01-05"))
            )
        ),
        merge(segments, false, false)
    );
  }

  @Test
  public void testOverlappingMerge10()
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
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-01/2012-01-03")))),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-02/2012-01-07")))),
        merge(segments, false, false)
    );
  }

  @Test
  public void testOverlappingMerge11()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(80).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P4D")).version("2").size(120).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-03/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P1D")).version("1").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(1).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("2").size(80).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-01/2012-01-03"), new Interval("2012-01-03/2012-01-05"))
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-04/2012-01-07"), new Interval("2012-01-02/2012-01-04"))
            )
        ),
        merge(segments, false, false)
    );
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
                   .size(30)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(2))
                   .size(20)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(20))
                   .size(200)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-02/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(7))
                   .size(40)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(1))
                   .size(30)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(1500))
                   .size(30)
                   .build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(
                    new Interval("2012-01-01/2012-01-02"),
                    new Interval("2012-01-02/2012-01-04")
                )
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(
                    new Interval("2012-01-02/2012-01-04"),
                    new Interval("2012-01-01/2012-01-02")
                )
            )
        ),
        merge(segments, false, false)
    );
  }

  @Test
  public void testMergeIncompleteNumberedShardSpecs()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 2))
                   .size(30)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-02/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(1, 2))
                   .size(40)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(2, 1500))
                   .size(30)
                   .build()
    );

    Assert.assertEquals(
        ImmutableList.of(),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(),
        merge(segments, false, false)
    );
  }

  @Test
  public void testMergeNumberedShardSpecs()
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 2))
                   .size(30)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(1, 2))
                   .size(30)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-02/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 2))
                   .size(20)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-02/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(1, 2))
                   .size(20)
                   .build()
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-01/2012-01-03")))),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(Pair.of("foo", ImmutableList.of(new Interval("2012-01-01/2012-01-03")))),
        merge(segments, false, false)
    );
  }

  @Test
  public void testMergeMixedShardSpecs()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 2))
                   .size(25)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-01/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(1, 2))
                   .size(25)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("1")
                   .size(50)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-04/P1D"))
                   .version("1")
                   .shardSpec(new LinearShardSpec(1500))
                   .size(100)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-05/P1D"))
                   .version("1")
                   .shardSpec(new NumberedShardSpec(0, 1500))
                   .size(1)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-06/P1D"))
                   .version("1")
                   .shardSpec(new HashBasedNumberedShardSpec(0, 2, mapper))
                   .size(25)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-06/P1D"))
                   .version("1")
                   .shardSpec(new HashBasedNumberedShardSpec(1, 2, mapper))
                   .size(25)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-07/P1D"))
                   .version("1")
                   .shardSpec(new SingleDimensionShardSpec("dim", null, "a", 0))
                   .size(25)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-07/P1D"))
                   .version("1")
                   .shardSpec(new SingleDimensionShardSpec("dim", "a", "b", 1))
                   .size(25)
                   .build(),
        DataSegment.builder().dataSource("foo")
                   .interval(new Interval("2012-01-07/P1D"))
                   .version("1")
                   .shardSpec(new SingleDimensionShardSpec("dim", "b", null, 2))
                   .size(25)
                   .build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-01/2012-01-04"), new Interval("2012-01-06/2012-01-08"))
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-03/2012-01-05"), new Interval("2012-01-06/2012-01-08"))
            )
        ),
        merge(segments, true, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-06/2012-01-08"), new Interval("2012-01-01/2012-01-04"))
            )
        ),
        merge(segments, false, false)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(new Interval("2012-01-06/2012-01-08"))
            )
        ),
        merge(segments, true, false)
    );
  }

  /**
   * Segment timeline
   * <p/>
   * Day:    1   2   3   4   5   6   7   8   9   10  11  12  13  14
   * Shard0: |_O_|_O_|_O_|_O_|_S_|_S_|_S_|_O_|_S_|_O_|_S_|_O_|_S_|
   * Shard1:         |_S_|                   |_S_|   |_S_|_S_|
   * Shard2:         |_S_|                   |_S_|   |_S_|
   */
  @Test
  public void testMergeComplex()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-01/P1D")).version("2").size(120).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-02/P1D")).version("2").size(120).build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("3")
                   .shardSpec(new NumberedShardSpec(0, 3))
                   .size(500)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("3")
                   .shardSpec(new NumberedShardSpec(1, 3))
                   .size(20)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-03/P1D"))
                   .version("3")
                   .shardSpec(new NumberedShardSpec(2, 3))
                   .size(20)
                   .build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-04/P5D")).version("1").size(500).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-05/P1D")).version("3").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-06/P1D")).version("3").size(50).build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-07/P1D")).version("2").size(80).build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-09/P1D"))
                   .version("2")
                   .shardSpec(new SingleDimensionShardSpec("dim", null, "a", 0))
                   .size(10)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-09/P1D"))
                   .version("2")
                   .shardSpec(new SingleDimensionShardSpec("dim", "a", "b", 1))
                   .size(10)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-09/P1D"))
                   .version("2")
                   .shardSpec(new SingleDimensionShardSpec("dim", "b", null, 2))
                   .size(10)
                   .build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-10/P1D")).version("2").size(100).build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-11/P1D"))
                   .version("2")
                   .shardSpec(new HashBasedNumberedShardSpec(0, 3, mapper))
                   .size(50)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-11/P1D"))
                   .version("2")
                   .shardSpec(new HashBasedNumberedShardSpec(1, 3, mapper))
                   .size(50)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-11/P1D"))
                   .version("2")
                   .shardSpec(new HashBasedNumberedShardSpec(2, 3, mapper))
                   .size(50)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-12/P1D"))
                   .version("2")
                   .shardSpec(new NumberedShardSpec(0, 2))
                   .size(100)
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(new Interval("2012-01-12/P1D"))
                   .version("2")
                   .shardSpec(new NumberedShardSpec(1, 2))
                   .size(1)
                   .build(),
        DataSegment.builder().dataSource("foo").interval(new Interval("2012-01-13/P1D")).version("2").size(50).build()
    );

    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(
                    new Interval("2012-01-03/2012-01-04"),
                    new Interval("2012-01-05/2012-01-07"),
                    new Interval("2012-01-07/2012-01-09"),
                    new Interval("2012-01-09/2012-01-11"),
                    new Interval("2012-01-11/2012-01-12"),
                    new Interval("2012-01-12/2012-01-13")
                )
            )
        ),
        merge(segments, false, true)
    );
    Assert.assertEquals(
        ImmutableList.of(
            Pair.of(
                "foo",
                ImmutableList.of(
                    new Interval("2012-01-12/2012-01-14"),
                    new Interval("2012-01-11/2012-01-12"),
                    new Interval("2012-01-08/2012-01-10"),
                    new Interval("2012-01-06/2012-01-08"),
                    new Interval("2012-01-04/2012-01-06"),
                    new Interval("2012-01-03/2012-01-04")
                )
            )
        ),
        merge(segments, false, false)
    );
  }


  /**
   * Runs DruidCoordinatorHadoopSegmentMerger on a particular set of segments and returns the list of unbalanced
   * sections that should be reindexed.
   */
  private static List<Pair<String, List<Interval>>> merge(
      final Collection<DataSegment> segments,
      final boolean keepSegmentGapDuringMerge,
      boolean scanFromOldToNew
  )
  {
    final List<Pair<String, List<Interval>>> retVal = Lists.newArrayList();
    final IndexingServiceClient indexingServiceClient = new IndexingServiceClient(null, null, null)
    {

      @Override
      public String hadoopMergeSegments(
          String dataSource,
          List<Interval> intervalsToReindex,
          AggregatorFactory[] aggregators,
          QueryGranularity queryGranularity,
          List<String> dimensions,
          Map<String, Object> tuningConfig,
          List<String> hadoopCoordinates
      )
      {
        retVal.add(Pair.of(dataSource, intervalsToReindex));
        return null;
      }

      @Override
      public List<Map<String, Object>> getIncompleteTasks()
      {
        return null;
      }
    };

    final AtomicReference<DatasourceWhitelist> whitelistRef = new AtomicReference<DatasourceWhitelist>(null);
    final DruidCoordinatorHadoopSegmentMerger merger = new DruidCoordinatorHadoopSegmentMerger(
        indexingServiceClient,
        whitelistRef,
        scanFromOldToNew
    );
    final DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams.newBuilder()
                                                                              .withAvailableSegments(
                                                                                  ImmutableSet.copyOf(
                                                                                      segments
                                                                                  )
                                                                              )
                                                                              .withDynamicConfigs(
                                                                                  new CoordinatorDynamicConfig.Builder()
                                                                                      .withMergeBytesLimit(
                                                                                          mergeBytesLimit
                                                                                      )
                                                                                      .withhadoopMergeConfig(
                                                                                          new DruidCoordinatorHadoopMergeConfig(
                                                                                              keepSegmentGapDuringMerge,
                                                                                              null,
                                                                                              null,
                                                                                              ImmutableList.<CoordinatorHadoopMergeSpec>of(
                                                                                                  new CoordinatorHadoopMergeSpec(
                                                                                                      "foo",
                                                                                                      null,
                                                                                                      null,
                                                                                                      null
                                                                                                  ))
                                                                                          ))
                                                                                      .build()
                                                                              )
                                                                              .build();
    merger.run(params);
    return retVal;
  }
}
