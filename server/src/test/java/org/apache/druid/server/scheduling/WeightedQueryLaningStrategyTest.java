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

package org.apache.druid.server.scheduling;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.QueryLaningStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class WeightedQueryLaningStrategyTest
{
  private static final Map<String, WeightedQueryLaningStrategy.LaneConfig> TWO_LANES = Map.of(
      "low", new WeightedQueryLaningStrategy.LaneConfig(1, 30),
      "very-low", new WeightedQueryLaningStrategy.LaneConfig(3, 10)
  );

  private Druids.TimeseriesQueryBuilder queryBuilder;

  @Before
  public void setup()
  {
    queryBuilder = Druids.newTimeseriesQueryBuilder()
                         .dataSource("test")
                         .intervals(List.of(Intervals.of("2020-01-01/2020-01-02")))
                         .granularity(Granularities.DAY)
                         .aggregators(new CountAggregatorFactory("count"));
  }

  @Test
  public void testGetLaneLimits()
  {
    WeightedQueryLaningStrategy strategy = newStrategy(null, null, 10, null);

    Object2IntMap<String> limits = strategy.getLaneLimits(100);
    Assert.assertEquals(2, limits.size());
    Assert.assertEquals(30, limits.getInt("low"));
    Assert.assertEquals(10, limits.getInt("very-low"));
  }

  @Test
  public void testComputeLane_noViolations()
  {
    WeightedQueryLaningStrategy strategy = newStrategy(null, null, 10000, null);
    TimeseriesQuery query = queryBuilder.build();
    Optional<String> lane = strategy.computeLane(QueryPlus.wrap(query), Set.of());
    Assert.assertFalse(lane.isPresent());
  }

  @Test
  public void testComputeLane_oneViolation_segmentCount()
  {
    // segmentCountThreshold=1, query has 5 segments → score=1 → "low"
    WeightedQueryLaningStrategy strategy = newStrategy(null, null, 1, null);
    TimeseriesQuery query = queryBuilder.build();
    Set<SegmentServerSelector> segments = makeSegments(5);
    Optional<String> lane = strategy.computeLane(QueryPlus.wrap(query), segments);
    Assert.assertTrue(lane.isPresent());
    Assert.assertEquals("low", lane.get());
  }

  @Test
  public void testComputeLane_twoViolations_matchesLowerMinScore()
  {
    // segmentCountThreshold=1 + durationThreshold=PT1S → score=2
    // Matches "low" (minScore=1) but NOT "very-low" (minScore=3)
    WeightedQueryLaningStrategy strategy = new WeightedQueryLaningStrategy(
        null,
        "PT1S",
        1,
        null,
        TWO_LANES
    );
    TimeseriesQuery query = queryBuilder.build();
    Set<SegmentServerSelector> segments = makeSegments(5);
    Optional<String> lane = strategy.computeLane(QueryPlus.wrap(query), segments);
    Assert.assertTrue(lane.isPresent());
    Assert.assertEquals("low", lane.get());
  }

  @Test
  public void testComputeLane_allViolations_mostRestrictiveLane()
  {
    // All 4 thresholds set very low → score=4 → meets "very-low" (minScore=3)
    WeightedQueryLaningStrategy strategy = new WeightedQueryLaningStrategy(
        "PT1S",
        "PT1S",
        1,
        "PT1S",
        TWO_LANES
    );
    TimeseriesQuery query = queryBuilder.build();
    Set<SegmentServerSelector> segments = makeSegments(5);
    Optional<String> lane = strategy.computeLane(QueryPlus.wrap(query), segments);
    Assert.assertTrue(lane.isPresent());
    Assert.assertEquals("very-low", lane.get());
  }

  @Test
  public void testComputeLane_existingLane_preserved()
  {
    WeightedQueryLaningStrategy strategy = newStrategy(null, null, 10000, null);
    TimeseriesQuery query = queryBuilder
        .context(Map.of(QueryContexts.LANE_KEY, "custom"))
        .build();
    Optional<String> lane = strategy.computeLane(QueryPlus.wrap(query), Set.of());
    Assert.assertTrue(lane.isPresent());
    Assert.assertEquals("custom", lane.get());
  }

  @Test
  public void testComputeLane_segmentRangeWithDifferentIntervals()
  {
    // Segments spanning different intervals to validate segmentRange summing
    WeightedQueryLaningStrategy strategy = newStrategy(null, null, null, "PT1S");
    TimeseriesQuery query = queryBuilder.build();
    Set<SegmentServerSelector> segments = new HashSet<>();
    segments.add(new SegmentServerSelector(
        new SegmentDescriptor(Intervals.of("2020-01-01/2020-01-02"), "v1", 0)
    ));
    segments.add(new SegmentServerSelector(
        new SegmentDescriptor(Intervals.of("2020-01-02/2020-01-03"), "v1", 1)
    ));
    // Total range = 1 day + 1 day = 2 days > 1 second → score=1 → "low"
    Optional<String> lane = strategy.computeLane(QueryPlus.wrap(query), segments);
    Assert.assertTrue(lane.isPresent());
    Assert.assertEquals("low", lane.get());
  }

  @Test
  public void testValidation_noThresholds()
  {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> newStrategy(null, null, null, null)
    );
  }

  @Test
  public void testValidation_noLanes()
  {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new WeightedQueryLaningStrategy(
            null,
            null,
            10,
            null,
            Map.of()
        )
    );
  }

  @Test
  public void testValidation_nullLanes()
  {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new WeightedQueryLaningStrategy(
            null,
            null,
            10,
            null,
            null
        )
    );
  }

  @Test
  public void testLaneConfig_invalidMinScore()
  {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new WeightedQueryLaningStrategy.LaneConfig(0, 30)
    );
  }

  @Test
  public void testLaneConfig_invalidMaxPercent()
  {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new WeightedQueryLaningStrategy.LaneConfig(1, 0)
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    String json = "{\n"
                  + "  \"strategy\": \"weighted\",\n"
                  + "  \"segmentCountThreshold\": 1000,\n"
                  + "  \"durationThreshold\": \"P1D\",\n"
                  + "  \"lanes\": {\n"
                  + "    \"low\": { \"minScore\": 1, \"maxPercent\": 30 },\n"
                  + "    \"very-low\": { \"minScore\": 3, \"maxPercent\": 10 }\n"
                  + "  }\n"
                  + "}";

    QueryLaningStrategy deserialized = mapper.readValue(json, QueryLaningStrategy.class);
    Assert.assertTrue(deserialized instanceof WeightedQueryLaningStrategy);

    Object2IntMap<String> limits = deserialized.getLaneLimits(100);
    Assert.assertEquals(30, limits.getInt("low"));
    Assert.assertEquals(10, limits.getInt("very-low"));
  }

  private static WeightedQueryLaningStrategy newStrategy(
      String periodThreshold,
      String durationThreshold,
      Integer segmentCountThreshold,
      String segmentRangeThreshold
  )
  {
    return new WeightedQueryLaningStrategy(
        periodThreshold,
        durationThreshold,
        segmentCountThreshold,
        segmentRangeThreshold,
        TWO_LANES
    );
  }

  private static Set<SegmentServerSelector> makeSegments(int count)
  {
    Set<SegmentServerSelector> segments = new HashSet<>();
    for (int i = 0; i < count; i++) {
      segments.add(new SegmentServerSelector(
          new SegmentDescriptor(Intervals.of("2020-01-01/2020-01-02"), "v1", i)
      ));
    }
    return segments;
  }
}
