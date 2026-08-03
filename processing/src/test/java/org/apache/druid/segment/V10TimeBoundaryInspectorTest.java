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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.projections.AggregateProjectionSchema;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.apache.druid.segment.projections.ProjectionSchema;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

class V10TimeBoundaryInspectorTest
{
  private static final Interval FALLBACK_INTERVAL = Intervals.of("2025-01-01/2025-01-02");

  @Test
  void testExactBoundsWhenMinMaxPresent()
  {
    final ProjectionMetadata projection = new ProjectionMetadata(
        100,
        someSchema(),
        DateTimes.of("2025-01-01T03:14:15").getMillis(),
        DateTimes.of("2025-01-01T21:12:11").getMillis()
    );

    final V10TimeBoundaryInspector inspector =
        V10TimeBoundaryInspector.forBaseProjection(projection, FALLBACK_INTERVAL);

    Assertions.assertTrue(inspector.isMinMaxExact(), "min/max from metadata are exact regardless of sort order");
    Assertions.assertEquals(DateTimes.of("2025-01-01T03:14:15"), inspector.getMinTime());
    Assertions.assertEquals(DateTimes.of("2025-01-01T21:12:11"), inspector.getMaxTime());
  }

  @Test
  void testInexactFallbackWhenMinMaxMissing()
  {
    final ProjectionMetadata projection = new ProjectionMetadata(100, someSchema());

    final V10TimeBoundaryInspector inspector =
        V10TimeBoundaryInspector.forBaseProjection(projection, FALLBACK_INTERVAL);

    Assertions.assertFalse(inspector.isMinMaxExact(), "no metadata min/max → fall back to inexact interval bounds");
    Assertions.assertEquals(FALLBACK_INTERVAL.getStart(), inspector.getMinTime());
    Assertions.assertEquals(FALLBACK_INTERVAL.getEnd(), inspector.getMaxTime());
  }

  @Test
  void testInexactFallbackWhenOnlyOneOfMinOrMaxMissing()
  {
    // both fields must be present to report exact; if either is null we fall back (this shouldn't happen)
    final ProjectionMetadata onlyMin = new ProjectionMetadata(
        100,
        someSchema(),
        DateTimes.of("2025-01-01T03:00:00").getMillis(),
        null
    );
    Assertions.assertFalse(
        V10TimeBoundaryInspector.forBaseProjection(onlyMin, FALLBACK_INTERVAL).isMinMaxExact()
    );

    final ProjectionMetadata onlyMax = new ProjectionMetadata(
        100,
        someSchema(),
        null,
        DateTimes.of("2025-01-01T21:00:00").getMillis()
    );
    Assertions.assertFalse(
        V10TimeBoundaryInspector.forBaseProjection(onlyMax, FALLBACK_INTERVAL).isMinMaxExact()
    );
  }

  private static ProjectionSchema someSchema()
  {
    return new AggregateProjectionSchema(
        "p",
        "time",
        null,
        VirtualColumns.create(Granularities.toVirtualColumn(Granularities.HOUR, "time")),
        Arrays.asList("a", "time"),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        Arrays.asList(OrderBy.ascending("a"), OrderBy.ascending("time"))
    );
  }
}
