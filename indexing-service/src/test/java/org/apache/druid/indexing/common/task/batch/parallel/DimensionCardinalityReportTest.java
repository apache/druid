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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.datasketches.hll.HllSketch;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestHelper;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DimensionCardinalityReportTest
{
  private static final ObjectMapper OBJECT_MAPPER = ParallelIndexTestingFactory.createObjectMapper();

  private DimensionCardinalityReport target;

  @Before
  public void setup()
  {
    Interval interval = Intervals.ETERNITY;
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    Map<Interval, byte[]> intervalToCardinality = Collections.singletonMap(interval, collector.toByteArray());
    String taskId = "abc";
    target = new DimensionCardinalityReport(taskId, intervalToCardinality);
  }

  @Test
  public void serializesDeserializes()
  {
    TestHelper.testSerializesDeserializes(OBJECT_MAPPER, target);
  }

  @Test
  public void abidesEqualsContract()
  {
    EqualsVerifier.forClass(DimensionCardinalityReport.class)
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testSupervisorDetermineNumShardsFromCardinalityReport()
  {
    List<DimensionCardinalityReport> reports = new ArrayList<>();

    HllSketch collector1 = DimensionCardinalityReport.createHllSketchForReport();
    collector1.update(IndexTask.HASH_FUNCTION.hashLong(1L).asBytes());
    collector1.update(IndexTask.HASH_FUNCTION.hashLong(200L).asBytes());
    DimensionCardinalityReport report1 = new DimensionCardinalityReport(
        "taskA",
        ImmutableMap.of(
            Intervals.of("1970-01-01T00:00:00.000Z/1970-01-02T00:00:00.000Z"),
            collector1.toCompactByteArray()
        )
    );
    reports.add(report1);

    HllSketch collector2 = DimensionCardinalityReport.createHllSketchForReport();
    collector2.update(IndexTask.HASH_FUNCTION.hashLong(1000L).asBytes());
    collector2.update(IndexTask.HASH_FUNCTION.hashLong(30000L).asBytes());
    DimensionCardinalityReport report2 = new DimensionCardinalityReport(
        "taskB",
        ImmutableMap.of(
            Intervals.of("1970-01-01T00:00:00.000Z/1970-01-02T00:00:00.000Z"),
            collector2.toCompactByteArray()
        )
    );
    reports.add(report2);

    // Separate interval with only 1 value
    HllSketch collector3 = DimensionCardinalityReport.createHllSketchForReport();
    collector3.update(IndexTask.HASH_FUNCTION.hashLong(99000L).asBytes());
    DimensionCardinalityReport report3 = new DimensionCardinalityReport(
        "taskC",
        ImmutableMap.of(
            Intervals.of("1970-01-02T00:00:00.000Z/1970-01-03T00:00:00.000Z"),
            collector3.toCompactByteArray()
        )
    );
    reports.add(report3);

    // first interval in test has cardinality 4
    Map<Interval, Integer> intervalToNumShards = ParallelIndexSupervisorTask.determineNumShardsFromCardinalityReport(
        reports,
        1
    );
    Assert.assertEquals(
        ImmutableMap.of(
            Intervals.of("1970-01-01/P1D"),
            4,
            Intervals.of("1970-01-02/P1D"),
            1
        ),
        intervalToNumShards
    );

    intervalToNumShards = ParallelIndexSupervisorTask.determineNumShardsFromCardinalityReport(
        reports,
        2
    );
    Assert.assertEquals(
        ImmutableMap.of(
            Intervals.of("1970-01-01/P1D"),
            2,
            Intervals.of("1970-01-02/P1D"),
            1
        ),
        intervalToNumShards
    );

    intervalToNumShards = ParallelIndexSupervisorTask.determineNumShardsFromCardinalityReport(
        reports,
        3
    );
    Assert.assertEquals(
        ImmutableMap.of(
            Intervals.of("1970-01-01/P1D"),
            1,
            Intervals.of("1970-01-02/P1D"),
            1
        ),
        intervalToNumShards
    );

    intervalToNumShards = ParallelIndexSupervisorTask.determineNumShardsFromCardinalityReport(
        reports,
        4
    );
    Assert.assertEquals(
        ImmutableMap.of(
            Intervals.of("1970-01-01/P1D"),
            1,
            Intervals.of("1970-01-02/P1D"),
            1
        ),
        intervalToNumShards
    );

    intervalToNumShards = ParallelIndexSupervisorTask.determineNumShardsFromCardinalityReport(
        reports,
        5
    );
    Assert.assertEquals(
        ImmutableMap.of(
            Intervals.of("1970-01-01/P1D"),
            1,
            Intervals.of("1970-01-02/P1D"),
            1
        ),
        intervalToNumShards
    );
  }
}
