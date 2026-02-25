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

package org.apache.druid.server.compaction;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class CompactionCandidateTest
{
  private static final String DATASOURCE = "test_datasource";

  @Test
  public void testFrom()
  {
    List<DataSegment> segments = createTestSegments(3);

    CompactionCandidate candidate = CompactionCandidate.from(segments, null);

    Assert.assertEquals(segments, candidate.getSegments());
    Assert.assertEquals(DATASOURCE, candidate.getDataSource());
    Assert.assertEquals(3, candidate.numSegments());
    Assert.assertNotNull(candidate.getUmbrellaInterval());
    Assert.assertNotNull(candidate.getCompactionInterval());
    Assert.assertNotNull(candidate.getStats());
  }

  @Test
  public void testThrowsOnNullOrEmptySegments()
  {
    Assert.assertThrows(
        DruidException.class,
        () -> CompactionCandidate.from(null, null)
    );

    Assert.assertThrows(
        DruidException.class,
        () -> CompactionCandidate.from(Collections.emptyList(), null)
    );
  }

  private static List<DataSegment> createTestSegments(int count)
  {
    return CreateDataSegments.ofDatasource(DATASOURCE)
                             .forIntervals(count, Granularities.DAY)
                             .startingAt("2024-01-01")
                             .eachOfSizeInMb(100);
  }
}
