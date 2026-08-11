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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class CompactionSkipStatisticsTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    final CompactionSkipStatistics stats = CompactionSkipStatistics.of(
        CompactionSkipReason.REJECTED_BY_SEARCH_POLICY,
        CompactionStatistics.create(100, 10L, 5, 2)
    );

    final CompactionSkipStatistics deserialized = mapper.readValue(
        mapper.writeValueAsString(stats),
        CompactionSkipStatistics.class
    );
    Assert.assertEquals(stats, deserialized);
    Assert.assertEquals(100, deserialized.getBytes());
    Assert.assertEquals(5, deserialized.getSegmentCount());
    Assert.assertEquals(2, deserialized.getIntervalCount());
  }

  @Test
  public void testCategoryIsSerialized()
  {
    Assert.assertEquals(
        CompactionSkipReason.Category.OUT_OF_SCOPE,
        CompactionSkipStatistics
            .of(CompactionSkipReason.SKIP_OFFSET, new CompactionStatistics())
            .getCategory()
    );
  }

  @Test
  public void testCategoryIsDerivedFromReasonAndNotReadFromPayload() throws Exception
  {
    final String payloadWithWrongCategory =
        "{\"reason\":\"SKIP_OFFSET\",\"category\":\"DEFERRED\","
        + "\"bytes\":1,\"segmentCount\":1,\"intervalCount\":1}";

    final CompactionSkipStatistics deserialized
        = mapper.readValue(payloadWithWrongCategory, CompactionSkipStatistics.class);
    Assert.assertEquals(CompactionSkipReason.SKIP_OFFSET, deserialized.getReason());
    Assert.assertEquals(CompactionSkipReason.Category.OUT_OF_SCOPE, deserialized.getCategory());
  }
}
