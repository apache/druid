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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class SegmentsMetadataManagerConfigTest
{
  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  @Test
  public void testDefaults() throws Exception
  {
    final SegmentsMetadataManagerConfig config =
        mapper.readValue("{}", SegmentsMetadataManagerConfig.class);

    Assert.assertEquals(SegmentMetadataCache.UsageMode.IF_SYNCED, config.getCacheUsageMode());
    // Incremental sync is opt-in and off by default
    Assert.assertFalse(config.isUseIncrementalSync());
  }

  @Test
  public void testDeserializeUseIncrementalSync() throws Exception
  {
    final SegmentsMetadataManagerConfig config =
        mapper.readValue("{\"useIncrementalSync\": true}", SegmentsMetadataManagerConfig.class);
    Assert.assertTrue(config.isUseIncrementalSync());
  }
}
