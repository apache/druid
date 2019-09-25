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

package org.apache.druid.query.scan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class ScanQueryConfigTest
{
  private final ObjectMapper MAPPER = new DefaultObjectMapper();

  private final ImmutableMap<String, String> CONFIG_MAP = ImmutableMap
      .<String, String>builder()
      .put("maxSegmentPartitionsOrderedInMemory", "1")
      .put("maxRowsQueuedForOrdering", "1")
      .put("legacy", "true")
      .build();

  private final ImmutableMap<String, String> CONFIG_MAP2 = ImmutableMap
      .<String, String>builder()
      .put("legacy", "false")
      .put("maxSegmentPartitionsOrderedInMemory", "42")
      .build();

  private final ImmutableMap<String, String> CONFIG_MAP_EMPTY = ImmutableMap
      .<String, String>builder()
      .build();

  @Test
  public void testSerde()
  {
    final ScanQueryConfig config = MAPPER.convertValue(CONFIG_MAP, ScanQueryConfig.class);
    Assert.assertEquals(1, config.getMaxRowsQueuedForOrdering());
    Assert.assertEquals(1, config.getMaxSegmentPartitionsOrderedInMemory());
    Assert.assertTrue(config.isLegacy());

    final ScanQueryConfig config2 = MAPPER.convertValue(CONFIG_MAP2, ScanQueryConfig.class);
    Assert.assertEquals(100000, config2.getMaxRowsQueuedForOrdering());
    Assert.assertEquals(42, config2.getMaxSegmentPartitionsOrderedInMemory());
    Assert.assertFalse(config2.isLegacy());

    final ScanQueryConfig config3 = MAPPER.convertValue(CONFIG_MAP_EMPTY, ScanQueryConfig.class);
    Assert.assertEquals(100000, config3.getMaxRowsQueuedForOrdering());
    Assert.assertEquals(50, config3.getMaxSegmentPartitionsOrderedInMemory());
    Assert.assertFalse(config3.isLegacy());
  }
}
