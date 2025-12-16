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

package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class PersistedCompactionStateManagerConfigTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String json = "{}";
    CompactionStateManagerConfig config = mapper.readValue(json, CompactionStateManagerConfig.class);
    Assert.assertEquals(100, config.getCacheSize());
    Assert.assertEquals(100, config.getPrewarmFingerprintCount());
  }

  @Test
  public void testSerdeRoundTripWithOverrides() throws Exception
  {
    String json = "{\"cacheSize\": 1000, \"prewarmFingerprintCount\": 500}";
    CompactionStateManagerConfig config = mapper.readValue(json, CompactionStateManagerConfig.class);
    String serialized = mapper.writeValueAsString(config);
    CompactionStateManagerConfig deserialized = mapper.readValue(serialized, CompactionStateManagerConfig.class);
    Assert.assertEquals(1000, deserialized.getCacheSize());
    Assert.assertEquals(500, deserialized.getPrewarmFingerprintCount());
  }
}
