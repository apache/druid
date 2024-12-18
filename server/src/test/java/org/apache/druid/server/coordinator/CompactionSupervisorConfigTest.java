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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class CompactionSupervisorConfigTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Test
  public void testCompactionSupervisorConfigSerde() throws JsonProcessingException
  {
    final boolean enabled = true;
    final CompactionEngine defaultEngine = CompactionEngine.MSQ;
    CompactionSupervisorConfig compactionSupervisorConfig =
        OBJECT_MAPPER.readValue(
            OBJECT_MAPPER.writeValueAsString(ImmutableMap.of("enabled", enabled, "engine", defaultEngine)),
            CompactionSupervisorConfig.class
        );
    Assert.assertEquals(new CompactionSupervisorConfig(enabled, defaultEngine), compactionSupervisorConfig);
  }

  @Test
  public void testCompactionSupervisorConfigEquality()
  {
    Assert.assertEquals(
        new CompactionSupervisorConfig(true, CompactionEngine.MSQ),
        new CompactionSupervisorConfig(true, CompactionEngine.MSQ)
    );
    Assert.assertNotEquals(
        new CompactionSupervisorConfig(true, CompactionEngine.NATIVE),
        new CompactionSupervisorConfig(true, CompactionEngine.MSQ)
    );
    Assert.assertNotEquals(new CompactionSupervisorConfig(true, CompactionEngine.NATIVE), "true");
  }
}
