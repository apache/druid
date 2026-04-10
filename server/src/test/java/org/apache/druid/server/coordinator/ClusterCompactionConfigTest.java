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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.compaction.NewestSegmentFirstPolicy;
import org.junit.Assert;
import org.junit.Test;

public class ClusterCompactionConfigTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testDefaults()
  {
    ClusterCompactionConfig config = new ClusterCompactionConfig(null, null, null, null, null, null);

    Assert.assertEquals(0.1, config.getCompactionTaskSlotRatio(), 0.0001);
    Assert.assertEquals(Integer.MAX_VALUE, config.getMaxCompactionTaskSlots());
    Assert.assertTrue(config.isUseSupervisors());
    Assert.assertEquals(CompactionEngine.NATIVE, config.getEngine());
    Assert.assertNotNull(config.getCompactionPolicy());
    Assert.assertTrue(config.isStoreCompactionStatePerSegment());
  }

  @Test
  public void testSerde() throws Exception
  {
    ClusterCompactionConfig config = ClusterCompactionConfig.builder()
                                                            .compactionTaskSlotRatio(0.5)
                                                            .maxCompactionTaskSlots(10)
                                                            .useSupervisors(false)
                                                            .engine(CompactionEngine.NATIVE)
                                                            .storeCompactionStatePerSegment(false)
                                                            .build();

    String json = MAPPER.writeValueAsString(config);
    ClusterCompactionConfig deserialized = MAPPER.readValue(json, ClusterCompactionConfig.class);

    Assert.assertEquals(config, deserialized);
  }

  @Test
  public void testBuilder()
  {
    NewestSegmentFirstPolicy policy = new NewestSegmentFirstPolicy(null);
    ClusterCompactionConfig config = ClusterCompactionConfig.builder()
                                                            .compactionTaskSlotRatio(0.3)
                                                            .maxCompactionTaskSlots(5)
                                                            .useSupervisors(true)
                                                            .engine(CompactionEngine.MSQ)
                                                            .compactionPolicy(policy)
                                                            .storeCompactionStatePerSegment(true)
                                                            .build();

    Assert.assertEquals(0.3, config.getCompactionTaskSlotRatio(), 0.0001);
    Assert.assertEquals(5, config.getMaxCompactionTaskSlots());
    Assert.assertTrue(config.isUseSupervisors());
    Assert.assertEquals(CompactionEngine.MSQ, config.getEngine());
    Assert.assertEquals(policy, config.getCompactionPolicy());
    Assert.assertTrue(config.isStoreCompactionStatePerSegment());
  }

  @Test
  public void testToBuilder()
  {
    ClusterCompactionConfig original = ClusterCompactionConfig.builder()
                                                              .compactionTaskSlotRatio(0.7)
                                                              .maxCompactionTaskSlots(20)
                                                              .useSupervisors(false)
                                                              .engine(CompactionEngine.NATIVE)
                                                              .storeCompactionStatePerSegment(false)
                                                              .build();

    ClusterCompactionConfig modified = original.toBuilder()
                                               .compactionTaskSlotRatio(0.8)
                                               .build();

    Assert.assertEquals(0.8, modified.getCompactionTaskSlotRatio(), 0.0001);
    Assert.assertEquals(20, modified.getMaxCompactionTaskSlots());
    Assert.assertFalse(modified.isUseSupervisors());
  }

  @Test
  public void testMsqEngineRequiresSupervisors()
  {
    DruidException e = Assert.assertThrows(
        DruidException.class,
        () -> ClusterCompactionConfig.builder().useSupervisors(false).engine(CompactionEngine.MSQ).build()
    );
    Assert.assertEquals("MSQ Compaction engine can be used only with compaction supervisors.", e.getMessage());
    Assert.assertEquals(DruidException.Category.INVALID_INPUT, e.getCategory());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    ClusterCompactionConfig config1 = ClusterCompactionConfig.builder()
                                                             .compactionTaskSlotRatio(0.5)
                                                             .maxCompactionTaskSlots(10)
                                                             .build();

    ClusterCompactionConfig config2 = ClusterCompactionConfig.builder()
                                                             .compactionTaskSlotRatio(0.5)
                                                             .maxCompactionTaskSlots(10)
                                                             .build();

    ClusterCompactionConfig config3 = ClusterCompactionConfig.builder()
                                                             .compactionTaskSlotRatio(0.6)
                                                             .maxCompactionTaskSlots(10)
                                                             .build();

    Assert.assertEquals(config1, config2);
    Assert.assertEquals(config1.hashCode(), config2.hashCode());
    Assert.assertNotEquals(config1, config3);
    Assert.assertNotEquals(config3, config2);
  }
}
