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

package org.apache.druid.server.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class PerSegmentTimeoutConfigTest
{
  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws Exception
  {
    String json = "{\"perSegmentTimeoutMs\": 5000, \"monitorOnly\": true}";
    PerSegmentTimeoutConfig config = mapper.readValue(json, PerSegmentTimeoutConfig.class);
    Assert.assertEquals(5000L, config.getPerSegmentTimeoutMs());
    Assert.assertTrue(config.isMonitorOnly());

    // Round-trip
    PerSegmentTimeoutConfig roundTripped = mapper.readValue(
        mapper.writeValueAsString(config),
        PerSegmentTimeoutConfig.class
    );
    Assert.assertEquals(config, roundTripped);
  }

  @Test
  public void testMonitorOnlyDefaultsToFalse() throws Exception
  {
    String json = "{\"perSegmentTimeoutMs\": 3000}";
    PerSegmentTimeoutConfig config = mapper.readValue(json, PerSegmentTimeoutConfig.class);
    Assert.assertEquals(3000L, config.getPerSegmentTimeoutMs());
    Assert.assertFalse(config.isMonitorOnly());
  }

  @Test
  public void testRejectsZeroTimeout()
  {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new PerSegmentTimeoutConfig(0, null)
    );
  }

  @Test
  public void testRejectsNegativeTimeout()
  {
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new PerSegmentTimeoutConfig(-1, null)
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(PerSegmentTimeoutConfig.class)
                  .usingGetClass()
                  .verify();
  }
}
