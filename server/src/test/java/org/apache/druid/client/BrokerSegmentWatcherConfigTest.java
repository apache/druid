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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class BrokerSegmentWatcherConfigTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws Exception
  {
    //defaults
    String json = "{}";

    BrokerSegmentWatcherConfig config = MAPPER.readValue(
        MAPPER.writeValueAsString(
            MAPPER.readValue(json, BrokerSegmentWatcherConfig.class)
        ),
        BrokerSegmentWatcherConfig.class
    );

    Assert.assertNull(config.getWatchedTiers());
    Assert.assertTrue(config.isWatchRealtimeTasks());
    Assert.assertNull(config.getIgnoredTiers());

    //non-defaults
    json = "{ \"watchedTiers\": [\"t1\", \"t2\"], \"watchedDataSources\": [\"ds1\", \"ds2\"], \"watchRealtimeTasks\": false }";

    config = MAPPER.readValue(
        MAPPER.writeValueAsString(
            MAPPER.readValue(json, BrokerSegmentWatcherConfig.class)
        ),
        BrokerSegmentWatcherConfig.class
    );

    Assert.assertEquals(ImmutableSet.of("t1", "t2"), config.getWatchedTiers());
    Assert.assertNull(config.getIgnoredTiers());
    Assert.assertEquals(ImmutableSet.of("ds1", "ds2"), config.getWatchedDataSources());
    Assert.assertFalse(config.isWatchRealtimeTasks());

    // json with ignoredTiers
    json = "{ \"ignoredTiers\": [\"t3\", \"t4\"], \"watchedDataSources\": [\"ds1\", \"ds2\"] }";

    config = MAPPER.readValue(
        MAPPER.writeValueAsString(
            MAPPER.readValue(json, BrokerSegmentWatcherConfig.class)
        ),
        BrokerSegmentWatcherConfig.class
    );

    Assert.assertNull(config.getWatchedTiers());
    Assert.assertEquals(ImmutableSet.of("t3", "t4"), config.getIgnoredTiers());
    Assert.assertEquals(ImmutableSet.of("ds1", "ds2"), config.getWatchedDataSources());
    Assert.assertTrue(config.isWatchRealtimeTasks());
  }
}
