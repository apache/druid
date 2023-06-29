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

package org.apache.druid.msq.counters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class CountersSnapshotTreeTest
{
  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper =
        TestHelper.makeJsonMapper().registerModules(new MSQIndexingModule().getJacksonModules());

    final ChannelCounters channelCounters = new ChannelCounters();
    channelCounters.addFile(10, 13);
    channelCounters.setTotalFiles(14);

    final CounterSnapshotsTree snapshotsTree = new CounterSnapshotsTree();
    snapshotsTree.put(1, 2, new CounterSnapshots(ImmutableMap.of("ctr", channelCounters.snapshot())));

    final String json = mapper.writeValueAsString(snapshotsTree);
    final CounterSnapshotsTree snapshotsTree2 = mapper.readValue(json, CounterSnapshotsTree.class);

    Assert.assertEquals(snapshotsTree.copyMap(), snapshotsTree2.copyMap());
  }
}
