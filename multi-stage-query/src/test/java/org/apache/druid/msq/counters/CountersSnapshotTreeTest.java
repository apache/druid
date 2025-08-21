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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.segment.TestHelper;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;

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

  @Test
  public void testSerdeUnknownCounter() throws Exception
  {
    final ObjectMapper serializationMapper =
        TestHelper.makeJsonMapper().registerModules(new MSQIndexingModule().getJacksonModules());
    serializationMapper.registerSubtypes(TestCounterSnapshot.class);

    final ObjectMapper deserializationMapper =
        TestHelper.makeJsonMapper().registerModules(new MSQIndexingModule().getJacksonModules());

    final TestCounter testCounter = new TestCounter(10);
    final CounterSnapshotsTree snapshotsTree = new CounterSnapshotsTree();
    snapshotsTree.put(1, 2, new CounterSnapshots(ImmutableMap.of("ctr", testCounter.snapshot())));

    final String json = serializationMapper.writeValueAsString(snapshotsTree);
    final CounterSnapshotsTree snapshotsTree2 = serializationMapper.readValue(json, CounterSnapshotsTree.class);
    final CounterSnapshotsTree snapshotsTree3 = deserializationMapper.readValue(json, CounterSnapshotsTree.class);

    Assert.assertEquals(snapshotsTree.copyMap(), snapshotsTree2.copyMap());
    Assert.assertNotEquals(snapshotsTree.copyMap(), snapshotsTree3.copyMap());

    // Confirm that deserializationMapper reads the TestCounterSnapshot as a NilQueryCounterSnapshot.
    MatcherAssert.assertThat(
        snapshotsTree3.copyMap().get(1).get(2).getMap().get("ctr"),
        CoreMatchers.instanceOf(NilQueryCounterSnapshot.class)
    );
  }

  private static class TestCounter implements QueryCounter
  {
    private final int n;

    public TestCounter(int n)
    {
      this.n = n;
    }

    @Override
    public QueryCounterSnapshot snapshot()
    {
      return new TestCounterSnapshot(n);
    }
  }

  @JsonTypeName("test")
  private static class TestCounterSnapshot implements QueryCounterSnapshot
  {
    private final int n;

    @JsonCreator
    public TestCounterSnapshot(@JsonProperty("n") int n)
    {
      this.n = n;
    }

    @JsonProperty("n")
    public int getN()
    {
      return n;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestCounterSnapshot that = (TestCounterSnapshot) o;
      return n == that.n;
    }

    @Override
    public int hashCode()
    {
      return Objects.hashCode(n);
    }

    @Override
    public String toString()
    {
      return "TestCounterSnapshot{" +
             "n=" + n +
             '}';
    }
  }
}
