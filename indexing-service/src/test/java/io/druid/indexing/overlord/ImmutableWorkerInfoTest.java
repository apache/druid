/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.druid.indexing.worker.Worker;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

public class ImmutableWorkerInfoTest
{
  @Test
  public void testSerde() throws Exception
  {
    ImmutableWorkerInfo workerInfo = new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker", "192.0.0.1", 10, "v1"
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        new DateTime("2015-01-01T01:01:01Z")
    );
    ObjectMapper mapper = new DefaultObjectMapper();
    final ImmutableWorkerInfo serde = mapper.readValue(
        mapper.writeValueAsString(workerInfo),
        ImmutableWorkerInfo.class
    );
    Assert.assertEquals(workerInfo, serde);
  }

  @Test
  public void testEqualsAndSerde()
  {
    // Everything equal
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker", "192.0.0.1", 10, "v1"
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        new DateTime("2015-01-01T01:01:01Z")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker", "192.0.0.1", 10, "v1"
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        new DateTime("2015-01-01T01:01:01Z")
    ), true);

    // different worker same tasks
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker1", "192.0.0.1", 10, "v1"
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        new DateTime("2015-01-01T01:01:01Z")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker2", "192.0.0.1", 10, "v1"
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        new DateTime("2015-01-01T01:01:01Z")
    ), false);

    // same worker different task groups
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker", "192.0.0.1", 10, "v1"
        ),
        2,
        ImmutableSet.of("grp3", "grp2"),
        ImmutableSet.of("task1", "task2"),
        new DateTime("2015-01-01T01:01:01Z")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker", "192.0.0.1", 10, "v1"
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        new DateTime("2015-01-01T01:01:01Z")
    ), false);

    // same worker different tasks
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker1", "192.0.0.1", 10, "v1"
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        new DateTime("2015-01-01T01:01:01Z")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker2", "192.0.0.1", 10, "v1"
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task3"),
        new DateTime("2015-01-01T01:01:01Z")
    ), false);

    // same worker different capacity
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker1", "192.0.0.1", 10, "v1"
        ),
        3,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        new DateTime("2015-01-01T01:01:01Z")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker2", "192.0.0.1", 10, "v1"
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        new DateTime("2015-01-01T01:01:01Z")
    ), false);

    // same worker different lastCompletedTaskTime
    assertEqualsAndHashCode(new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker1", "192.0.0.1", 10, "v1"
        ),
        3,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        new DateTime("2015-01-01T01:01:01Z")
    ), new ImmutableWorkerInfo(
        new Worker(
            "http", "testWorker2", "192.0.0.1", 10, "v1"
        ),
        2,
        ImmutableSet.of("grp1", "grp2"),
        ImmutableSet.of("task1", "task2"),
        new DateTime("2015-01-01T01:01:02Z")
    ), false);

  }

  private void assertEqualsAndHashCode(ImmutableWorkerInfo o1, ImmutableWorkerInfo o2, boolean shouldMatch)
  {
    if (shouldMatch) {
      Assert.assertTrue(o1.equals(o2));
      Assert.assertEquals(o1.hashCode(), o2.hashCode());
    } else {
      Assert.assertFalse(o1.equals(o2));
      Assert.assertNotEquals(o1.hashCode(), o2.hashCode());
    }
  }
}
