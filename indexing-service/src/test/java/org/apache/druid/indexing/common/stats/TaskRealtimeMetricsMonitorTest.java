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

package org.apache.druid.indexing.common.stats;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.incremental.NoopRowIngestionMeters;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TaskRealtimeMetricsMonitorTest
{
  private ServiceEmitter emitter;
  private FireDepartment fireDepartment;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    emitter = EasyMock.mock(ServiceEmitter.class);
    fireDepartment = EasyMock.mock(FireDepartment.class);
  }

  @Test
  public void testLastRoundMetricsEmission()
  {
    FireDepartmentMetrics metrics = new FireDepartmentMetrics();
    RowIngestionMeters rowIngestionMeters = new NoopRowIngestionMeters();
    DataSchema schema = new DataSchema("dataSource", null, null, null, null, null, null, null);
    EasyMock.expect(fireDepartment.getMetrics()).andReturn(metrics);
    EasyMock.expectLastCall().times(2);
    EasyMock.expect(fireDepartment.getDataSchema()).andReturn(schema);
    EasyMock.expectLastCall().times(2);
    EasyMock.replay(fireDepartment);

    TaskRealtimeMetricsMonitor monitor = new TaskRealtimeMetricsMonitor(fireDepartment, rowIngestionMeters, ImmutableMap.of());

    Assert.assertFalse(monitor.isStarted());
    boolean zerothRound = monitor.monitor(emitter);
    monitor.start();
    Assert.assertTrue(monitor.isStarted());
    boolean firstRound = monitor.monitor(emitter);
    monitor.stop();
    Assert.assertFalse(monitor.isStarted());
    boolean secondRound = monitor.monitor(emitter);
    boolean thirdRound = monitor.monitor(emitter);

    Assert.assertFalse(zerothRound);
    Assert.assertTrue(firstRound && secondRound);
    Assert.assertFalse(thirdRound);

    EasyMock.verify(fireDepartment);
  }

}

