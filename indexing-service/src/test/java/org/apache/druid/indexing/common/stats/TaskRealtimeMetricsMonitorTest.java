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

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.RealtimeMetricsMonitor;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TaskRealtimeMetricsMonitorTest
{
  private ServiceEmitter emitter;
  private FireDepartment fireDepartment;

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
    DataSchema schema = new DataSchema("dataSource", null, null, null, null, null, null, null);
    // Expect fireDepartment calls twice. Once after start and once when calling monitorAndStop
    EasyMock.expect(fireDepartment.getMetrics()).andReturn(metrics);
    EasyMock.expectLastCall().times(2);
    EasyMock.expect(fireDepartment.getDataSchema()).andReturn(schema);
    EasyMock.expectLastCall().times(2);
    EasyMock.replay(fireDepartment);

    RealtimeMetricsMonitor monitor = new RealtimeMetricsMonitor(ImmutableList.of(fireDepartment));

    // No emission since the monitor has not begun
    Assert.assertFalse(monitor.monitor(emitter));

    monitor.start();
    // Monitor has started and a call to fireDepartment is made
    Assert.assertTrue(monitor.isStarted());
    Assert.assertTrue(monitor.monitor(emitter));

    // The next call triggers a fireDepartment call but the subsequent call doesn't emit metrics
    monitor.monitorAndStop(emitter);
    Assert.assertFalse(monitor.monitor(emitter));

    EasyMock.verify(fireDepartment);
  }

}

