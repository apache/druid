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

package org.apache.druid.java.util.metrics;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import oshi.util.Util;

import java.util.List;

public class SysMonitorOshiTest
{
  @Test
  public void testDoMonitor()
  {

    ServiceEmitter serviceEmitter = Mockito.mock(ServiceEmitter.class);
    SysMonitorOshi sysMonitorOshi = new SysMonitorOshi();
    serviceEmitter.start();
    sysMonitorOshi.monitor(serviceEmitter);

    Assert.assertTrue(sysMonitorOshi.doMonitor(serviceEmitter));

  }
  @Test
  public void testDefaultFeedSysMonitorOshi()
  {
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
    SysMonitorOshi m = new SysMonitorOshi();
    m.start();
    m.monitor(emitter);
    // Sleep for 2 sec to get all metrics which are difference of prev and now metrics
    Util.sleep(2000);
    m.monitor(emitter);
    m.stop();
    checkEvents(emitter.getEvents(), "metrics");
  }

  private void checkEvents(List<Event> events, String expectedFeed)
  {
    Assert.assertFalse("no events emitted", events.isEmpty());
    for (Event e : events) {
      if (!expectedFeed.equals(e.getFeed())) {
        String message = StringUtils.format("\"feed\" in event: %s", e.toMap().toString());
        Assert.assertEquals(message, expectedFeed, e.getFeed());
      }
    }
  }


}
