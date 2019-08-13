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

package org.apache.druid.tests.indexer;

import org.apache.druid.curator.discovery.ServerDiscoverySelector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.clients.EventReceiverFirehoseTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ServerDiscoveryUtil;
import org.apache.druid.tests.TestNGGroup;
import org.joda.time.DateTime;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * See {@link AbstractITRealtimeIndexTaskTest} for test details.
 */
@Test(groups = TestNGGroup.REALTIME_INDEX)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITAppenderatorDriverRealtimeIndexTaskTest extends AbstractITRealtimeIndexTaskTest
{
  private static final Logger LOG = new Logger(ITAppenderatorDriverRealtimeIndexTaskTest.class);
  private static final String REALTIME_TASK_RESOURCE = "/indexer/wikipedia_realtime_appenderator_index_task.json";
  private static final String REALTIME_QUERIES_RESOURCE = "/indexer/wikipedia_realtime_appenderator_index_queries.json";

  @Test
  public void testRealtimeIndexTask()
  {
    doTest();
  }

  @Override
  void postEvents() throws Exception
  {
    final ServerDiscoverySelector eventReceiverSelector = factory.createSelector(EVENT_RECEIVER_SERVICE_NAME);
    eventReceiverSelector.start();
    InputStreamReader isr;
    try {
      isr = new InputStreamReader(
          ITRealtimeIndexTaskTest.class.getResourceAsStream(EVENT_DATA_FILE),
          StandardCharsets.UTF_8
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    try (BufferedReader reader = new BufferedReader(isr)) {
      ServerDiscoveryUtil.waitUntilInstanceReady(eventReceiverSelector, "Event Receiver");
      // Use the host from the config file and the port announced in zookeeper
      String host = config.getMiddleManagerHost() + ":" + eventReceiverSelector.pick().getPort();
      LOG.info("Event Receiver Found at host [%s]", host);
      EventReceiverFirehoseTestClient client = new EventReceiverFirehoseTestClient(
          host,
          EVENT_RECEIVER_SERVICE_NAME,
          jsonMapper,
          httpClient,
          smileMapper
      );
      // there are 22 lines in the file
      int i = 1;
      DateTime dt = DateTimes.nowUtc();  // timestamp used for sending each event
      dtFirst = dt;
      dtLast = dt;
      String line;
      while ((line = reader.readLine()) != null) {
        if (i == 15) { // for the 15th line, use a time before the window
          dt = dt.minusMinutes(10);
          dtFirst = dt; // oldest timestamp
        } else if (i == 16) { // remember this time to use in the expected response from the groupBy query
          dtGroupBy = dt;
        } else if (i == 18) { // use a time 6 seconds ago so it will be out of order
          dt = dt.minusSeconds(6);
        }
        String event = StringUtils.replace(line, TIME_PLACEHOLDER, EVENT_FMT.print(dt));
        LOG.info("sending event: [%s]\n", event);
        Collection<Map<String, Object>> events = new ArrayList<>();
        events.add(this.jsonMapper.readValue(event, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT));
        int eventsPosted = client.postEvents(events, this.jsonMapper, MediaType.APPLICATION_JSON);
        if (eventsPosted != events.size()) {
          throw new ISE("Event not posted");
        }

        try {
          Thread.sleep(DELAY_BETWEEN_EVENTS_SECS * 1000);
        }
        catch (InterruptedException ignored) {
          /* nothing */
        }
        dtLast = dt; // latest timestamp
        dt = DateTimes.nowUtc();
        i++;
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    finally {
      eventReceiverSelector.stop();
    }
  }

  @Override
  String getTaskResource()
  {
    return REALTIME_TASK_RESOURCE;
  }

  @Override
  String getQueriesResource()
  {
    return REALTIME_QUERIES_RESOURCE;
  }
}
