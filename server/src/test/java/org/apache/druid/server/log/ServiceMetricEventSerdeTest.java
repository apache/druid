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

package org.apache.druid.server.log;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.Assert;
import org.junit.Test;

public class ServiceMetricEventSerdeTest
{

  @Test
  public void testSerializeServiceMetricEventMap() throws JsonProcessingException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    String timestamp = "2022-08-17T18:51:00.000Z";
    Event event = ServiceMetricEvent.builder()
                                    .setFeed("my-feed")
                                    .build(DateTimes.of(timestamp), "m1", 1)
                                    .build("my-service", "my-host");

    String actual = mapper.writeValueAsString(event.toMap());
    String expected = "{"
                      + "\"feed\":\"my-feed\","
                      + "\"timestamp\":\""
                      + timestamp
                      + "\","
                      + "\"metric\":\"m1\","
                      + "\"value\":1,"
                      + "\"service\":\"my-service\","
                      + "\"host\":\"my-host\""
                      + "}";
    Assert.assertEquals(mapper.readTree(expected), mapper.readTree(actual));
  }

}
