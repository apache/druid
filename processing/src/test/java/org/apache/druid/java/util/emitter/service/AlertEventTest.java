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

package org.apache.druid.java.util.emitter.service;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.emitter.service.AlertEvent.Severity;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 */
public class AlertEventTest
{
  @Test
  public void testStupid()
  {
    AlertEvent event = AlertBuilder.create("blargy")
                                   .addData("something1", "a")
                                   .addData("something2", "b")
                                   .build("test", "localhost");

    Assert.assertEquals(
        ImmutableMap.<String, Object>builder()
            .put("feed", "alerts")
            .put("timestamp", event.getCreatedTime().toString())
            .put("service", "test")
            .put("host", "localhost")
            .put("severity", "component-failure")
            .put("description", "blargy")
            .put("data", ImmutableMap.<String, Object>of("something1", "a", "something2", "b"))
            .build(),
        event.toMap()
    );
  }

  @Test
  public void testAnomaly()
  {
    AlertEvent event = AlertBuilder.create("blargy")
                                   .severity(Severity.ANOMALY)
                                   .addData("something1", "a")
                                   .addData("something2", "b")
                                   .build("test", "localhost");

    Assert.assertEquals(
        ImmutableMap.<String, Object>builder()
            .put("feed", "alerts")
            .put("timestamp", event.getCreatedTime().toString())
            .put("service", "test")
            .put("host", "localhost")
            .put("severity", "anomaly")
            .put("description", "blargy")
            .put("data", ImmutableMap.<String, Object>of("something1", "a", "something2", "b"))
            .build(),
        event.toMap()
    );
  }

  @Test
  public void testComponentFailure()
  {
    AlertEvent event = AlertBuilder.create("blargy")
                                   .severity(Severity.COMPONENT_FAILURE)
                                   .addData("something1", "a")
                                   .addData("something2", "b")
                                   .build("test", "localhost");

    Assert.assertEquals(
        ImmutableMap.<String, Object>builder()
            .put("feed", "alerts")
            .put("timestamp", event.getCreatedTime().toString())
            .put("service", "test")
            .put("host", "localhost")
            .put("severity", "component-failure")
            .put("description", "blargy")
            .put("data", ImmutableMap.<String, Object>of("something1", "a", "something2", "b"))
            .build(),
        event.toMap()
    );
  }

  @Test
  public void testServiceFailure()
  {
    AlertEvent event = AlertBuilder.create("blargy")
                                   .severity(Severity.SERVICE_FAILURE)
                                   .addData("something1", "a")
                                   .addData("something2", "b")
                                   .build("test", "localhost");

    Assert.assertEquals(
        ImmutableMap.<String, Object>builder()
            .put("feed", "alerts")
            .put("timestamp", event.getCreatedTime().toString())
            .put("service", "test")
            .put("host", "localhost")
            .put("severity", "service-failure")
            .put("description", "blargy")
            .put("data", ImmutableMap.<String, Object>of("something1", "a", "something2", "b"))
            .build(),
        event.toMap()
    );
  }

  @Test
  public void testDefaulting()
  {
    final String service = "some service";
    final String host = "some host";
    final String desc = "some description";
    final Map<String, Object> data = ImmutableMap.<String, Object>builder().put("a", "1").put("b", "2").build();
    for (Severity severity : new Severity[]{Severity.ANOMALY, Severity.COMPONENT_FAILURE, Severity.SERVICE_FAILURE}) {
      Assert.assertEquals(
          contents(new AlertEvent(service, host, desc, data)),
          contents(new AlertEvent(service, host, Severity.COMPONENT_FAILURE, desc, data))
      );

      Assert.assertEquals(
          contents(new AlertEvent(service, host, desc)),
          contents(new AlertEvent(service, host, Severity.COMPONENT_FAILURE, desc, ImmutableMap.of()))
      );

      Assert.assertEquals(
          contents(AlertBuilder.create(desc).addData("a", "1").addData("b", "2").build(service, host)),
          contents(new AlertEvent(service, host, Severity.COMPONENT_FAILURE, desc, data))
      );

      Assert.assertEquals(
          contents(AlertBuilder.create(desc).addData(data).build(service, host)),
          contents(new AlertEvent(service, host, Severity.COMPONENT_FAILURE, desc, data))
      );

      Assert.assertEquals(
          contents(AlertBuilder.create(desc)
                               .severity(severity)
                               .addData("a", "1")
                               .addData("b", "2")
                               .build(service, host)),
          contents(new AlertEvent(service, host, severity, desc, data))
      );

      Assert.assertEquals(
          contents(AlertBuilder.create(desc).severity(severity).addData(data).build(service, host)),
          contents(new AlertEvent(service, host, severity, desc, data))
      );
    }
  }

  public Map<String, Object> contents(AlertEvent a)
  {
    return Maps.filterKeys(a.toMap(), new Predicate<String>()
    {
      @Override
      public boolean apply(String k)
      {
        return !"timestamp".equals(k);
      }
    });
  }
}
