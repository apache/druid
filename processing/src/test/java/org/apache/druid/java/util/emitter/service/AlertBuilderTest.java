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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class AlertBuilderTest
{
  @Test
  public void testAlertBuilder()
  {
    final AlertEvent alertEvent =
        AlertBuilder.create("alert[%s]", "oops")
                    .addData(ImmutableMap.of("foo", "bar"))
                    .addData(ImmutableMap.of("baz", "qux"))
                    .addThrowable(new RuntimeException("an exception!"))
                    .build("druid/test", "example.com");

    final EventMap alertMap = alertEvent.toMap();

    Assert.assertEquals("alerts", alertMap.get("feed"));
    Assert.assertEquals("alert[oops]", alertMap.get("description"));
    Assert.assertEquals("druid/test", alertMap.get("service"));
    Assert.assertEquals("example.com", alertMap.get("host"));

    final Map<String, Object> dataMap = (Map<String, Object>) alertMap.get("data");
    Assert.assertEquals("java.lang.RuntimeException", dataMap.get("exceptionType"));
    Assert.assertEquals("an exception!", dataMap.get("exceptionMessage"));
    Assert.assertEquals("bar", dataMap.get("foo"));
    Assert.assertEquals("qux", dataMap.get("baz"));
  }
}
