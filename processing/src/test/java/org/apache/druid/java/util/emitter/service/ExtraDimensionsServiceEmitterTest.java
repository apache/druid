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

import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ExtraDimensionsServiceEmitterTest
{
  @Test
  public void test_emit_addsDimensions()
  {
    final StubServiceEmitter delegate = new StubServiceEmitter("svc", "host");
    delegate.start();
    final ServiceEmitter emitter = new ExtraDimensionsServiceEmitter(delegate, Map.of("extra", "value", "dim", "y"));

    emitter.emit(ServiceMetricEvent.builder().setDimension("dim", "x").setMetric("metric", 1));

    // The extra dimension is added, but the metric's own value of "dim" is kept.
    final ServiceMetricEvent event = delegate.getMetricEvents("metric").get(0);
    Assertions.assertEquals(Map.of("dim", "x", "extra", "value"), event.getUserDims());
    Assertions.assertEquals("svc", event.getService());
    Assertions.assertEquals("host", event.getHost());
  }
}
