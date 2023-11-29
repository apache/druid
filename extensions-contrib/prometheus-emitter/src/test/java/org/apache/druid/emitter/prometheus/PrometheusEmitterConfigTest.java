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

package org.apache.druid.emitter.prometheus;

import io.prometheus.client.CollectorRegistry;
import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PrometheusEmitterConfigTest
{
  @Test
  public void testEmitterConfigWithBadExtraLabels()
  {
    CollectorRegistry.defaultRegistry.clear();

    Map<String, String> extraLabels = new HashMap<>();
    extraLabels.put("label Name", "label Value");

    // Expect an exception thrown by our own PrometheusEmitterConfig due to invalid label key
    Exception exception = Assert.assertThrows(DruidException.class, () -> {
      new PrometheusEmitterConfig(PrometheusEmitterConfig.Strategy.exporter, null, null, 0, null, false, true, 60, extraLabels, false, null);
    });

    String expectedMessage = "Invalid metric label name [label Name]. Label names must conform to the pattern [[a-zA-Z_:][a-zA-Z0-9_:]*]";
    String actualMessage = exception.getMessage();

    Assert.assertTrue(actualMessage.contains(expectedMessage));
  }

}
