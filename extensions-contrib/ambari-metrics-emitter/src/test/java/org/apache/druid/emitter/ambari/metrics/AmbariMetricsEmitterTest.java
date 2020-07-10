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

package org.apache.druid.emitter.ambari.metrics;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class AmbariMetricsEmitterTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(
        ObjectMapper.class,
        new DefaultObjectMapper()
    ));
  }

  @Test
  public void testSerDeAmbariMetricsEmitterConfig()
  {
    AmbariMetricsEmitterConfig config = new AmbariMetricsEmitterConfig(
        "hostname",
        8080,
        "http",
        "truststore.path",
        "truststore.type",
        "truststore.password",
        1000,
        1000L,
        100,
        new SendAllTimelineEventConverter("prefix", "druid"),
        Collections.emptyList(),
        500L,
        400L
    );
    AmbariMetricsEmitter emitter = new AmbariMetricsEmitter(config, Collections.emptyList());
    Assert.assertEquals("8080", emitter.getCollectorPort());
    Assert.assertEquals("http", emitter.getCollectorProtocol());
    Assert.assertEquals("http://myHost:8080/ws/v1/timeline/metrics", emitter.getCollectorUri("myHost"));
    Assert.assertEquals("hostname", emitter.getHostname());
    Assert.assertNull(emitter.getZookeeperQuorum());
    Assert.assertEquals(Collections.singleton("hostname"), emitter.getConfiguredCollectorHosts());
  }
}
