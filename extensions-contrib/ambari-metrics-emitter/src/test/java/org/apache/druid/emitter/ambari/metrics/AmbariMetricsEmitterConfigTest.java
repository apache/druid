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

import java.io.IOException;
import java.util.Collections;

public class AmbariMetricsEmitterConfigTest
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
  public void testSerDeAmbariMetricsEmitterConfig() throws IOException
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
        Collections.EMPTY_LIST,
        500L,
        400L
    );
    AmbariMetricsEmitterConfig serde = mapper.readerFor(AmbariMetricsEmitterConfig.class).readValue(
        mapper.writeValueAsBytes(config)
    );
    Assert.assertEquals(config, serde);
  }

  @Test
  public void testSerDeDruidToTimelineEventConverter() throws IOException
  {
    SendAllTimelineEventConverter sendAllConverter = new SendAllTimelineEventConverter("prefix", "druid");
    DruidToTimelineMetricConverter serde = mapper.readerFor(DruidToTimelineMetricConverter.class)
                                                 .readValue(mapper.writeValueAsBytes(sendAllConverter));
    Assert.assertEquals(sendAllConverter, serde);

    WhiteListBasedDruidToTimelineEventConverter whiteListBasedDruidToTimelineEventConverter = new WhiteListBasedDruidToTimelineEventConverter(
        "prefix",
        "druid",
        "",
        new DefaultObjectMapper()
    );
    serde = mapper.readerFor(DruidToTimelineMetricConverter.class)
                  .readValue(mapper.writeValueAsBytes(
                      whiteListBasedDruidToTimelineEventConverter));
    Assert.assertEquals(whiteListBasedDruidToTimelineEventConverter, serde);
  }
}
