/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.emitter.kafka;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class KafkaEmitterConfigTest
{
  private ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper()));
  }

  @Test
  public void testSerDeserKafkaEmitterConfig() throws IOException
  {
    KafkaEmitterConfig kafkaEmitterConfig = new KafkaEmitterConfig("hostname", "metricTest",
                                                                   "alertTest", "clusterNameTest",
                                                                   ImmutableMap.<String, String>builder()
                                                                       .put("testKey", "testValue").build()
    );
    String kafkaEmitterConfigString = mapper.writeValueAsString(kafkaEmitterConfig);
    KafkaEmitterConfig kafkaEmitterConfigExpected = mapper.reader(KafkaEmitterConfig.class)
                                                          .readValue(kafkaEmitterConfigString);
    Assert.assertEquals(kafkaEmitterConfigExpected, kafkaEmitterConfig);
  }
}
