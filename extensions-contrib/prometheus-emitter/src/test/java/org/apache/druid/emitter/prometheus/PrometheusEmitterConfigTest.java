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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PrometheusEmitterConfigTest
{

  private ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper()));
  }

  @Test
  public void testSerDeserPrometheusEmitterConfig() throws Exception
  {
    PrometheusEmitterConfig prometheusEmitterConfig = new PrometheusEmitterConfig("10.100.201.218", 9091, null, "druid", null, null);
    String prometheusEmitterConfigString = mapper.writeValueAsString(prometheusEmitterConfig);
    PrometheusEmitterConfig expectedPrometheusEmitterConfig = mapper.readerFor(PrometheusEmitterConfig.class)
        .readValue(prometheusEmitterConfigString);

    Assert.assertEquals(expectedPrometheusEmitterConfig, prometheusEmitterConfig);
  }

}
