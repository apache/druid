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

package org.apache.druid.emitter.opentsdb;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OpentsdbEmitterConfigTest
{
  private ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper()));
  }

  @Test
  public void testSerDeserOpentsdbEmitterConfig() throws Exception
  {
    OpentsdbEmitterConfig opentsdbEmitterConfig = new OpentsdbEmitterConfig("localhost", 9999, 2000, 2000, 200, 2000, 10000L, null, "druid");
    String opentsdbEmitterConfigString = mapper.writeValueAsString(opentsdbEmitterConfig);
    OpentsdbEmitterConfig expectedOpentsdbEmitterConfig = mapper.readerFor(OpentsdbEmitterConfig.class)
                                                                .readValue(opentsdbEmitterConfigString);
    Assert.assertEquals(expectedOpentsdbEmitterConfig, opentsdbEmitterConfig);
  }

  @Test
  public void testSerDeserOpentsdbEmitterConfigWithNamespacePrefixContainingSpace() throws Exception
  {
    OpentsdbEmitterConfig opentsdbEmitterConfig = new OpentsdbEmitterConfig("localhost", 9999, 2000, 2000, 200, 2000, 10000L, null, "legendary druid");
    String opentsdbEmitterConfigString = mapper.writeValueAsString(opentsdbEmitterConfig);
    OpentsdbEmitterConfig expectedOpentsdbEmitterConfig = mapper.readerFor(OpentsdbEmitterConfig.class)
                                                                .readValue(opentsdbEmitterConfigString);
    Assert.assertEquals(expectedOpentsdbEmitterConfig, opentsdbEmitterConfig);
  }

  @Test
  public void testSerDeserOpentsdbEmitterConfigWithNullNamespacePrefix() throws Exception
  {
    OpentsdbEmitterConfig opentsdbEmitterConfig = new OpentsdbEmitterConfig("localhost", 9999, 2000, 2000, 200, 2000, 10000L, null, null);
    String opentsdbEmitterConfigString = mapper.writeValueAsString(opentsdbEmitterConfig);
    OpentsdbEmitterConfig expectedOpentsdbEmitterConfig = mapper.readerFor(OpentsdbEmitterConfig.class)
        .readValue(opentsdbEmitterConfigString);
    Assert.assertEquals(expectedOpentsdbEmitterConfig, opentsdbEmitterConfig);
  }

  @Test
  public void testSerDeserOpentsdbEmitterConfigWithEmptyNamespacePrefix() throws Exception
  {
    OpentsdbEmitterConfig opentsdbEmitterConfig = new OpentsdbEmitterConfig("localhost", 9999, 2000, 2000, 200, 2000, 10000L, null, "");
    String opentsdbEmitterConfigString = mapper.writeValueAsString(opentsdbEmitterConfig);
    OpentsdbEmitterConfig expectedOpentsdbEmitterConfig = mapper.readerFor(OpentsdbEmitterConfig.class)
        .readValue(opentsdbEmitterConfigString);
    Assert.assertEquals(expectedOpentsdbEmitterConfig, opentsdbEmitterConfig);
  }

  @Test
  public void testJacksonModules()
  {
    Assert.assertTrue(new OpentsdbEmitterModule().getJacksonModules().isEmpty());
  }
}
