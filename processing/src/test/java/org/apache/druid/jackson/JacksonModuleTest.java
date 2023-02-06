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

package org.apache.druid.jackson;

import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.databind.util.LinkedNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class JacksonModuleTest
{
  @Test
  public void testNullServiceNameInProperties()
  {
    JacksonModule module = new JacksonModule();
    verifyUnknownServiceName((DefaultObjectMapper) module.jsonMapper(new Properties()));
    verifyUnknownServiceName((DefaultObjectMapper) module.smileMapper(new Properties()));
    verifyUnknownServiceName((DefaultObjectMapper) module.jsonMapperOnlyNonNullValue(new Properties()));
  }

  @Test
  public void testNullProperties()
  {
    JacksonModule module = new JacksonModule();
    verifyUnknownServiceName((DefaultObjectMapper) module.jsonMapper(null));
    verifyUnknownServiceName((DefaultObjectMapper) module.smileMapper(null));
    verifyUnknownServiceName((DefaultObjectMapper) module.jsonMapperOnlyNonNullValue(null));
  }

  @Test
  public void testServiceNameInProperties()
  {
    JacksonModule module = new JacksonModule();
    Properties properties = new Properties();
    properties.setProperty("druid.service", "myService_1");
    verifyServiceName((DefaultObjectMapper) module.jsonMapper(properties), "myService_1");
    properties.setProperty("druid.service", "myService_2");
    verifyServiceName((DefaultObjectMapper) module.smileMapper(properties), "myService_2");
    properties.setProperty("druid.service", "myService_3");
    verifyServiceName((DefaultObjectMapper) module.jsonMapperOnlyNonNullValue(properties), "myService_3");
  }

  private void verifyUnknownServiceName(DefaultObjectMapper objectMapper)
  {
    verifyServiceName(objectMapper, null);
  }

  private void verifyServiceName(DefaultObjectMapper objectMapper, String expectedServiceName)
  {
    LinkedNode<DeserializationProblemHandler> handlers = objectMapper.getDeserializationConfig().getProblemHandlers();
    Assert.assertNull(handlers.next());
    DefaultObjectMapper.DefaultDeserializationProblemHandler handler =
        (DefaultObjectMapper.DefaultDeserializationProblemHandler) handlers.value();
    Assert.assertEquals(expectedServiceName, handler.getServiceName());
  }
}
