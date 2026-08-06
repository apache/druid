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

package org.apache.druid.server.initialization.jetty;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import javax.ws.rs.core.Response;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.WARN)
public class CustomExceptionMapperTest
{
  @Mock
  private JsonParser jsonParser;

  private CustomExceptionMapper customExceptionMapper;

  @BeforeEach
  public void setUp()
  {
    customExceptionMapper = new CustomExceptionMapper();
  }

  @Test
  public void testResponseWithSimpleMessage()
  {
    final JsonMappingException exception = JsonMappingException.from(jsonParser, "Test exception");
    final Response response = customExceptionMapper.toResponse(exception);

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assertions.assertTrue(response.getEntity() instanceof ImmutableMap);

    final ImmutableMap<Object, Object> map = (ImmutableMap<Object, Object>) response.getEntity();
    Assertions.assertEquals(1, map.size());
    Assertions.assertEquals("Test exception", map.get(CustomExceptionMapper.ERROR_KEY));
  }

  @Test
  public void testResponseWithLongMessage()
  {
    final JsonMappingException exception = JsonMappingException.from(jsonParser, "Test exception\nStack trace\nMisc details");
    final Response response = customExceptionMapper.toResponse(exception);

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assertions.assertTrue(response.getEntity() instanceof ImmutableMap);

    final ImmutableMap<Object, Object> map = (ImmutableMap<Object, Object>) response.getEntity();
    Assertions.assertEquals(1, map.size());
    Assertions.assertEquals("Test exception", map.get(CustomExceptionMapper.ERROR_KEY));
  }
}
