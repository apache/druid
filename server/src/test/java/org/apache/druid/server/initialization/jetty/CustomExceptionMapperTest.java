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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.ws.rs.core.Response;

@RunWith(MockitoJUnitRunner.class)
public class CustomExceptionMapperTest
{
  @Mock
  private JsonParser jsonParser;

  private CustomExceptionMapper customExceptionMapper;

  @Before
  public void setUp()
  {
    customExceptionMapper = new CustomExceptionMapper();
  }

  @Test
  public void testResponseWithSimpleMessage()
  {
    final JsonMappingException exception = JsonMappingException.from(jsonParser, "Test exception");
    final Response response = customExceptionMapper.toResponse(exception);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertTrue(response.getEntity() instanceof ImmutableMap);

    final ImmutableMap<Object, Object> map = (ImmutableMap<Object, Object>) response.getEntity();
    Assert.assertEquals(1, map.size());
    Assert.assertEquals("Test exception", map.get(CustomExceptionMapper.ERROR_KEY));
  }

  @Test
  public void testResponseWithLongMessage()
  {
    final JsonMappingException exception = JsonMappingException.from(jsonParser, "Test exception\nStack trace\nMisc details");
    final Response response = customExceptionMapper.toResponse(exception);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertTrue(response.getEntity() instanceof ImmutableMap);

    final ImmutableMap<Object, Object> map = (ImmutableMap<Object, Object>) response.getEntity();
    Assert.assertEquals(1, map.size());
    Assert.assertEquals("Test exception", map.get(CustomExceptionMapper.ERROR_KEY));
  }
}
