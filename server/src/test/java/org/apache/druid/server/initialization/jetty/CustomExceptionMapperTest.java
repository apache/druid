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
import org.apache.druid.server.initialization.ServerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.ws.rs.core.Response;

@RunWith(MockitoJUnitRunner.class)
public class CustomExceptionMapperTest
{
  @Mock
  private ServerConfig serverConfig;

  @Mock
  private JsonParser jsonParser;

  private CustomExceptionMapper customExceptionMapper;

  @Before
  public void setUp()
  {
    customExceptionMapper = new CustomExceptionMapper(serverConfig);
  }

  @Test
  public void testResponseWithoutDetail()
  {
    Mockito.when(serverConfig.isShowDetailedJsonMappingError()).thenReturn(false);

    final JsonMappingException exception = JsonMappingException.from(jsonParser, "Test exception");
    final Response response = customExceptionMapper.toResponse(exception);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertTrue(response.getEntity() instanceof ImmutableMap);

    final ImmutableMap<Object, Object> map = (ImmutableMap) response.getEntity();
    Assert.assertEquals(1, map.size());
    Assert.assertEquals(CustomExceptionMapper.UNABLE_TO_PROCESS_ERROR, map.get(CustomExceptionMapper.ERROR_KEY));
    Assert.assertNull(map.get(CustomExceptionMapper.ERR_MSG_KEY));
  }

  @Test
  public void testResponseWithDetail()
  {
    Mockito.when(serverConfig.isShowDetailedJsonMappingError()).thenReturn(true);

    final JsonMappingException exception = JsonMappingException.from(jsonParser, "Test exception");
    final Response response = customExceptionMapper.toResponse(exception);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertTrue(response.getEntity() instanceof ImmutableMap);

    final ImmutableMap<Object, Object> map = (ImmutableMap) response.getEntity();
    Assert.assertEquals(2, map.size());
    Assert.assertEquals(CustomExceptionMapper.UNABLE_TO_PROCESS_ERROR, map.get(CustomExceptionMapper.ERROR_KEY));
    Assert.assertEquals("Test exception", map.get(CustomExceptionMapper.ERR_MSG_KEY));
  }
}
