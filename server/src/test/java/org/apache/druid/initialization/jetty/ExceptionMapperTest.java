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

package org.apache.druid.initialization.jetty;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.druid.server.initialization.jetty.CustomExceptionMapper;
import org.apache.druid.server.initialization.jetty.ResponseStatusException;
import org.apache.druid.server.initialization.jetty.ResponseStatusExceptionMapper;
import org.apache.druid.test.utils.ResponseTestUtils;
import org.junit.Test;

import javax.ws.rs.core.Response;

public class ExceptionMapperTest
{
  @Test
  public void testCustomExceptionMapperWithNullMessage()
  {
    ResponseTestUtils.assertErrorResponse(
        new CustomExceptionMapper().toResponse(new JsonMappingException(null)),
        Response.Status.BAD_REQUEST,
        "unknown json mapping exception"
    );
  }

  @Test
  public void testCustomExceptionMapperWithMessage()
  {
    ResponseTestUtils.assertErrorResponse(
        new CustomExceptionMapper().toResponse(new JsonMappingException("this exception message")),
        Response.Status.BAD_REQUEST,
        "this exception message"
    );
  }

  @Test
  public void testResponseStatusExceptionMapper()
  {
    ResponseTestUtils.assertErrorResponse(
        new ResponseStatusExceptionMapper().toResponse(new ResponseStatusException(Response.Status.BAD_REQUEST, "this error message")),
        Response.Status.BAD_REQUEST,
        "this error message"
    );
  }
}
