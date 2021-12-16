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

package org.apache.druid.testing.clients;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;

/**
 * This class holds those responses whose HTTP status is > 200
 * <p>
 * This class inherits from {@link ISE} so that it's compatible with old test code which catches the ISE class
 */
public class ResponseException extends ISE
{
  private final ObjectMapper jsonMapper;
  private final StatusResponseHolder response;

  public ResponseException(ObjectMapper jsonMapper,
                           StatusResponseHolder response,
                           String formatText,
                           Object... arguments)
  {
    super(formatText, arguments);
    this.jsonMapper = jsonMapper;
    this.response = response;
  }

  public StatusResponseHolder getResponse()
  {
    return response;
  }

  /**
   * Deserialize the response body content to an object.
   *
   * This requires the Content-Type header of response to be set as 'application/json'.
   */
  public <T> T bodyToObject(Class<T> clazz)
  {
    String type = response.getHeaders().get("Content-Type");
    if (!"application/json".equals(type)) {
      throw new ISE("Content-Type is [{%s], expected [application/json]", type);
    }

    try {
      return jsonMapper.readValue(response.getContent(), clazz);
    }
    catch (JsonProcessingException e) {
      throw new ISE(e, "unable to deserialize content to object");
    }
  }

  public static class ErrorContent
  {
    private final String error;

    @JsonCreator
    public ErrorContent(@JsonProperty("error") String error)
    {
      this.error = error;
    }

    public String getError()
    {
      return error;
    }
  }
}
