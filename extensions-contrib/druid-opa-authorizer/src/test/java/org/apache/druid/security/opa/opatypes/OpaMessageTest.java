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

package org.apache.druid.security.opa.opatypes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.server.security.AuthenticationResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class OpaMessageTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  @SuppressWarnings("unchecked")
  public void testSerialization() throws Exception
  {
    final AuthenticationResult authResult = new AuthenticationResult("user", "authorizer", "authenticator", null);
    final OpaMessage message = new OpaMessage(authResult, "READ", "resource", "type");

    final String json = mapper.writeValueAsString(message);

    // Verify the structure: {"input":{"authenticationResult":{"identity":"user",...},"action":"READ","resource":{"name":"resource","type":"type"}}}
    final Map<String, Object> map = mapper.readValue(json, new TypeReference<>() {});
    Assert.assertTrue(map.containsKey("input"));

    final Map<String, Object> input = (Map<String, Object>) map.get("input");
    Assert.assertEquals("READ", input.get("action"));

    final Map<String, Object> resource = (Map<String, Object>) input.get("resource");
    Assert.assertEquals("resource", resource.get("name"));
    Assert.assertEquals("type", resource.get("type"));

    final Map<String, Object> authenticationResult = (Map<String, Object>) input.get("authenticationResult");
    Assert.assertEquals("user", authenticationResult.get("identity"));
  }
}
