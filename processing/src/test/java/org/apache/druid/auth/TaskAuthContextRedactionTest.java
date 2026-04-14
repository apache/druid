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

package org.apache.druid.auth;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Map;

public class TaskAuthContextRedactionTest
{
  private ObjectMapper mapper;
  private ObjectMapper redactingMapper;

  @Before
  public void setUp()
  {
    mapper = new ObjectMapper();
    mapper.registerSubtypes(new NamedType(TestTaskAuthContext.class, "test"));

    redactingMapper = mapper.copy()
        .addMixIn(TaskAuthContext.class, TaskAuthContextRedactionMixIn.class);
  }

  @Test
  public void testSerializationIncludesCredentials() throws Exception
  {
    TaskAuthContext ctx = new TestTaskAuthContext(
        "user@example.com",
        Map.of("token", "secret-oauth-token"),
        Map.of("expiry", "2026-12-31")
    );
    String json = mapper.writeValueAsString(ctx);
    Assert.assertTrue(json.contains("secret-oauth-token"));
    Assert.assertTrue(json.contains("user@example.com"));
    Assert.assertTrue(json.contains("2026-12-31"));
  }

  @Test
  public void testRedactingMapperRemovesCredentials() throws Exception
  {
    TaskAuthContext ctx = new TestTaskAuthContext(
        "user@example.com",
        Map.of("token", "secret-oauth-token"),
        Map.of("expiry", "2026-12-31")
    );
    String json = redactingMapper.writeValueAsString(ctx);
    Assert.assertFalse(json.contains("secret-oauth-token"));
    Assert.assertFalse(json.contains("\"credentials\""));
    Assert.assertTrue(json.contains("user@example.com"));
    Assert.assertTrue(json.contains("2026-12-31"));
  }

  @Test
  public void testRedactionWithNullCredentials() throws Exception
  {
    TaskAuthContext ctx = new TestTaskAuthContext("user@example.com", null, null);
    String json = redactingMapper.writeValueAsString(ctx);
    Assert.assertFalse(json.contains("\"credentials\""));
    Assert.assertTrue(json.contains("user@example.com"));
  }

  @JsonTypeName("test")
  public static class TestTaskAuthContext implements TaskAuthContext
  {
    private final String identity;
    private final Map<String, String> credentials;
    private final Map<String, Object> metadata;

    public TestTaskAuthContext(
        @JsonProperty("identity") String identity,
        @JsonProperty("credentials") @Nullable Map<String, String> credentials,
        @JsonProperty("metadata") @Nullable Map<String, Object> metadata
    )
    {
      this.identity = identity;
      this.credentials = credentials;
      this.metadata = metadata;
    }

    @Override
    @JsonProperty
    public String getIdentity()
    {
      return identity;
    }

    @Override
    @JsonProperty
    @Nullable
    public Map<String, String> getCredentials()
    {
      return credentials;
    }

    @Override
    @JsonProperty
    @Nullable
    public Map<String, Object> getMetadata()
    {
      return metadata;
    }
  }
}
