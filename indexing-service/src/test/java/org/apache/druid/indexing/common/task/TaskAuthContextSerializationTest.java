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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.druid.auth.TaskAuthContext;
import org.apache.druid.auth.TaskAuthContextRedactionMixIn;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.metadata.PasswordProviderRedactionMixIn;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Map;

public class TaskAuthContextSerializationTest
{
  private ObjectMapper mapper;
  private ObjectMapper redactingMapper;

  @Before
  public void setUp()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(
        new NamedType(NoopTask.class, "noop"),
        new NamedType(TestTaskAuthContext.class, "test")
    );

    redactingMapper = mapper.copy()
        .addMixIn(PasswordProvider.class, PasswordProviderRedactionMixIn.class)
        .addMixIn(TaskAuthContext.class, TaskAuthContextRedactionMixIn.class);
  }

  @Test
  public void testNullAuthContextIsOmittedFromJson() throws Exception
  {
    NoopTask task = NoopTask.create();
    Assert.assertNull(task.getTaskAuthContext());

    String json = mapper.writeValueAsString(task);
    Assert.assertFalse(json.contains("taskAuthContext"));
  }

  @Test
  public void testAuthContextSerializedWhenPresent() throws Exception
  {
    NoopTask task = NoopTask.create();
    task.setTaskAuthContext(new TestTaskAuthContext(
        "user@test.com",
        Map.of("token", "my-secret-token"),
        null
    ));

    String json = mapper.writeValueAsString(task);
    Assert.assertTrue(json.contains("taskAuthContext"));
    Assert.assertTrue(json.contains("user@test.com"));
    Assert.assertTrue(json.contains("my-secret-token"));
  }

  @Test
  public void testRedactingMapperRemovesCredentialsFromTask() throws Exception
  {
    NoopTask task = NoopTask.create();
    task.setTaskAuthContext(new TestTaskAuthContext(
        "user@test.com",
        Map.of("token", "my-secret-token"),
        Map.of("scope", "catalog")
    ));

    String json = redactingMapper.writeValueAsString(task);

    Assert.assertFalse(json.contains("my-secret-token"));
    Assert.assertTrue(json.contains("user@test.com"));
    Assert.assertTrue(json.contains("catalog"));
  }

  @Test
  public void testRoundTripSerializationPreservesAuthContext() throws Exception
  {
    NoopTask original = NoopTask.create();
    original.setTaskAuthContext(new TestTaskAuthContext(
        "user@test.com",
        Map.of("token", "my-secret-token"),
        null
    ));

    String json = mapper.writeValueAsString(original);
    Task deserialized = mapper.readValue(json, Task.class);

    Assert.assertNotNull(deserialized.getTaskAuthContext());
    Assert.assertEquals("user@test.com", deserialized.getTaskAuthContext().getIdentity());
    Assert.assertEquals("my-secret-token", deserialized.getTaskAuthContext().getCredentials().get("token"));
  }

  @Test
  public void testRedactedJsonDoesNotDeserializeCredentials() throws Exception
  {
    NoopTask original = NoopTask.create();
    original.setTaskAuthContext(new TestTaskAuthContext(
        "user@test.com",
        Map.of("token", "my-secret-token"),
        null
    ));

    String redactedJson = redactingMapper.writeValueAsString(original);
    Task deserialized = mapper.readValue(redactedJson, Task.class);
    Assert.assertNotNull(deserialized.getTaskAuthContext());
    Assert.assertEquals("user@test.com", deserialized.getTaskAuthContext().getIdentity());
    Assert.assertNull(deserialized.getTaskAuthContext().getCredentials());
  }

  @Test
  public void testSetAndGetAuthContext()
  {
    NoopTask task = NoopTask.create();
    Assert.assertNull(task.getTaskAuthContext());

    TestTaskAuthContext ctx = new TestTaskAuthContext("user", Map.of("k", "v"), null);
    task.setTaskAuthContext(ctx);
    Assert.assertSame(ctx, task.getTaskAuthContext());

    task.setTaskAuthContext(null);
    Assert.assertNull(task.getTaskAuthContext());
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
