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

package org.apache.druid.sql.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class GetQueriesResponseTest
{
  private static ObjectMapper jsonMapper;

  @BeforeAll
  static void setUp()
  {
    jsonMapper = TestHelper.makeJsonMapper().registerModules(getJacksonModules());
  }

  @Test
  public void test_serde() throws Exception
  {
    final GetQueriesResponse response = new GetQueriesResponse(
        Collections.singletonList(
            new TestQueryInfo(
                "query",
                "xyz",
                "abc"
            )
        )
    );
    final GetQueriesResponse response2 =
        jsonMapper.readValue(jsonMapper.writeValueAsBytes(response), GetQueriesResponse.class);
    Assertions.assertEquals(response, response2);
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(GetQueriesResponse.class).usingGetClass().verify();
  }

  static class TestQueryInfo implements QueryInfo
  {
    private final String query;
    private final String identity;
    private final String authenticator;

    @JsonCreator
    public TestQueryInfo(
        @JsonProperty("query") String query,
        @JsonProperty("identity") String identity,
        @JsonProperty("authenticator") String authenticator
    )
    {
      this.query = query;
      this.identity = identity;
      this.authenticator = authenticator;
    }

    @JsonProperty
    public String getQuery()
    {
      return query;
    }

    @JsonProperty
    public String getIdentity()
    {
      return identity;
    }

    @JsonProperty
    public String getAuthenticator()
    {
      return authenticator;
    }

    @Override
    public String engine()
    {
      return "test";
    }

    @Override
    public String state()
    {
      return "RUNNING";
    }

    @Override
    public String executionId()
    {
      return "test-execution-id";
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestQueryInfo that = (TestQueryInfo) o;
      return Objects.equals(query, that.query)
             && Objects.equals(identity, that.identity)
             && Objects.equals(authenticator, that.authenticator);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(query, identity, authenticator);
    }
  }

  private static List<? extends Module> getJacksonModules()
  {
    return Collections.<com.fasterxml.jackson.databind.Module>singletonList(
        new SimpleModule("TestModule").registerSubtypes(
            new NamedType(
                TestQueryInfo.class,
                "test"
            )
        )
    );
  }
}
