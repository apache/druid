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

package org.apache.druid.server.security;

import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.query.context.QueryContextParameters;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class AuthConfigTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.configure().usingGetClass().forClass(AuthConfig.class).verify();
  }

  @Test
  public void testEmitAuthMetricsDefaultsFalse()
  {
    Assertions.assertFalse(new AuthConfig().isEmitAuthMetrics());
    Assertions.assertFalse(AuthConfig.newBuilder().build().isEmitAuthMetrics());
  }

  @Test
  public void testEmitAuthMetricsCanBeEnabled()
  {
    Assertions.assertTrue(AuthConfig.newBuilder().setEmitAuthMetrics(true).build().isEmitAuthMetrics());
  }

  @Test
  public void testContextSecurity()
  {
    // No security
    {
      AuthConfig config = new AuthConfig();
      Set<String> keys = ImmutableSet.of("a", "b", QueryContextParameters.SQL_QUERY_ID.getName());
      Assertions.assertTrue(config.contextKeysToAuthorize(keys).isEmpty());
    }

    // Default security
    {
      AuthConfig config = AuthConfig.newBuilder()
          .setAuthorizeQueryContextParams(true)
          .build();
      Set<String> keys = ImmutableSet.of("a", "b", QueryContextParameters.SQL_QUERY_ID.getName());
      Assertions.assertEquals(ImmutableSet.of("a", "b"), config.contextKeysToAuthorize(keys));
    }

    // Specify unsecured keys (white-list)
    {
      AuthConfig config = AuthConfig.newBuilder()
          .setAuthorizeQueryContextParams(true)
          .setUnsecuredContextKeys(ImmutableSet.of("a"))
          .build();
      Set<String> keys = ImmutableSet.of("a", "b", QueryContextParameters.SQL_QUERY_ID.getName());
      Assertions.assertEquals(ImmutableSet.of("b"), config.contextKeysToAuthorize(keys));
    }

    // Specify secured keys (black-list)
    {
      AuthConfig config = AuthConfig.newBuilder()
          .setAuthorizeQueryContextParams(true)
          .setSecuredContextKeys(ImmutableSet.of("a"))
          .build();
      Set<String> keys = ImmutableSet.of("a", "b", QueryContextParameters.SQL_QUERY_ID.getName());
      Assertions.assertEquals(ImmutableSet.of("a"), config.contextKeysToAuthorize(keys));
    }

    // Specify both
    {
      AuthConfig config = AuthConfig.newBuilder()
          .setAuthorizeQueryContextParams(true)
          .setUnsecuredContextKeys(ImmutableSet.of("a", "b"))
          .setSecuredContextKeys(ImmutableSet.of("b", "c"))
          .build();
      Set<String> keys = ImmutableSet.of("a", "b", "c", "d", QueryContextParameters.SQL_QUERY_ID.getName());
      Assertions.assertEquals(ImmutableSet.of("c"), config.contextKeysToAuthorize(keys));
    }
  }
}
