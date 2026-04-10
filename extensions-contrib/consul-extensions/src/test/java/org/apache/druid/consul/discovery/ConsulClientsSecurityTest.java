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

package org.apache.druid.consul.discovery;

import com.ecwid.consul.v1.ConsulClient;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Consul client security validation, especially around basic auth over HTTP.
 */
public class ConsulClientsSecurityTest
{
  @Test
  public void testBasicAuthOverHttpFailsFastByDefault()
  {
    ConsulDiscoveryConfig config = TestUtils.builder()
        .servicePrefix("druid")
        .basicAuthUser("admin")
        .basicAuthPassword("secret")
        // No TLS configured
        .build();

    IllegalStateException exception = Assert.assertThrows(
        IllegalStateException.class,
        () -> ConsulClients.create(config)
    );

    Assert.assertTrue(
        "Exception should mention TLS not enabled",
        exception.getMessage().contains("TLS is not enabled")
    );
    Assert.assertTrue(
        "Exception should mention cleartext transmission",
        exception.getMessage().contains("cleartext")
    );
    Assert.assertTrue(
        "Exception should mention allowBasicAuthOverHttp flag",
        exception.getMessage().contains("allowBasicAuthOverHttp")
    );
  }

  @Test
  public void testBasicAuthOverHttpSucceedsWithExplicitFlag()
  {
    ConsulDiscoveryConfig config = TestUtils.builder()
        .servicePrefix("druid")
        .basicAuthUser("admin")
        .basicAuthPassword("secret")
        .allowBasicAuthOverHttp(true)
        // No TLS configured
        .build();

    // Should not throw with the flag enabled
    ConsulClient client = ConsulClients.create(config);
    Assert.assertNotNull(client);
  }

  @Test
  public void testBasicAuthOverHttpFailsWhenExplicitlyDisabled()
  {
    ConsulDiscoveryConfig config = TestUtils.builder()
        .servicePrefix("druid")
        .basicAuthUser("admin")
        .basicAuthPassword("secret")
        .allowBasicAuthOverHttp(false)
        // No TLS configured
        .build();

    IllegalStateException exception = Assert.assertThrows(
        IllegalStateException.class,
        () -> ConsulClients.create(config)
    );

    Assert.assertTrue(
        "Exception should mention TLS not enabled",
        exception.getMessage().contains("TLS is not enabled")
    );
  }

  @Test
  public void testNoBasicAuthOverHttpSucceeds()
  {
    ConsulDiscoveryConfig config = TestUtils.builder()
        .servicePrefix("druid")
        // No basic auth configured
        .build();

    // Should succeed without basic auth even without TLS
    ConsulClient client = ConsulClients.create(config);
    Assert.assertNotNull(client);
  }

  @Test
  public void testBasicAuthUserWithoutPasswordDoesNotTriggerValidation()
  {
    ConsulDiscoveryConfig config = TestUtils.builder()
        .servicePrefix("druid")
        .basicAuthUser("admin")
        // No password configured
        .build();

    // Should succeed - validation only applies when both user and password are set
    ConsulClient client = ConsulClients.create(config);
    Assert.assertNotNull(client);
  }

  @Test
  public void testBasicAuthPasswordWithoutUserDoesNotTriggerValidation()
  {
    ConsulDiscoveryConfig config = TestUtils.builder()
        .servicePrefix("druid")
        .basicAuthPassword("secret")
        // No user configured
        .build();

    // Should succeed - validation only applies when both user and password are set
    ConsulClient client = ConsulClients.create(config);
    Assert.assertNotNull(client);
  }

  @Test
  public void testAllowBasicAuthOverHttpDefaultsToFalse()
  {
    ConsulDiscoveryConfig config = TestUtils.builder()
        .servicePrefix("druid")
        .build();

    Assert.assertFalse(
        "allowBasicAuthOverHttp should default to false",
        config.getAuth().getAllowBasicAuthOverHttp()
    );
  }
}
