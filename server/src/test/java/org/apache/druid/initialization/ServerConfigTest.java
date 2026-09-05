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

package org.apache.druid.initialization;

import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.apache.druid.common.exception.AllowedRegexErrorResponseTransformStrategy;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.initialization.ServerConfig;
import org.eclipse.jetty.http.UriCompliance;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.ws.rs.HttpMethod;

public class ServerConfigTest
{
  private static final DefaultObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    ServerConfig defaultConfig = new ServerConfig();
    String defaultConfigJson = OBJECT_MAPPER.writeValueAsString(defaultConfig);
    ServerConfig defaultConfig2 = OBJECT_MAPPER.readValue(defaultConfigJson, ServerConfig.class);
    Assertions.assertEquals(defaultConfig, defaultConfig2);
    Assertions.assertFalse(defaultConfig2.isEnableForwardedRequestCustomizer());
    Assertions.assertFalse(defaultConfig2.isEnableHSTS());
    Assertions.assertFalse(defaultConfig2.isEnableResponseIdentityHeaders());
    Assertions.assertEquals(UriCompliance.LEGACY, defaultConfig.getUriCompliance());
    Assertions.assertEquals(true, defaultConfig.isEnforceStrictSNIHostChecking());

    ServerConfig modifiedConfig = new ServerConfig(
        999,
        888,
        defaultConfig.isEnableRequestLimit(),
        defaultConfig.getMaxIdleTime(),
        defaultConfig.getDefaultQueryTimeout(),
        defaultConfig.getMaxScatterGatherBytes(),
        defaultConfig.getMaxSubqueryRows(),
        defaultConfig.getMaxSubqueryBytes(),
        defaultConfig.isuseNestedForUnknownTypeInSubquery(),
        defaultConfig.getMaxQueryTimeout(),
        defaultConfig.getMaxRequestHeaderSize(),
        defaultConfig.getGracefulShutdownTimeout(),
        defaultConfig.getUnannouncePropagationDelay(),
        defaultConfig.getInflateBufferSize(),
        defaultConfig.getCompressionLevel(),
        true,
        ImmutableList.of(HttpMethod.OPTIONS),
        true,
        new AllowedRegexErrorResponseTransformStrategy(ImmutableList.of(".*")),
        "my-cool-policy",
        true,
        UriCompliance.RFC3986,
        false
    );
    String modifiedConfigJson = OBJECT_MAPPER.writeValueAsString(modifiedConfig);
    ServerConfig modifiedConfig2 = OBJECT_MAPPER.readValue(modifiedConfigJson, ServerConfig.class);
    Assertions.assertEquals(modifiedConfig, modifiedConfig2);
    Assertions.assertEquals(999, modifiedConfig2.getNumThreads());
    Assertions.assertEquals(888, modifiedConfig2.getQueueSize());
    Assertions.assertTrue(modifiedConfig2.getErrorResponseTransformStrategy() instanceof AllowedRegexErrorResponseTransformStrategy);
    Assertions.assertTrue(modifiedConfig2.isEnableForwardedRequestCustomizer());
    Assertions.assertEquals(1, modifiedConfig2.getAllowedHttpMethods().size());
    Assertions.assertTrue(modifiedConfig2.getAllowedHttpMethods().contains(HttpMethod.OPTIONS));
    Assertions.assertEquals("my-cool-policy", modifiedConfig.getContentSecurityPolicy());
    Assertions.assertEquals("my-cool-policy", modifiedConfig2.getContentSecurityPolicy());
    Assertions.assertTrue(modifiedConfig2.isEnableHSTS());
    Assertions.assertEquals(UriCompliance.RFC3986, modifiedConfig2.getUriCompliance());
    Assertions.assertFalse(modifiedConfig2.isEnforceStrictSNIHostChecking());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ServerConfig.class)
                  // this class uses non-final fields for serialization / de-serialization.
                  // There are no setters that mutate the fields, once the object is instantiated.
                  .suppress(Warning.NONFINAL_FIELDS)
                  .usingGetClass()
                  .verify();
  }
}
