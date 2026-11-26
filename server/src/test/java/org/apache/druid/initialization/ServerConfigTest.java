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
import jakarta.validation.Validation;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.apache.druid.common.exception.AllowedRegexErrorResponseTransformStrategy;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.initialization.ServerConfig;
import org.eclipse.jetty.http.UriCompliance;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.ws.rs.HttpMethod;
import java.util.Properties;

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
    Assertions.assertEquals(0, defaultConfig2.getHeaderCacheSize());

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
        true,
        UriCompliance.RFC3986,
        false,
        true,
        512
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
    Assertions.assertTrue(modifiedConfig2.isEnableResponseIdentityHeaders());
    Assertions.assertEquals(UriCompliance.RFC3986, modifiedConfig2.getUriCompliance());
    Assertions.assertFalse(modifiedConfig2.isEnforceStrictSNIHostChecking());
    Assertions.assertEquals(512, modifiedConfig2.getHeaderCacheSize());
  }

  @Test
  public void testHumanReadableQueryTimeoutProperties()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.server.http.defaultQueryTimeout", "5m");
    properties.setProperty("druid.server.http.maxQueryTimeout", "PT10M");

    final ServerConfig config = fromProperties(properties);
    Assertions.assertEquals(300_000L, config.getDefaultQueryTimeout());
    Assertions.assertEquals(600_000L, config.getMaxQueryTimeout());
  }

  @Test
  public void testLegacyQueryTimeoutPropertiesKeepFullLongRange()
  {
    final ServerConfig defaults = fromProperties(new Properties());
    Assertions.assertEquals(300_000L, defaults.getDefaultQueryTimeout());
    Assertions.assertEquals(Long.MAX_VALUE, defaults.getMaxQueryTimeout());

    final Properties properties = new Properties();
    properties.setProperty("druid.server.http.defaultQueryTimeout", "300000");
    properties.setProperty("druid.server.http.maxQueryTimeout", String.valueOf(Long.MAX_VALUE));

    final ServerConfig config = fromProperties(properties);
    Assertions.assertEquals(300_000L, config.getDefaultQueryTimeout());
    Assertions.assertEquals(Long.MAX_VALUE, config.getMaxQueryTimeout());
  }

  @Test
  public void testQueryTimeoutSubMillisecondValuesRoundUp()
  {
    final Properties properties = new Properties();
    properties.setProperty("druid.server.http.defaultQueryTimeout", "250us");
    properties.setProperty("druid.server.http.maxQueryTimeout", "PT0.000001S");

    final ServerConfig config = fromProperties(properties);
    Assertions.assertEquals(1L, config.getDefaultQueryTimeout());
    Assertions.assertEquals(1L, config.getMaxQueryTimeout());
  }

  @Test
  public void testQueryTimeoutJsonKeepsNumericMillisecondSerialization() throws Exception
  {
    final ServerConfig config = OBJECT_MAPPER.readValue(
        "{\"defaultQueryTimeout\":\"30s\",\"maxQueryTimeout\":\"PT1M\"}",
        ServerConfig.class
    );
    Assertions.assertEquals(30_000L, config.getDefaultQueryTimeout());
    Assertions.assertEquals(60_000L, config.getMaxQueryTimeout());
    final String json = OBJECT_MAPPER.writeValueAsString(config);
    Assertions.assertTrue(OBJECT_MAPPER.readTree(json).get("defaultQueryTimeout").isIntegralNumber());
    Assertions.assertTrue(OBJECT_MAPPER.readTree(json).get("maxQueryTimeout").isIntegralNumber());
  }

  @Test
  public void testInvalidQueryTimeoutPropertiesAreRejected()
  {
    for (String invalid : new String[]{"-5s", "1.5h", "P1M", "30S", "9999999999d"}) {
      final Properties properties = new Properties();
      properties.setProperty("druid.server.http.defaultQueryTimeout", invalid);
      Assertions.assertThrows(RuntimeException.class, () -> fromProperties(properties), invalid);
    }

    final Properties properties = new Properties();
    properties.setProperty("druid.server.http.maxQueryTimeout", "0");
    Assertions.assertThrows(RuntimeException.class, () -> fromProperties(properties));
  }

  private static ServerConfig fromProperties(Properties properties)
  {
    final JsonConfigurator configurator = new JsonConfigurator(
        OBJECT_MAPPER,
        Validation.buildDefaultValidatorFactory().getValidator()
    );
    return configurator.configurate(properties, "druid.server.http", ServerConfig.class);
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
