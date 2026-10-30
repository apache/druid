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

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.server.log.StartupLoggingConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class SensitivePropertiesTest
{
  @Test
  public void testDefaults()
  {
    final SensitiveProperties sensitiveProperties = new SensitiveProperties(null, null);

    Assertions.assertTrue(sensitiveProperties.isSensitive("druid.metadata.storage.connector.password"));
    Assertions.assertTrue(sensitiveProperties.isSensitive("druid.escalator.internalClientPassword"));
    Assertions.assertTrue(sensitiveProperties.isSensitive("druid.s3.accessKey"));
    Assertions.assertTrue(sensitiveProperties.isSensitive("druid.s3.secretKey"));
    Assertions.assertTrue(sensitiveProperties.isSensitive("druid.zk.service.pwd"));
    Assertions.assertTrue(sensitiveProperties.isSensitive("druid.emitter.kafka.producer.hiddenProperties"));
    Assertions.assertTrue(sensitiveProperties.isSensitive("druid.emitter.http.basicAuthentication"));
    Assertions.assertTrue(sensitiveProperties.isSensitive("druid.auth.pac4j.cookiePassphrase"));
    Assertions.assertFalse(sensitiveProperties.isSensitive("druid.host"));
    Assertions.assertFalse(sensitiveProperties.isSensitive("druid.emitter.kafka.bootstrap.servers"));
  }

  @Test
  public void testHiddenPropertiesReplaceDefaults()
  {
    final SensitiveProperties sensitiveProperties = makeSensitiveProperties(
        "druid.server.hiddenProperties",
        "[\"MyCustomName\"]"
    );

    Assertions.assertTrue(sensitiveProperties.isSensitive("druid.mycustomname.value"));
    Assertions.assertFalse(sensitiveProperties.isSensitive("druid.s3.accessKey"));
    // the former default of the deprecated maskProperties still applies
    Assertions.assertTrue(sensitiveProperties.isSensitive("druid.metadata.storage.connector.password"));
  }

  @Test
  public void testDeprecatedMaskPropertiesAreHonored()
  {
    final SensitiveProperties sensitiveProperties = makeSensitiveProperties(
        "druid.startup.logging.maskProperties",
        "[\"MyMaskedName\"]"
    );

    Assertions.assertTrue(sensitiveProperties.isSensitive("druid.MYMASKEDNAME.value"));
    Assertions.assertTrue(sensitiveProperties.isSensitive("druid.metadata.storage.connector.password"));
  }

  private static SensitiveProperties makeSensitiveProperties(final String propertyName, final String propertyValue)
  {
    final Properties properties = new Properties();
    properties.setProperty(propertyName, propertyValue);
    final Injector injector = new CoreInjectorBuilder(new StartupInjectorBuilder().withProperties(properties).build())
        .add((Module) SensitivePropertiesTest::bindConfigs)
        .build();
    return injector.getInstance(SensitiveProperties.class);
  }

  /**
   * Binds the configs read by {@link SensitiveProperties}, as {@link org.apache.druid.guice.StartupLoggingModule}
   * and {@link org.apache.druid.guice.StorageNodeModule} do.
   */
  public static void bindConfigs(final Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.startup.logging", StartupLoggingConfig.class);
    JsonConfigProvider.bind(binder, "druid.server", DruidServerConfig.class);
    binder.bind(SegmentLoaderConfig.class).toInstance(SegmentLoaderConfig.builder().build());
  }
}
