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

package org.apache.druid.metadata.storage.mysql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.MetadataConfigModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.security.EscalatorModule;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class MySQLMetadataStorageModuleTest
{
  @Test
  public void testSslConfig()
  {
    final Injector injector = createInjector();
    final String propertyPrefix = "druid.metadata.mysql.ssl";
    final JsonConfigProvider<MySQLConnectorSslConfig> provider = JsonConfigProvider.of(
        propertyPrefix,
        MySQLConnectorSslConfig.class
    );
    final Properties properties = new Properties();
    properties.setProperty(propertyPrefix + ".useSSL", "true");
    properties.setProperty(propertyPrefix + ".trustCertificateKeyStoreUrl", "url");
    properties.setProperty(propertyPrefix + ".trustCertificateKeyStoreType", "type");
    properties.setProperty(propertyPrefix + ".trustCertificateKeyStorePassword", "secret");
    properties.setProperty(propertyPrefix + ".clientCertificateKeyStoreUrl", "url");
    properties.setProperty(propertyPrefix + ".clientCertificateKeyStoreType", "type");
    properties.setProperty(propertyPrefix + ".clientCertificateKeyStorePassword", "secret");
    properties.setProperty(propertyPrefix + ".enabledSSLCipherSuites", "[\"some\", \"ciphers\"]");
    properties.setProperty(propertyPrefix + ".enabledTLSProtocols", "[\"some\", \"protocols\"]");
    properties.setProperty(propertyPrefix + ".verifyServerCertificate", "true");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final MySQLConnectorSslConfig config = provider.get();
    Assert.assertTrue(config.isUseSSL());
    Assert.assertEquals("url", config.getTrustCertificateKeyStoreUrl());
    Assert.assertEquals("type", config.getTrustCertificateKeyStoreType());
    Assert.assertEquals("secret", config.getTrustCertificateKeyStorePassword());
    Assert.assertEquals("url", config.getClientCertificateKeyStoreUrl());
    Assert.assertEquals("type", config.getClientCertificateKeyStoreType());
    Assert.assertEquals("secret", config.getClientCertificateKeyStorePassword());
    Assert.assertEquals(ImmutableList.of("some", "ciphers"), config.getEnabledSSLCipherSuites());
    Assert.assertEquals(ImmutableList.of("some", "protocols"), config.getEnabledTLSProtocols());
    Assert.assertTrue(config.isVerifyServerCertificate());
  }

  @Test
  public void testDriverConfigDefault()
  {
    final Injector injector = createInjector();
    final String propertyPrefix = "druid.metadata.mysql.driver";
    final JsonConfigProvider<MySQLConnectorDriverConfig> provider = JsonConfigProvider.of(
        propertyPrefix,
        MySQLConnectorDriverConfig.class
    );
    final Properties properties = new Properties();
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final MySQLConnectorDriverConfig config = provider.get();
    Assert.assertEquals(new MySQLConnectorDriverConfig().getDriverClassName(), config.getDriverClassName());
  }

  @Test
  public void testDriverConfig()
  {
    final Injector injector = createInjector();
    final String propertyPrefix = "druid.metadata.mysql.driver";
    final JsonConfigProvider<MySQLConnectorDriverConfig> provider = JsonConfigProvider.of(
        propertyPrefix,
        MySQLConnectorDriverConfig.class
    );
    final Properties properties = new Properties();
    properties.setProperty(propertyPrefix + ".driverClassName", "some.driver.classname");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final MySQLConnectorDriverConfig config = provider.get();
    Assert.assertEquals("some.driver.classname", config.getDriverClassName());
  }

  private Injector createInjector()
  {
    MySQLMetadataStorageModule module = new MySQLMetadataStorageModule();
    Injector injector = GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.of(
            new EscalatorModule(),
            new MetadataConfigModule(),
            new LifecycleModule(),
            module,
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                module.createBindingChoices(binder, "mysql");
              }

              @Provides
              public ServiceEmitter getEmitter()
              {
                return new ServiceEmitter("test", "localhost", new NoopEmitter());
              }
            }
        )
    );
    ObjectMapper mapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    mapper.registerModules(module.getJacksonModules());
    return injector;
  }
}
