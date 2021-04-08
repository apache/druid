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

package org.apache.druid.server.initialization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

public class ExternalStorageAccessSecurityModuleTest
{
  @Test
  public void testSecurityConfigDefault()
  {
    JdbcAccessSecurityConfig securityConfig = makeInjectorWithProperties(new Properties()).getInstance(
        JdbcAccessSecurityConfig.class
    );
    Assert.assertNotNull(securityConfig);
    Assert.assertEquals(
        JdbcAccessSecurityConfig.DEFAULT_ALLOWED_PROPERTIES,
        securityConfig.getAllowedProperties()
    );
    Assert.assertTrue(securityConfig.isAllowUnknownJdbcUrlFormat());
    Assert.assertTrue(securityConfig.isEnforceAllowedProperties());
  }

  @Test
  public void testSecurityConfigOverride()
  {
    Properties properties = new Properties();
    properties.setProperty("druid.access.jdbc.allowedProperties", "[\"valid1\", \"valid2\", \"valid3\"]");
    properties.setProperty("druid.access.jdbc.allowUnknownJdbcUrlFormat", "false");
    properties.setProperty("druid.access.jdbc.enforceAllowedProperties", "true");
    JdbcAccessSecurityConfig securityConfig = makeInjectorWithProperties(properties).getInstance(
        JdbcAccessSecurityConfig.class
    );
    Assert.assertNotNull(securityConfig);
    Assert.assertEquals(
        ImmutableSet.of(
            "valid1",
            "valid2",
            "valid3"
        ),
        securityConfig.getAllowedProperties()
    );
    Assert.assertFalse(securityConfig.isAllowUnknownJdbcUrlFormat());
    Assert.assertTrue(securityConfig.isEnforceAllowedProperties());
  }

  private static Injector makeInjectorWithProperties(final Properties props)
  {
    return Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            binder -> {
              binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
              binder.bind(JsonConfigurator.class).in(LazySingleton.class);
              binder.bind(Properties.class).toInstance(props);
            },
            new ExternalStorageAccessSecurityModule()
        )
    );
  }
}
