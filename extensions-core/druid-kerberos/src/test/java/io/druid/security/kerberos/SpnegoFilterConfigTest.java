/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.security.kerberos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.guice.ConfigModule;
import io.druid.guice.DruidGuiceExtensions;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.PropertiesModule;
import io.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

public class SpnegoFilterConfigTest
{
  @Test
  public void testserde()
  {
    Injector injector = Guice.createInjector(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.install(new PropertiesModule(Arrays.asList("test.runtime.properties")));
            binder.install(new ConfigModule());
            binder.install(new DruidGuiceExtensions());
            JsonConfigProvider.bind(binder, "druid.hadoop.security.spnego", SpnegoFilterConfig.class);
          }

          @Provides
          @LazySingleton
          public ObjectMapper jsonMapper()
          {
            return new DefaultObjectMapper();
          }
        }
    );

    Properties props = injector.getInstance(Properties.class);
    SpnegoFilterConfig config = injector.getInstance(SpnegoFilterConfig.class);

    Assert.assertEquals(props.getProperty("druid.hadoop.security.spnego.principal"), config.getPrincipal());
    Assert.assertEquals(props.getProperty("druid.hadoop.security.spnego.keytab"), config.getKeytab());
    Assert.assertEquals(props.getProperty("druid.hadoop.security.spnego.authToLocal"), config.getAuthToLocal());


  }
}
