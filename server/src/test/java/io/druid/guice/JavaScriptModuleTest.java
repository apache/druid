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

package io.druid.guice;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.druid.js.JavaScriptConfig;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

public class JavaScriptModuleTest
{
  @Test
  public void testInjectionDefault() throws Exception
  {
    JavaScriptConfig config = makeInjectorWithProperties(new Properties()).getInstance(JavaScriptConfig.class);
    Assert.assertFalse(config.isDisabled());
  }

  @Test
  public void testInjectionDisabled() throws Exception
  {
    final Properties props = new Properties();
    props.setProperty("druid.javascript.disabled", "true");
    JavaScriptConfig config = makeInjectorWithProperties(props).getInstance(JavaScriptConfig.class);
    Assert.assertTrue(config.isDisabled());
  }

  private Injector makeInjectorWithProperties(final Properties props)
  {
    return Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
                binder.bind(JsonConfigurator.class).in(LazySingleton.class);
                binder.bind(Properties.class).toInstance(props);
              }
            },
            new JavaScriptModule()
        )
    );
  }
}
