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

package io.druid.server.emitter;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.ParametrizedUriEmitter;
import io.druid.guice.DruidGuiceExtensions;
import io.druid.guice.JsonConfigurator;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ServerModule;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

public class EmitterModuleTest
{

  @Test
  public void testParametrizedUriEmitterConfig()
  {
    final Properties props = new Properties();
    props.setProperty("druid.emitter", "parametrized");
    props.setProperty("druid.emitter.parametrized.recipientBaseUrlPattern", "http://example.com:8888/{feed}");
    props.setProperty("druid.emitter.parametrized.httpEmitting.flushMillis", "1");
    props.setProperty("druid.emitter.parametrized.httpEmitting.flushCount", "2");
    props.setProperty("druid.emitter.parametrized.httpEmitting.basicAuthentication", "a:b");
    props.setProperty("druid.emitter.parametrized.httpEmitting.batchingStrategy", "NEWLINES");
    props.setProperty("druid.emitter.parametrized.httpEmitting.maxBatchSize", "4");
    props.setProperty("druid.emitter.parametrized.httpEmitting.flushTimeOut", "1000");

    final Emitter emitter =
        makeInjectorWithProperties(props).getInstance(Emitter.class);
    // Testing that ParametrizedUriEmitter is successfully deserialized from the above config
    Assert.assertTrue(emitter instanceof ParametrizedUriEmitter);
  }

  private Injector makeInjectorWithProperties(final Properties props)
  {
    return Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            new LifecycleModule(),
            new ServerModule(),
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
            new EmitterModule(props)
        )
    );
  }
}
