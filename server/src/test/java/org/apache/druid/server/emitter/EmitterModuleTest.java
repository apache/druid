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

package org.apache.druid.server.emitter;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.ServerModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.core.ParametrizedUriEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.server.DruidNode;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.validation.Validation;
import javax.validation.Validator;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class EmitterModuleTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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

    final Emitter emitter = makeInjectorWithProperties(props).getInstance(Emitter.class);

    // Testing that ParametrizedUriEmitter is successfully deserialized from the above config
    Assert.assertThat(emitter, CoreMatchers.instanceOf(ParametrizedUriEmitter.class));
  }

  @Test
  public void testMissingEmitterType()
  {
    final Properties props = new Properties();
    props.setProperty("druid.emitter", "");

    final Emitter emitter = makeInjectorWithProperties(props).getInstance(Emitter.class);
    Assert.assertThat(emitter, CoreMatchers.instanceOf(NoopEmitter.class));
  }

  @Test
  public void testInvalidEmitterType()
  {
    final Properties props = new Properties();
    props.setProperty("druid.emitter", "invalid");

    expectedException.expectMessage("Unknown emitter type[druid.emitter]=[invalid]");
    makeInjectorWithProperties(props).getInstance(Emitter.class);
  }

  @Test
  public void testGetServiceEmitterWithExtraServiceDimensionsHasAllDimensions()
  {
    final Properties props = new Properties();
    props.setProperty("druid.emitter", "noop");
    props.setProperty("druid.service", "druid/test");
    props.setProperty("druid.host", "localhost");

    ServiceEmitter emitter = makeInjectorWithProperties(props).getInstance(ServiceEmitter.class);
    ServiceEventBuilder<TestEvent> eventBuilder = new ServiceEventBuilder<TestEvent>()
    {
      @Override
      public TestEvent build(ImmutableMap<String, String> serviceDimensions)
      {
        Assert.assertFalse(serviceDimensions.containsKey("extraButEmpty"));
        Assert.assertEquals("extraVal", serviceDimensions.get("extra"));
        Assert.assertEquals("bar", serviceDimensions.get("foo"));
        return new TestEvent(serviceDimensions);
      }
    };
    emitter.emit(eventBuilder);
  }

  private Injector makeInjectorWithProperties(final Properties props)
  {
    return Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            new LifecycleModule(),
            binder -> binder.bind(new TypeLiteral<Supplier<DruidNode>>()
                            {
                            })
                            .annotatedWith(Self.class)
                            .toInstance(
                                Suppliers.ofInstance(new DruidNode("test", "host", false, 8091, null, true, false))
                            ),
            new JacksonModule(),
            binder -> {
              binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
              binder.bind(JsonConfigurator.class).in(LazySingleton.class);
              binder.bind(Properties.class).toInstance(props);
            },
            binder -> {
              MapBinder<String, Optional<String>> extraDims = MapBinder.newMapBinder(
                  binder,
                  new TypeLiteral<String>()
                  {
                  },
                  new TypeLiteral<Optional<String>>()
                  {
                  },
                  ExtraServiceDimensions.class
              );
              extraDims.addBinding("extra").toInstance(Optional.of("extraVal"));
              extraDims.addBinding("extraButEmpty").toInstance(Optional.empty());
              MapBinder.newMapBinder(binder, String.class, String.class, ExtraServiceDimensions.class)
                       .addBinding("foo").toInstance("bar");
            },
            new EmitterModule(props)
        )
    );
  }

  public static class TestEvent implements Event
  {

    private final Map<String, String> dimensions;

    public TestEvent(Map<String, String> dimensions)
    {
      this.dimensions = dimensions;
    }

    @Override
    public EventMap toMap()
    {
      return EventMap.builder().putAll(dimensions).build();
    }

    @Override
    public String getFeed()
    {
      return "test";
    }
  }
}
