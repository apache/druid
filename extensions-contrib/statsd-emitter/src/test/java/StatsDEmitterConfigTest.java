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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.timgroup.statsd.StatsDClient;
import io.druid.emitter.statsd.DefaultStatsDEventHandler;
import io.druid.emitter.statsd.StatsDDimension;
import io.druid.emitter.statsd.StatsDEmitterConfig;
import io.druid.emitter.statsd.StatsDEventHandler;
import io.druid.guice.DruidScopes;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.JsonConfigurator;
import io.druid.guice.LazySingleton;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.emitter.core.Event;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.validation.executable.ExecutableValidator;
import javax.validation.metadata.BeanDescriptor;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class StatsDEmitterConfigTest
{
  @Test
  public void testDefaultEventHandlerDeserialization1()
  {
    Properties props = new Properties();
    props.setProperty("druid.emitter.statsd.hostname", "host");
    props.setProperty("druid.emitter.statsd.port", "123");
    props.setProperty("druid.emitter.statsd.eventHandler", "{\"type\": \"default\"}");

    final Injector injector = Guice.createInjector(
        (Module) binder -> {
          JsonConfigProvider.bind(binder, "druid.emitter.statsd", StatsDEmitterConfig.class);
          ObjectMapper mapper = new DefaultObjectMapper();

          mapper.registerModule(
              new SimpleModule().registerSubtypes(
                  new NamedType(TestStatsDEventHandler.class, "test")
              ));
          binder.bind(Properties.class).toInstance(props);
          binder.bindScope(LazySingleton.class, DruidScopes.SINGLETON);
          binder.bind(JsonConfigurator.class).toInstance(new JsonConfigurator(
              mapper,
              new Validator()
              {
                @Override
                public <T> Set<ConstraintViolation<T>> validate(T object, Class<?>... groups)
                {
                  return ImmutableSet.of();
                }

                @Override
                public <T> Set<ConstraintViolation<T>> validateProperty(
                    T object,
                    String propertyName,
                    Class<?>... groups
                )
                {
                  return ImmutableSet.of();
                }

                @Override
                public <T> Set<ConstraintViolation<T>> validateValue(
                    Class<T> beanType, String propertyName, Object value, Class<?>... groups
                )
                {
                  return ImmutableSet.of();
                }

                @Override
                public BeanDescriptor getConstraintsForClass(Class<?> clazz)
                {
                  return null;
                }

                @Override
                public <T> T unwrap(Class<T> type)
                {
                  return null;
                }

                @Override
                public ExecutableValidator forExecutables()
                {
                  return null;
                }
              }
          ));

        }
    );
    StatsDEmitterConfig statsDEmitterConfig = injector.getInstance(StatsDEmitterConfig.class);
    Assert.assertTrue(statsDEmitterConfig.getEventHandler() instanceof DefaultStatsDEventHandler);
  }

  @Test
  public void testDefaultEventHandlerDeserialization2()
  {
    Properties props = new Properties();
    props.setProperty("druid.emitter.statsd.hostname", "host");
    props.setProperty("druid.emitter.statsd.port", "123");
    props.setProperty("druid.emitter.statsd.eventHandler", "{\"type\": \"nothing\"}");

    final Injector injector = Guice.createInjector(
        (Module) binder -> {
          JsonConfigProvider.bind(binder, "druid.emitter.statsd", StatsDEmitterConfig.class);
          ObjectMapper mapper = new DefaultObjectMapper();

          mapper.registerModule(
              new SimpleModule().registerSubtypes(
                  new NamedType(TestStatsDEventHandler.class, "test")
              ));
          binder.bind(Properties.class).toInstance(props);
          binder.bindScope(LazySingleton.class, DruidScopes.SINGLETON);
          binder.bind(JsonConfigurator.class).toInstance(new JsonConfigurator(
              mapper,
              new Validator()
              {
                @Override
                public <T> Set<ConstraintViolation<T>> validate(T object, Class<?>... groups)
                {
                  return ImmutableSet.of();
                }

                @Override
                public <T> Set<ConstraintViolation<T>> validateProperty(
                    T object,
                    String propertyName,
                    Class<?>... groups
                )
                {
                  return ImmutableSet.of();
                }

                @Override
                public <T> Set<ConstraintViolation<T>> validateValue(
                    Class<T> beanType, String propertyName, Object value, Class<?>... groups
                )
                {
                  return ImmutableSet.of();
                }

                @Override
                public BeanDescriptor getConstraintsForClass(Class<?> clazz)
                {
                  return null;
                }

                @Override
                public <T> T unwrap(Class<T> type)
                {
                  return null;
                }

                @Override
                public ExecutableValidator forExecutables()
                {
                  return null;
                }
              }
          ));

        }
    );
    StatsDEmitterConfig statsDEmitterConfig = injector.getInstance(StatsDEmitterConfig.class);
    Assert.assertTrue(statsDEmitterConfig.getEventHandler() instanceof DefaultStatsDEventHandler);
  }

  @Test
  public void testDefaultEventHandlerDeserialization3()
  {
    Properties props = new Properties();
    props.setProperty("druid.emitter.statsd.hostname", "host");
    props.setProperty("druid.emitter.statsd.port", "123");
    // no eventHandler property set

    final Injector injector = Guice.createInjector(
        (Module) binder -> {
          JsonConfigProvider.bind(binder, "druid.emitter.statsd", StatsDEmitterConfig.class);
          ObjectMapper mapper = new DefaultObjectMapper();

          mapper.registerModule(
              new SimpleModule().registerSubtypes(
                  new NamedType(TestStatsDEventHandler.class, "test")
              ));
          binder.bind(Properties.class).toInstance(props);
          binder.bindScope(LazySingleton.class, DruidScopes.SINGLETON);
          binder.bind(JsonConfigurator.class).toInstance(new JsonConfigurator(
              mapper,
              new Validator()
              {
                @Override
                public <T> Set<ConstraintViolation<T>> validate(T object, Class<?>... groups)
                {
                  return ImmutableSet.of();
                }

                @Override
                public <T> Set<ConstraintViolation<T>> validateProperty(
                    T object,
                    String propertyName,
                    Class<?>... groups
                )
                {
                  return ImmutableSet.of();
                }

                @Override
                public <T> Set<ConstraintViolation<T>> validateValue(
                    Class<T> beanType, String propertyName, Object value, Class<?>... groups
                )
                {
                  return ImmutableSet.of();
                }

                @Override
                public BeanDescriptor getConstraintsForClass(Class<?> clazz)
                {
                  return null;
                }

                @Override
                public <T> T unwrap(Class<T> type)
                {
                  return null;
                }

                @Override
                public ExecutableValidator forExecutables()
                {
                  return null;
                }
              }
          ));

        }
    );
    StatsDEmitterConfig statsDEmitterConfig = injector.getInstance(StatsDEmitterConfig.class);
    Assert.assertTrue(statsDEmitterConfig.getEventHandler() instanceof DefaultStatsDEventHandler);
  }


  @Test
  public void testCustomEventHandlerDeserialization()
  {
    Properties props = new Properties();
    props.setProperty("druid.emitter.statsd.hostname", "host");
    props.setProperty("druid.emitter.statsd.port", "123");
    props.setProperty("druid.emitter.statsd.eventHandler", "{\"type\": \"test\"}");

    final Injector injector = Guice.createInjector(
        (Module) binder -> {
          JsonConfigProvider.bind(binder, "druid.emitter.statsd", StatsDEmitterConfig.class);
          ObjectMapper mapper = new DefaultObjectMapper();

          mapper.registerModule(
              new SimpleModule().registerSubtypes(
                  new NamedType(TestStatsDEventHandler.class, "test")
              ));
          binder.bind(Properties.class).toInstance(props);
          binder.bindScope(LazySingleton.class, DruidScopes.SINGLETON);
          binder.bind(JsonConfigurator.class).toInstance(new JsonConfigurator(
              mapper,
              new Validator()
              {
                @Override
                public <T> Set<ConstraintViolation<T>> validate(T object, Class<?>... groups)
                {
                  return ImmutableSet.of();
                }

                @Override
                public <T> Set<ConstraintViolation<T>> validateProperty(
                    T object,
                    String propertyName,
                    Class<?>... groups
                )
                {
                  return ImmutableSet.of();
                }

                @Override
                public <T> Set<ConstraintViolation<T>> validateValue(
                    Class<T> beanType, String propertyName, Object value, Class<?>... groups
                )
                {
                  return ImmutableSet.of();
                }

                @Override
                public BeanDescriptor getConstraintsForClass(Class<?> clazz)
                {
                  return null;
                }

                @Override
                public <T> T unwrap(Class<T> type)
                {
                  return null;
                }

                @Override
                public ExecutableValidator forExecutables()
                {
                  return null;
                }
              }
          ));

        }
    );
    StatsDEmitterConfig statsDEmitterConfig = injector.getInstance(StatsDEmitterConfig.class);
    Assert.assertTrue(statsDEmitterConfig.getEventHandler() instanceof TestStatsDEventHandler);
  }

  static class TestStatsDEventHandler implements StatsDEventHandler
  {
    @Override
    public void handleEvent(
        StatsDClient statsDClient, Event event, StatsDEmitterConfig config, Map<String, StatsDDimension> filterMap
    )
    {

    }
  }
}
