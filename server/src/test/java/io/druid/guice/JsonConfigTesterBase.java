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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.initialization.Initialization;
import io.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 *
 */
public abstract class JsonConfigTesterBase<T>
{

  protected static final String configPrefix = "druid.test.prefix";
  protected Injector injector;
  protected final Class<T> clazz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];

  protected Map<String, String> propertyValues = new HashMap<>();
  protected int assertions = 0;
  protected Properties testProperties = new Properties();

  protected static String getPropertyKey(String fieldName)
  {
    return StringUtils.format(
        "%s.%s",
        configPrefix, fieldName
    );
  }
  protected static String getPropertyKey(Field field)
  {
    JsonProperty jsonProperty = field.getAnnotation(JsonProperty.class);
    if (null != jsonProperty) {
      return getPropertyKey(
          (jsonProperty.value() == null || jsonProperty.value().isEmpty())
          ? field.getName()
          : jsonProperty.value()
      );
    }
    return null;
  }

  private final Module simpleJsonConfigModule = new Module()
  {
    @Override
    public void configure(Binder binder)
    {
      binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test");
      binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
      binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
      JsonConfigProvider.bind(binder, configPrefix, clazz);
    }
  };


  protected final void validateEntries(T config)
      throws IllegalAccessException, NoSuchMethodException, InvocationTargetException
  {
    for (Field field : clazz.getDeclaredFields()) {
      final String propertyKey = getPropertyKey(field);
      if (null != propertyKey) {
        field.setAccessible(true);
        String getter = StringUtils.format(
            "get%s%s",
            StringUtils.toUpperCase(field.getName().substring(0, 1)),
            field.getName().substring(1)
        );
        Method method = clazz.getDeclaredMethod(getter);
        final String value;
        if (null != method) {
          value = method.invoke(config).toString();
        } else {
          value = field.get(config).toString();
        }

        Assert.assertEquals(propertyValues.get(propertyKey), value);
        ++assertions;
      }
    }
  }

  protected JsonConfigurator configurator;
  protected JsonConfigProvider<T> configProvider;

  @Before
  public void setup() throws IllegalAccessException
  {
    assertions = 0;
    T fakeValues = EasyMock.createNiceMock(clazz);
    propertyValues.clear();
    testProperties.clear();
    for (Field field : clazz.getDeclaredFields()) {
      final String propertyKey = getPropertyKey(field);
      if (null != propertyKey) {
        field.setAccessible(true);
        Class<?> fieldType = field.getType();
        if (String.class.isAssignableFrom(fieldType)) {
          propertyValues.put(propertyKey, UUID.randomUUID().toString());
        } else if (Collection.class.isAssignableFrom(fieldType)) {
          propertyValues.put(propertyKey, "[]");
        } else if (Map.class.isAssignableFrom(fieldType)) {
          propertyValues.put(propertyKey, "{}");
        } else {
          propertyValues.put(propertyKey, String.valueOf(field.get(fakeValues)));
        }
      }
    }
    testProperties.putAll(System.getProperties());
    testProperties.putAll(propertyValues);
    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(simpleJsonConfigModule)
    );
    configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
    configProvider = JsonConfigProvider.of(configPrefix, clazz);
  }

  @Test
  public final void simpleInjectionTest()
      throws IllegalAccessException, NoSuchMethodException, InvocationTargetException
  {
    configProvider.inject(testProperties, configurator);
    validateEntries(configProvider.get().get());
    Assert.assertEquals(propertyValues.size(), assertions);
  }


}
