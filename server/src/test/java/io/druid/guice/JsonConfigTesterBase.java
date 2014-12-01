/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.guice;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.initialization.Initialization;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
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

  protected static String getPropertyKey(String fieldName){
    return String.format(
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
        String getter = String.format(
            "get%s%s",
            field.getName().substring(0, 1).toUpperCase(),
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
        if (field.getType().isAssignableFrom(String.class)) {
          propertyValues.put(propertyKey, UUID.randomUUID().toString());
        } else {
          propertyValues.put(propertyKey, field.get(fakeValues).toString());
        }
      }
    }
    testProperties.putAll(System.getProperties());
    testProperties.putAll(propertyValues);
    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Object>of(simpleJsonConfigModule)
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
