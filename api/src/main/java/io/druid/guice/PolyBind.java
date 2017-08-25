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

import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.ProvisionException;
import com.google.inject.TypeLiteral;
import com.google.inject.binder.ScopedBindingBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.util.Types;
import io.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.lang.reflect.ParameterizedType;
import java.util.Map;
import java.util.Properties;

/**
 * Provides the ability to create "polymorphic" bindings.  Where the polymorphism is actually just making a decision
 * based on a value in a Properties.
 *
 * The workflow is that you first create a choice by calling createChoice().  Then you create options using the binder
 * returned by the optionBinder() method.  Multiple different modules can call optionBinder and all options will be
 * reflected at injection time as long as equivalent interface Key objects are passed into the various methods.
 */
public class PolyBind
{
  /**
   * Sets up a "choice" for the injector to resolve at injection time.
   *
   * @param binder the binder for the injector that is being configured
   * @param property the property that will be checked to determine the implementation choice
   * @param interfaceKey the interface that will be injected using this choice
   * @param defaultKey the default instance to be injected if the property doesn't match a choice.  Can be null
   * @param <T> interface type
   * @return A ScopedBindingBuilder so that scopes can be added to the binding, if required.
   */
  public static <T> ScopedBindingBuilder createChoice(
      Binder binder,
      String property,
      Key<T> interfaceKey,
      @Nullable Key<? extends T> defaultKey
  )
  {
    ConfiggedProvider<T> provider = new ConfiggedProvider<>(interfaceKey, property, defaultKey, null);
    return binder.bind(interfaceKey).toProvider(provider);
  }

  /**
   * @deprecated use {@link #createChoiceWithDefault(Binder, String, Key, String)}
   * instead. {@code defaultKey} argument is ignored.
   */
  @Deprecated
  public static <T> ScopedBindingBuilder createChoiceWithDefault(
      Binder binder,
      String property,
      Key<T> interfaceKey,
      Key<? extends T> defaultKey,
      String defaultPropertyValue
  )
  {
    return createChoiceWithDefault(binder, property, interfaceKey, defaultPropertyValue);
  }

  /**
   * Sets up a "choice" for the injector to resolve at injection time.
   *
   * @param binder the binder for the injector that is being configured
   * @param property the property that will be checked to determine the implementation choice
   * @param interfaceKey the interface that will be injected using this choice
   * @param defaultPropertyValue the default property value to use if the property is not set.
   * @param <T> interface type
   * @return A ScopedBindingBuilder so that scopes can be added to the binding, if required.
   */
  public static <T> ScopedBindingBuilder createChoiceWithDefault(
      Binder binder,
      String property,
      Key<T> interfaceKey,
      String defaultPropertyValue
  )
  {
    Preconditions.checkNotNull(defaultPropertyValue);
    ConfiggedProvider<T> provider = new ConfiggedProvider<>(interfaceKey, property, null, defaultPropertyValue);
    return binder.bind(interfaceKey).toProvider(provider);
  }

  /**
   * Binds an option for a specific choice.  The choice must already be registered on the injector for this to work.
   *
   * @param binder the binder for the injector that is being configured
   * @param interfaceKey the interface that will have an option added to it.  This must equal the
   *                     Key provided to createChoice
   * @param <T> interface type
   * @return A MapBinder that can be used to create the actual option bindings.
   */
  public static <T> MapBinder<String, T> optionBinder(Binder binder, Key<T> interfaceKey)
  {
    final TypeLiteral<T> interfaceType = interfaceKey.getTypeLiteral();

    if (interfaceKey.getAnnotation() != null) {
      return MapBinder.newMapBinder(
          binder, TypeLiteral.get(String.class), interfaceType, interfaceKey.getAnnotation()
      );
    } else if (interfaceKey.getAnnotationType() != null) {
      return MapBinder.newMapBinder(
          binder, TypeLiteral.get(String.class), interfaceType, interfaceKey.getAnnotationType()
      );
    } else {
      return MapBinder.newMapBinder(binder, TypeLiteral.get(String.class), interfaceType);
    }
  }

  static class ConfiggedProvider<T> implements Provider<T>
  {
    private final Key<T> key;
    private final String property;
    @Nullable
    private final Key<? extends T> defaultKey;
    @Nullable
    private final String defaultPropertyValue;

    private Injector injector;
    private Properties props;

    ConfiggedProvider(
        Key<T> key,
        String property,
        @Nullable Key<? extends T> defaultKey,
        @Nullable String defaultPropertyValue
    )
    {
      this.key = key;
      this.property = property;
      this.defaultKey = defaultKey;
      this.defaultPropertyValue = defaultPropertyValue;
    }

    @Inject
    void configure(Injector injector, Properties props)
    {
      this.injector = injector;
      this.props = props;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T get()
    {
      final ParameterizedType mapType = Types.mapOf(
          String.class, Types.newParameterizedType(Provider.class, key.getTypeLiteral().getType())
      );

      final Map<String, Provider<T>> implsMap;
      if (key.getAnnotation() != null) {
        implsMap = (Map<String, Provider<T>>) injector.getInstance(Key.get(mapType, key.getAnnotation()));
      } else if (key.getAnnotationType() != null) {
        implsMap = (Map<String, Provider<T>>) injector.getInstance(Key.get(mapType, key.getAnnotation()));
      } else {
        implsMap = (Map<String, Provider<T>>) injector.getInstance(Key.get(mapType));
      }

      String implName = props.getProperty(property);
      if (implName == null) {
        if (defaultPropertyValue == null) {
          if (defaultKey == null) {
            throw new ProvisionException(StringUtils.format("Some value must be configured for [%s]", key));
          }
          return injector.getInstance(defaultKey);
        }
        implName = defaultPropertyValue;
      }
      final Provider<T> provider = implsMap.get(implName);

      if (provider == null) {
        throw new ProvisionException(
            StringUtils.format("Unknown provider[%s] of %s, known options[%s]", implName, key, implsMap.keySet())
        );
      }

      return provider.get();
    }
  }
}
