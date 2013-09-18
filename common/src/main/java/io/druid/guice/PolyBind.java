/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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
    return binder.bind(interfaceKey).toProvider(new ConfiggedProvider<T>(interfaceKey, property, defaultKey));
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
    }
    else if (interfaceKey.getAnnotationType() != null) {
      return MapBinder.newMapBinder(
          binder, TypeLiteral.get(String.class), interfaceType, interfaceKey.getAnnotationType()
      );
    }
    else {
      return MapBinder.newMapBinder(binder, TypeLiteral.get(String.class), interfaceType);
    }
  }

  static class ConfiggedProvider<T> implements Provider<T>
  {
    private final Key<T> key;
    private final String property;
    private final Key<? extends T> defaultKey;

    private Injector injector;
    private Properties props;

    ConfiggedProvider(
        Key<T> key,
        String property,
        Key<? extends T> defaultKey
    )
    {
      this.key = key;
      this.property = property;
      this.defaultKey = defaultKey;
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
      }
      else if (key.getAnnotationType() != null) {
        implsMap = (Map<String, Provider<T>>) injector.getInstance(Key.get(mapType, key.getAnnotation()));
      }
      else {
        implsMap = (Map<String, Provider<T>>) injector.getInstance(Key.get(mapType));
      }

      final String implName = props.getProperty(property);
      final Provider<T> provider = implsMap.get(implName);

      if (provider == null) {
        if (defaultKey == null) {
          throw new ProvisionException(
              String.format("Unknown provider[%s] of %s, known options[%s]", implName, key, implsMap.keySet())
          );
        }
        return injector.getInstance(defaultKey);
      }

      return provider.get();
    }
  }
}
