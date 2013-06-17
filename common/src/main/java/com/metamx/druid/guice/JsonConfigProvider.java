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

package com.metamx.druid.guice;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.util.Types;

import java.util.Properties;

/**
 */
public class JsonConfigProvider<T> implements Provider<Supplier<T>>
{
  @SuppressWarnings("unchecked")
  public static <T> void bind(Binder binder, String propertyBase, Class<T> classToProvide)
  {
    binder.bind(Key.get(Types.newParameterizedType(Supplier.class, classToProvide)))
          .toProvider((Provider) of(propertyBase, classToProvide))
          .in(LazySingleton.class);
  }

  public static <T> JsonConfigProvider<T> of(String propertyBase, Class<T> classToProvide)
  {
    return new JsonConfigProvider<T>(propertyBase, classToProvide);
  }

  private final String propertyBase;
  private final Class<T> classToProvide;

  private Properties props;
  private JsonConfigurator configurator;

  public JsonConfigProvider(
      String propertyBase,
      Class<T> classToProvide
  )
  {
    this.propertyBase = propertyBase;
    this.classToProvide = classToProvide;
  }

  @Inject
  public void inject(
      Properties props,
      JsonConfigurator configurator
  )
  {
    this.props = props;
    this.configurator = configurator;
  }

  @Override
  public Supplier<T> get()
  {
    final T config = configurator.configurate(props, propertyBase, classToProvide);
    return Suppliers.ofInstance(config);
  }
}
