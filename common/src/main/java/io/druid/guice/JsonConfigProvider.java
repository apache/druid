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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.util.Types;

import java.lang.annotation.Annotation;
import java.util.Properties;

/**
 */
public class JsonConfigProvider<T> implements Provider<Supplier<T>>
{
  @SuppressWarnings("unchecked")
  public static <T> void bind(Binder binder, String propertyBase, Class<T> classToProvide)
  {
    bind(
        binder,
        propertyBase,
        classToProvide,
        Key.get(classToProvide),
        (Key) Key.get(Types.newParameterizedType(Supplier.class, classToProvide))
    );
  }

  @SuppressWarnings("unchecked")
  public static <T> void bind(Binder binder, String propertyBase, Class<T> classToProvide, Annotation annotation)
  {
    bind(
        binder,
        propertyBase,
        classToProvide,
        Key.get(classToProvide, annotation),
        (Key) Key.get(Types.newParameterizedType(Supplier.class, classToProvide), annotation)
    );
  }

  @SuppressWarnings("unchecked")
  public static <T> void bind(
      Binder binder,
      String propertyBase,
      Class<T> classToProvide,
      Class<? extends Annotation> annotation
  )
  {
    bind(
        binder,
        propertyBase,
        classToProvide,
        Key.get(classToProvide, annotation),
        (Key) Key.get(Types.newParameterizedType(Supplier.class, classToProvide), annotation)
    );
  }

  @SuppressWarnings("unchecked")
  public static <T> void bind(
      Binder binder,
      String propertyBase,
      Class<T> clazz,
      Key<T> instanceKey,
      Key<Supplier<T>> supplierKey
  )
  {
    binder.bind(supplierKey).toProvider((Provider) of(propertyBase, clazz)).in(LazySingleton.class);
    binder.bind(instanceKey).toProvider(new SupplierProvider<T>(supplierKey));
  }

  public static <T> JsonConfigProvider<T> of(String propertyBase, Class<T> classToProvide)
  {
    return new JsonConfigProvider<T>(propertyBase, classToProvide);
  }

  private final String propertyBase;
  private final Class<T> classToProvide;

  private Properties props;
  private JsonConfigurator configurator;

  private Supplier<T> retVal = null;

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
    if (retVal != null) {
      return retVal;
    }

    try {
      final T config = configurator.configurate(props, propertyBase, classToProvide);
      retVal = Suppliers.ofInstance(config);
    }
    catch (RuntimeException e) {
      // When a runtime exception gets thrown out, this provider will get called again if the object is asked for again.
      // This will have the same failed result, 'cause when it's called no parameters will have actually changed.
      // Guice will then report the same error multiple times, which is pretty annoying. Cache a null supplier and
      // return that instead.  This is technically enforcing a singleton, but such is life.
      retVal = Suppliers.ofInstance(null);
      throw e;
    }
    return retVal;
  }
}
