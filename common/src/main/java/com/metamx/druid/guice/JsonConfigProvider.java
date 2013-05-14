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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.metamx.common.IAE;
import com.metamx.common.ISE;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 */
public class JsonConfigProvider<T> implements Provider<Supplier<T>>
{

  private static final Joiner JOINER = Joiner.on(", ");

  public static <T> void bind(Binder binder, String propertyBase, Class<T> classToProvide)
  {
    binder.bind(new TypeLiteral<Supplier<T>>(){}).toProvider(of(propertyBase, classToProvide)).in(DruidScopes.SINGLETON);
  }

  public static <T> JsonConfigProvider<T> of(String propertyBase, Class<T> classToProvide)
  {
    return new JsonConfigProvider<T>(propertyBase, classToProvide);
  }

  private final String propertyBase;
  private final Class<T> classToProvide;

  private Supplier<T> supplier;

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
      ObjectMapper jsonMapper,
      Validator validator
  )
  {
    Map<String, Object> jsonMap = Maps.newHashMap();
    for (String prop : props.stringPropertyNames()) {
      if (prop.startsWith(propertyBase)) {
        final String propValue = props.getProperty(prop);
        try {
          jsonMap.put(prop.substring(propertyBase.length()), jsonMapper.readValue(propValue, Object.class));
        }
        catch (IOException e) {
          throw new IAE("Unable to parse an object out of prop[%s]=[%s]", prop, propValue);
        }
      }
    }

    final T config = jsonMapper.convertValue(jsonMap, classToProvide);

    final Set<ConstraintViolation<T>> violations = validator.validate(config);
    if (!violations.isEmpty()) {
      List<String> messages = Lists.newArrayList();

      for (ConstraintViolation<T> violation : violations) {
        messages.add(String.format("%s - %s", violation.getPropertyPath().toString(), violation.getMessage()));
      }

      throw new ISE("Configuration violations[%s]", JOINER.join(messages));
    }

    this.supplier = new Supplier<T>()
    {
      @Override
      public T get()
      {
        return config;
      }
    };
  }

  @Override
  public Supplier<T> get()
  {
    return supplier;
  }
}
