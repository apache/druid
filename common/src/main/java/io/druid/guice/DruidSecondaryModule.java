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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.introspect.GuiceAnnotationIntrospector;
import com.fasterxml.jackson.databind.introspect.GuiceInjectableValues;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import org.skife.config.ConfigurationObjectFactory;

import javax.validation.Validator;
import java.util.Properties;

/**
 */
public class DruidSecondaryModule implements Module
{
  private final Properties properties;
  private final ConfigurationObjectFactory factory;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final Validator validator;
  private final JsonConfigurator jsonConfigurator;

  @Inject
  public DruidSecondaryModule(
      Properties properties,
      ConfigurationObjectFactory factory,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      Validator validator,
      JsonConfigurator jsonConfigurator
  )
  {
    this.properties = properties;
    this.factory = factory;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.validator = validator;
    this.jsonConfigurator = jsonConfigurator;
  }

  @Override
  public void configure(Binder binder)
  {
    binder.install(new DruidGuiceExtensions());
    binder.bind(Properties.class).toInstance(properties);
    binder.bind(ConfigurationObjectFactory.class).toInstance(factory);
    binder.bind(ObjectMapper.class).to(Key.get(ObjectMapper.class, Json.class));
    binder.bind(Validator.class).toInstance(validator);
    binder.bind(JsonConfigurator.class).toInstance(jsonConfigurator);
  }

  @Provides @LazySingleton @Json
  public ObjectMapper getJsonMapper(final Injector injector)
  {
    setupJackson(injector, jsonMapper);
    return jsonMapper;
  }

  @Provides @LazySingleton @Smile
  public ObjectMapper getSmileMapper(Injector injector)
  {
    setupJackson(injector, smileMapper);
    return smileMapper;
  }

  private void setupJackson(Injector injector, final ObjectMapper mapper) {
    final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();

    mapper.setInjectableValues(new GuiceInjectableValues(injector));
    mapper.setAnnotationIntrospectors(
        new AnnotationIntrospectorPair(
            guiceIntrospector, mapper.getSerializationConfig().getAnnotationIntrospector()
        ),
        new AnnotationIntrospectorPair(
            guiceIntrospector, mapper.getDeserializationConfig().getAnnotationIntrospector()
        )
    );
  }
}
