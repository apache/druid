/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

  @Inject
  public DruidSecondaryModule(
      Properties properties,
      ConfigurationObjectFactory factory,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      Validator validator
  )
  {
    this.properties = properties;
    this.factory = factory;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.validator = validator;
  }

  @Override
  public void configure(Binder binder)
  {
    binder.install(new DruidGuiceExtensions());
    binder.bind(Properties.class).toInstance(properties);
    binder.bind(ConfigurationObjectFactory.class).toInstance(factory);
    binder.bind(ObjectMapper.class).to(Key.get(ObjectMapper.class, Json.class));
    binder.bind(Validator.class).toInstance(validator);
    binder.bind(JsonConfigurator.class);
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
