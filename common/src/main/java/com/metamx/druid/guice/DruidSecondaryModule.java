package com.metamx.druid.guice;

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
import com.metamx.druid.guice.annotations.Json;
import com.metamx.druid.guice.annotations.Smile;
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
