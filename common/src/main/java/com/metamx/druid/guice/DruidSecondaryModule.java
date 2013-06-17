package com.metamx.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.metamx.druid.jackson.Json;
import com.metamx.druid.jackson.Smile;
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
    binder.bind(ObjectMapper.class).annotatedWith(Json.class).toInstance(jsonMapper);
    binder.bind(ObjectMapper.class).annotatedWith(Smile.class).toInstance(smileMapper);
    binder.bind(Validator.class).toInstance(validator);
    binder.bind(JsonConfigurator.class).toInstance(jsonConfigurator);
  }
}
