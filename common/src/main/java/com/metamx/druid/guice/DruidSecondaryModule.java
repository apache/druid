package com.metamx.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.metamx.druid.jackson.Json;
import com.metamx.druid.jackson.Smile;
import org.skife.config.ConfigurationObjectFactory;

import java.util.Properties;

/**
 */
public class DruidSecondaryModule implements Module
{
  private final Properties properties;
  private final ConfigurationObjectFactory factory;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;

  @Inject
  public DruidSecondaryModule(
      Properties properties,
      ConfigurationObjectFactory factory,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper
  )
  {
    this.properties = properties;
    this.factory = factory;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
  }

  @Override
  public void configure(Binder binder)
  {
    binder.install(new DruidGuiceExtensions());
    binder.bind(Properties.class).toInstance(properties);
    binder.bind(ConfigurationObjectFactory.class).toInstance(factory);
    binder.bind(ObjectMapper.class).annotatedWith(Json.class).toInstance(jsonMapper);
    binder.bind(ObjectMapper.class).annotatedWith(Smile.class).toInstance(smileMapper);
  }
}
