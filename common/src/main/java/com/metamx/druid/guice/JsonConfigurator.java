package com.metamx.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.ProvisionException;
import com.google.inject.spi.Message;
import com.metamx.common.logger.Logger;

import javax.annotation.Nullable;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 */
public class JsonConfigurator
{
  private static final Logger log = new Logger(JsonConfigurator.class);

  private static final Joiner JOINER = Joiner.on("; ");

  private final ObjectMapper jsonMapper;
  private final Validator validator;

  @Inject
  public JsonConfigurator(
      ObjectMapper jsonMapper,
      Validator validator
  )
  {
    this.jsonMapper = jsonMapper;
    this.validator = validator;
  }

  public <T> T configurate(Properties props, String propertyPrefix, Class<T> clazz) throws ProvisionException
  {
    // Make it end with a period so we only include properties with sub-object thingies.
    final String propertyBase = propertyPrefix.endsWith(".") ? propertyPrefix : propertyPrefix + ".";

    Map<String, Object> jsonMap = Maps.newHashMap();
    for (String prop : props.stringPropertyNames()) {
      if (prop.startsWith(propertyBase)) {
        final String propValue = props.getProperty(prop);
        Object value;
        try {
          // If it's a String Jackson wants it to be quoted, so check if it's not an object or array and quote.
          String modifiedPropValue = propValue;
          if (! (modifiedPropValue.startsWith("[") || modifiedPropValue.startsWith("{"))) {
            modifiedPropValue = String.format("\"%s\"", modifiedPropValue);
          }
          value = jsonMapper.readValue(modifiedPropValue, Object.class);
        }
        catch (IOException e) {
          log.info(e, "Unable to parse [%s]=[%s] as a json object, using as is.", prop, propValue);
          value = propValue;
        }

        jsonMap.put(prop.substring(propertyBase.length()), value);
      }
    }

    final T config = jsonMapper.convertValue(jsonMap, clazz);

    final Set<ConstraintViolation<T>> violations = validator.validate(config);
    if (!violations.isEmpty()) {
      List<String> messages = Lists.newArrayList();

      for (ConstraintViolation<T> violation : violations) {
        messages.add(String.format("%s - %s", violation.getPropertyPath().toString(), violation.getMessage()));
      }

      throw new ProvisionException(
          Iterables.transform(
              messages,
              new Function<String, Message>()
              {
                @Nullable
                @Override
                public Message apply(@Nullable String input)
                {
                  return new Message(String.format("%s%s", propertyBase, input));
                }
              }
          )
      );
    }

    log.info("Loaded class[%s] as [%s]", clazz, config);

    return config;
  }
}
