/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.guice;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.ProvisionException;
import com.google.inject.spi.Message;

import io.druid.java.util.common.logger.Logger;

import javax.validation.ConstraintViolation;
import javax.validation.ElementKind;
import javax.validation.Path;
import javax.validation.Validator;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 */
public class JsonConfigurator
{
  private static final Logger log = new Logger(JsonConfigurator.class);

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
    verifyClazzIsConfigurable(clazz);

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
            modifiedPropValue = jsonMapper.writeValueAsString(propValue);
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

    final T config;
    try {
      config = jsonMapper.convertValue(jsonMap, clazz);
    }
    catch (IllegalArgumentException e) {
      throw new ProvisionException(
          String.format("Problem parsing object at prefix[%s]: %s.", propertyPrefix, e.getMessage()), e
      );
    }

    final Set<ConstraintViolation<T>> violations = validator.validate(config);
    if (!violations.isEmpty()) {
      List<String> messages = Lists.newArrayList();

      for (ConstraintViolation<T> violation : violations) {
        String path = "";
        try {
          Class<?> beanClazz = violation.getRootBeanClass();
          final Iterator<Path.Node> iter = violation.getPropertyPath().iterator();
          while (iter.hasNext()) {
            Path.Node next = iter.next();
            if (next.getKind() == ElementKind.PROPERTY) {
              final String fieldName = next.getName();
              final Field theField = beanClazz.getDeclaredField(fieldName);

              if (theField.getAnnotation(JacksonInject.class) != null) {
                path = String.format(" -- Injected field[%s] not bound!?", fieldName);
                break;
              }

              JsonProperty annotation = theField.getAnnotation(JsonProperty.class);
              final boolean noAnnotationValue = annotation == null || Strings.isNullOrEmpty(annotation.value());
              final String pathPart = noAnnotationValue ? fieldName : annotation.value();
              if (path.isEmpty()) {
                path += pathPart;
              }
              else {
                path += "." + pathPart;
              }
            }
          }
        }
        catch (NoSuchFieldException e) {
          throw Throwables.propagate(e);
        }

        messages.add(String.format("%s - %s", path, violation.getMessage()));
      }

      throw new ProvisionException(
          Iterables.transform(
              messages,
              new Function<String, Message>()
              {
                @Override
                public Message apply(String input)
                {
                  return new Message(String.format("%s%s", propertyBase, input));
                }
              }
          )
      );
    }

    log.info("Loaded class[%s] from props[%s] as [%s]", clazz, propertyBase, config);

    return config;
  }

  private <T> void verifyClazzIsConfigurable(Class<T> clazz)
  {
    final List<BeanPropertyDefinition> beanDefs = jsonMapper.getSerializationConfig()
                                                              .introspect(jsonMapper.constructType(clazz))
                                                              .findProperties();
    for (BeanPropertyDefinition beanDef : beanDefs) {
      final AnnotatedField field = beanDef.getField();
      if (field == null || !field.hasAnnotation(JsonProperty.class)) {
        throw new ProvisionException(
            String.format(
                "JsonConfigurator requires Jackson-annotated Config objects to have field annotations. %s doesn't",
                clazz
            )
        );
      }
    }
  }
}
