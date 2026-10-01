/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.guice;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import jakarta.validation.executable.ExecutableValidator;
import jakarta.validation.metadata.BeanDescriptor;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class JsonConfiguratorTest
{
  private static final String PROP_PREFIX = "test.property.prefix.";
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final Properties properties = new Properties();
  private final Map<String, String> originalSystemProperties = new HashMap<>();

  @BeforeEach
  public void setUp()
  {
    jsonMapper.registerSubtypes(MappableObject.class);
    for (String property : ImmutableList.of("my.property", "json.path", "sys.prop.name", "env.var.name")) {
      originalSystemProperties.put(property, System.getProperty(property));
    }
  }

  @AfterEach
  public void tearDown()
  {
    originalSystemProperties.forEach((property, value) -> {
      if (value == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, value);
      }
    });
  }

  final Validator validator = new Validator()
  {
    @Override
    public <T> Set<ConstraintViolation<T>> validate(T object, Class<?>... groups)
    {
      return ImmutableSet.of();
    }

    @Override
    public <T> Set<ConstraintViolation<T>> validateProperty(T object, String propertyName, Class<?>... groups)
    {
      return ImmutableSet.of();
    }

    @Override
    public <T> Set<ConstraintViolation<T>> validateValue(
        Class<T> beanType,
        String propertyName,
        Object value,
        Class<?>... groups
    )
    {
      return ImmutableSet.of();
    }

    @Override
    public BeanDescriptor getConstraintsForClass(Class<?> clazz)
    {
      return null;
    }

    @Override
    public <T> T unwrap(Class<T> type)
    {
      return null;
    }

    @Override
    public ExecutableValidator forExecutables()
    {
      return null;
    }
  };

  @Test
  public void testTest()
  {
    Assertions.assertEquals(
        new MappableObject("p1", ImmutableList.of("p2"), "p2"),
        new MappableObject("p1", ImmutableList.of("p2"), "p2")
    );
    Assertions.assertEquals(
        new MappableObject("p1", null, null),
        new MappableObject("p1", ImmutableList.of(), null)
    );
  }

  @Test
  public void testSimpleConfigurate()
  {
    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "prop1");
    properties.setProperty(PROP_PREFIX + "prop1List", "[\"prop2\"]");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assertions.assertEquals("prop1", obj.prop1);
    Assertions.assertEquals(ImmutableList.of("prop2"), obj.prop1List);
  }

  @Test
  public void testMissingConfigList()
  {
    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "prop1");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assertions.assertEquals("prop1", obj.prop1);
    Assertions.assertEquals(ImmutableList.of(), obj.prop1List);
  }

  @Test
  public void testMissingConfig()
  {
    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1List", "[\"prop2\"]");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assertions.assertNull(obj.prop1);
    Assertions.assertEquals(ImmutableList.of("prop2"), obj.prop1List);
  }

  @Test
  public void testQuotedConfig()
  {
    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "testing \"prop1\"");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assertions.assertEquals("testing \"prop1\"", obj.prop1);
    Assertions.assertEquals(ImmutableList.of(), obj.prop1List);
  }

  @Test
  public void testPropertyWithDot()
  {
    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop2.prop.2", "testing");
    properties.setProperty(PROP_PREFIX + "prop1", "prop1");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assertions.assertEquals("testing", obj.prop2);
    Assertions.assertEquals(ImmutableList.of(), obj.prop1List);
    Assertions.assertEquals("prop1", obj.prop1);

  }

  @Test
  public void testPropertyInterpolation()
  {
    System.setProperty("my.property", "value1");
    final List<String> list = ImmutableList.of("list", "of", "strings");

    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "${sys:my.property}");
    properties.setProperty(PROP_PREFIX + "prop1List", "${file:UTF-8:src/test/resources/list.json}");
    properties.setProperty(PROP_PREFIX + "prop2.prop.2", "${env:PATH}");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assertions.assertEquals(System.getProperty("my.property"), obj.prop1);
    Assertions.assertEquals(list, obj.prop1List);
    Assertions.assertEquals(System.getenv("PATH"), obj.prop2);
  }

  @Test
  public void testPropertyInterpolationInName()
  {
    System.setProperty("my.property", "value1");
    final List<String> list = ImmutableList.of("list", "of", "strings");

    System.setProperty("sys.prop.name", "my.property");
    System.setProperty("json.path", "src/test/resources/list.json");
    System.setProperty("env.var.name", "PATH");

    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "${sys:${sys:sys.prop.name}}");
    properties.setProperty(PROP_PREFIX + "prop1List", "${file:UTF-8:${sys:json.path}}");
    properties.setProperty(PROP_PREFIX + "prop2.prop.2", "${env:${sys:env.var.name}}");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assertions.assertEquals(System.getProperty("my.property"), obj.prop1);
    Assertions.assertEquals(list, obj.prop1List);
    Assertions.assertEquals(System.getenv("PATH"), obj.prop2);
  }

  @Test
  public void testPropertyInterpolationFallback()
  {
    List<String> list = ImmutableList.of("list", "of", "strings");

    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "${sys:my.property:-value1}");
    properties.setProperty(PROP_PREFIX + "prop1List", "${unknown:-[\"list\", \"of\", \"strings\"]}");
    properties.setProperty(PROP_PREFIX + "prop2.prop.2", "${MY_VAR:-value2}");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assertions.assertEquals("value1", obj.prop1);
    Assertions.assertEquals(list, obj.prop1List);
    Assertions.assertEquals("value2", obj.prop2);
  }

  @Test
  public void testPropertyInterpolationUndefinedException()
  {
    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "${sys:my.property}");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> configurator.configurate(properties, PROP_PREFIX, MappableObject.class)
    );
  }
}

class MappableObject
{
  @JsonProperty("prop1")
  final String prop1;
  @JsonProperty("prop1List")
  final List<String> prop1List;
  @JsonProperty("prop2.prop.2")
  final String prop2;

  @JsonCreator
  protected MappableObject(
      @JsonProperty("prop1") final String prop1,
      @JsonProperty("prop1List") final List<String> prop1List,
      @JsonProperty("prop2.prop.2") final String prop2
  )
  {
    this.prop1 = prop1;
    this.prop1List = prop1List == null ? ImmutableList.of() : prop1List;
    this.prop2 = prop2;
  }


  @JsonProperty
  public List<String> getProp1List()
  {
    return prop1List;
  }

  @JsonProperty
  public String getProp1()
  {
    return prop1;
  }

  @JsonProperty
  public String getProp2()
  {
    return prop2;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MappableObject object = (MappableObject) o;

    if (prop1 != null ? !prop1.equals(object.prop1) : object.prop1 != null) {
      return false;
    }
    return prop1List != null ? prop1List.equals(object.prop1List) : object.prop1List == null;

  }

  @Override
  public int hashCode()
  {
    int result = prop1 != null ? prop1.hashCode() : 0;
    result = 31 * result + (prop1List != null ? prop1List.hashCode() : 0);
    return result;
  }
}
