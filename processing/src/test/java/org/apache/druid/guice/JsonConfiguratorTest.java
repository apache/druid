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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import javax.validation.executable.ExecutableValidator;
import javax.validation.metadata.BeanDescriptor;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class JsonConfiguratorTest
{
  private static final String PROP_PREFIX = "test.property.prefix.";
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final Properties properties = new Properties();

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Before
  public void setUp()
  {
    jsonMapper.registerSubtypes(MappableObject.class);
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
    Assert.assertEquals(
        new MappableObject("p1", ImmutableList.of("p2"), "p2"),
        new MappableObject("p1", ImmutableList.of("p2"), "p2")
    );
    Assert.assertEquals(
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
    Assert.assertEquals("prop1", obj.prop1);
    Assert.assertEquals(ImmutableList.of("prop2"), obj.prop1List);
  }

  @Test
  public void testMissingConfigList()
  {
    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "prop1");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assert.assertEquals("prop1", obj.prop1);
    Assert.assertEquals(ImmutableList.of(), obj.prop1List);
  }

  @Test
  public void testMissingConfig()
  {
    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1List", "[\"prop2\"]");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assert.assertNull(obj.prop1);
    Assert.assertEquals(ImmutableList.of("prop2"), obj.prop1List);
  }

  @Test
  public void testQuotedConfig()
  {
    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "testing \"prop1\"");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assert.assertEquals("testing \"prop1\"", obj.prop1);
    Assert.assertEquals(ImmutableList.of(), obj.prop1List);
  }

  @Test
  public void testPropertyWithDot()
  {
    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop2.prop.2", "testing");
    properties.setProperty(PROP_PREFIX + "prop1", "prop1");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assert.assertEquals("testing", obj.prop2);
    Assert.assertEquals(ImmutableList.of(), obj.prop1List);
    Assert.assertEquals("prop1", obj.prop1);

  }

  @Test
  public void testPropertyInterpolation()
  {
    System.setProperty("my.property", "value1");
    List<String> list = ImmutableList.of("list", "of", "strings");
    environmentVariables.set("MY_VAR", "value2");

    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "${sys:my.property}");
    properties.setProperty(PROP_PREFIX + "prop1List", "${file:UTF-8:src/test/resources/list.json}");
    properties.setProperty(PROP_PREFIX + "prop2.prop.2", "${env:MY_VAR}");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assert.assertEquals(System.getProperty("my.property"), obj.prop1);
    Assert.assertEquals(list, obj.prop1List);
    Assert.assertEquals("value2", obj.prop2);
  }

  @Test
  public void testPropertyInterpolationInName()
  {
    System.setProperty("my.property", "value1");
    List<String> list = ImmutableList.of("list", "of", "strings");
    environmentVariables.set("MY_VAR", "value2");

    environmentVariables.set("SYS_PROP", "my.property");
    System.setProperty("json.path", "src/test/resources/list.json");
    environmentVariables.set("PROP2_NAME", "MY_VAR");

    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "${sys:${env:SYS_PROP}}");
    properties.setProperty(PROP_PREFIX + "prop1List", "${file:UTF-8:${sys:json.path}}");
    properties.setProperty(PROP_PREFIX + "prop2.prop.2", "${env:${env:PROP2_NAME}}");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assert.assertEquals(System.getProperty("my.property"), obj.prop1);
    Assert.assertEquals(list, obj.prop1List);
    Assert.assertEquals("value2", obj.prop2);
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
    Assert.assertEquals("value1", obj.prop1);
    Assert.assertEquals(list, obj.prop1List);
    Assert.assertEquals("value2", obj.prop2);
  }

  @Test
  public void testPropertyInterpolationUndefinedException()
  {
    final JsonConfigurator configurator = new JsonConfigurator(jsonMapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "${sys:my.property}");

    Assert.assertThrows(
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
