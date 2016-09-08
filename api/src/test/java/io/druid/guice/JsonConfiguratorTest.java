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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.druid.TestObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
  private final ObjectMapper mapper = new TestObjectMapper();
  private final Properties properties = new Properties();

  @Before
  public void setUp()
  {
    mapper.registerSubtypes(MappableObject.class);
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
        Class<T> beanType, String propertyName, Object value, Class<?>... groups
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
        new MappableObject("p1", ImmutableList.<String>of("p2")),
        new MappableObject("p1", ImmutableList.<String>of("p2"))
    );
    Assert.assertEquals(new MappableObject("p1", null), new MappableObject("p1", ImmutableList.<String>of()));
  }

  @Test
  public void testsimpleConfigurate() throws Exception
  {
    final JsonConfigurator configurator = new JsonConfigurator(mapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "prop1");
    properties.setProperty(PROP_PREFIX + "prop1List", "[\"prop2\"]");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assert.assertEquals("prop1", obj.prop1);
    Assert.assertEquals(ImmutableList.of("prop2"), obj.prop1List);
  }

  @Test
  public void testMissingConfigList()
  {
    final JsonConfigurator configurator = new JsonConfigurator(mapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "prop1");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assert.assertEquals("prop1", obj.prop1);
    Assert.assertEquals(ImmutableList.of(), obj.prop1List);
  }

  @Test
  public void testMissingConfig()
  {
    final JsonConfigurator configurator = new JsonConfigurator(mapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1List", "[\"prop2\"]");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assert.assertNull(obj.prop1);
    Assert.assertEquals(ImmutableList.of("prop2"), obj.prop1List);
  }

  @Test
  public void testQuotedConfig()
  {
    final JsonConfigurator configurator = new JsonConfigurator(mapper, validator);
    properties.setProperty(PROP_PREFIX + "prop1", "testing \"prop1\"");
    final MappableObject obj = configurator.configurate(properties, PROP_PREFIX, MappableObject.class);
    Assert.assertEquals("testing \"prop1\"", obj.prop1);
    Assert.assertEquals(ImmutableList.of(), obj.prop1List);
  }
}

class MappableObject
{
  @JsonProperty("prop1")
  final String prop1;
  @JsonProperty("prop1List")
  final List<String> prop1List;

  @JsonCreator
  protected MappableObject(
      @JsonProperty("prop1") final String prop1,
      @JsonProperty("prop1List") final List<String> prop1List
  )
  {
    this.prop1 = prop1;
    this.prop1List = prop1List == null ? ImmutableList.<String>of() : prop1List;
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
