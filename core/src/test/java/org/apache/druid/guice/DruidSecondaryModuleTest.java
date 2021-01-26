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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;
import javax.validation.Validation;
import javax.validation.Validator;
import java.util.List;
import java.util.Properties;

@RunWith(Enclosed.class)
public class DruidSecondaryModuleTest
{
  private static final String PROPERTY_NAME = "druid.injected.val";
  private static final String PROPERTY_VALUE = "this is the legit val";

  public static class ConstructorWithJacksonInjectTest
  {
    @Test
    public void testInjectWithAnEmptyPropertyNotOverrideInjection() throws JsonProcessingException
    {
      final Properties props = new Properties();
      props.setProperty(PROPERTY_NAME, PROPERTY_VALUE);

      final Injector injector = makeInjectorWithProperties(props);
      final ObjectMapper mapper = makeObjectMapper(injector);
      final String json = "{\"test\": \"this is an injection test\", \"\": \"nice try\" }";
      final ClassWithJacksonInject object = mapper.readValue(json, ClassWithJacksonInject.class);
      Assert.assertEquals("this is an injection test", object.test);
      Assert.assertEquals(PROPERTY_VALUE, object.injected.val);
    }

    @Test
    public void testInjectNormal() throws JsonProcessingException
    {
      final Properties props = new Properties();
      props.setProperty(PROPERTY_NAME, PROPERTY_VALUE);

      final Injector injector = makeInjectorWithProperties(props);
      final ObjectMapper mapper = makeObjectMapper(injector);
      final String json = "{\"test\": \"this is an injection test\" }";
      final ClassWithJacksonInject object = mapper.readValue(json, ClassWithJacksonInject.class);
      Assert.assertEquals("this is an injection test", object.test);
      Assert.assertEquals(PROPERTY_VALUE, object.injected.val);
    }

    @Test
    public void testInjectClassWithEmptyProperty() throws JsonProcessingException
    {
      final Properties props = new Properties();
      props.setProperty(PROPERTY_NAME, PROPERTY_VALUE);

      final Injector injector = makeInjectorWithProperties(props);
      final ObjectMapper mapper = makeObjectMapper(injector);
      final String json = "{\"test\": \"this is an injection test\", \"\": \"nice try\" }";
      final ClassWithEmptyProperty object = mapper.readValue(json, ClassWithEmptyProperty.class);
      Assert.assertEquals("this is an injection test", object.test);
      Assert.assertEquals(PROPERTY_VALUE, object.injected.val);
    }

    private static class ClassWithJacksonInject
    {
      private final String test;

      private InjectedParameter injected;

      @JsonCreator
      public ClassWithJacksonInject(
          @JsonProperty("test") String test,
          @JacksonInject InjectedParameter injected
      )
      {
        this.test = test;
        this.injected = injected;
      }

      @JsonProperty
      public String getTest()
      {
        return test;
      }
    }

    private static class ClassWithEmptyProperty
    {
      private final String test;

      private InjectedParameter injected;

      @JsonCreator
      public ClassWithEmptyProperty(
          @JsonProperty("test") String test,
          @JacksonInject @JsonProperty("") InjectedParameter injected
      )
      {
        this.test = test;
        this.injected = injected;
      }

      @JsonProperty
      public String getTest()
      {
        return test;
      }
    }
  }

  public static class ConstructorWithoutJacksonInjectTest
  {
    @Test
    public void testInjectionWithEmptyPropertyName() throws JsonProcessingException
    {
      final Properties props = new Properties();

      final Injector injector = makeInjectorWithProperties(props);
      final ObjectMapper mapper = makeObjectMapper(injector);
      final String json = "[\"this is\", \"an injection test\"]";
      final ClassWithConstructorOfEmptyName object = mapper.readValue(json, ClassWithConstructorOfEmptyName.class);
      Assert.assertEquals(ImmutableList.of("this is", "an injection test"), object.getTest());
    }

    @Test
    public void testInjectEmptyListWithEmptyPropertyName() throws JsonProcessingException
    {
      final Properties props = new Properties();

      final Injector injector = makeInjectorWithProperties(props);
      final ObjectMapper mapper = makeObjectMapper(injector);
      final String json = "[]";
      final ClassWithConstructorOfEmptyName object = mapper.readValue(json, ClassWithConstructorOfEmptyName.class);
      Assert.assertEquals(ImmutableList.of(), object.getTest());
    }

    @Test
    public void testInjectClassWithFactoryMethodOfEmptyPropertyName() throws JsonProcessingException
    {
      final Properties props = new Properties();

      final Injector injector = makeInjectorWithProperties(props);
      final ObjectMapper mapper = makeObjectMapper(injector);
      final String json = "[\"this is\", \"an injection test\"]";
      final ClassWithFactoryMethodOfEmptyName object = mapper.readValue(json, ClassWithFactoryMethodOfEmptyName.class);
      Assert.assertEquals(ImmutableList.of("this is", "an injection test"), object.getTest());
    }

    @Test
    public void testInjectEmptyListToClassWithFactoryMethodOfEmptyPropertyName() throws JsonProcessingException
    {
      final Properties props = new Properties();

      final Injector injector = makeInjectorWithProperties(props);
      final ObjectMapper mapper = makeObjectMapper(injector);
      final String json = "[]";
      final ClassWithFactoryMethodOfEmptyName object = mapper.readValue(json, ClassWithFactoryMethodOfEmptyName.class);
      Assert.assertEquals(ImmutableList.of(), object.getTest());
    }

    @Test
    public void testInjectClassOfEmptyConstructor() throws JsonProcessingException
    {
      final Properties props = new Properties();
      props.setProperty(PROPERTY_NAME, PROPERTY_VALUE);

      final Injector injector = makeInjectorWithProperties(props);
      final ObjectMapper mapper = makeObjectMapper(injector);
      final String json = "{}";
      final ClassOfEmptyConstructor object = mapper.readValue(json, ClassOfEmptyConstructor.class);
      Assert.assertEquals("empty constructor", object.val);
    }

    private static class ClassWithConstructorOfEmptyName
    {
      private final List<String> test;

      @JsonCreator
      public ClassWithConstructorOfEmptyName(List<String> test)
      {
        this.test = test;
      }

      @JsonValue
      public List<String> getTest()
      {
        return test;
      }
    }

    private static class ClassWithFactoryMethodOfEmptyName
    {
      private final List<String> test;

      @JsonCreator
      public static ClassWithFactoryMethodOfEmptyName create(List<String> test)
      {
        return new ClassWithFactoryMethodOfEmptyName(test);
      }

      private ClassWithFactoryMethodOfEmptyName(List<String> test)
      {
        this.test = test;
      }

      @JsonValue
      public List<String> getTest()
      {
        return test;
      }
    }

    private static class ClassOfEmptyConstructor
    {
      private final String val;

      @JsonCreator
      public ClassOfEmptyConstructor()
      {
        this.val = "empty constructor";
      }
    }
  }

  public static class ClassOfMultipleJsonCreatorsTest
  {
    @Test
    public void testDeserializeUsingMultiArgumentsConstructor() throws JsonProcessingException
    {
      final Properties props = new Properties();
      props.setProperty(PROPERTY_NAME, PROPERTY_VALUE);

      final Injector injector = makeInjectorWithProperties(props);
      final ObjectMapper mapper = makeObjectMapper(injector);
      final String json = "{\"val\": \"this is an injection test\", \"valLen\": 5, \"\": \"nice try\" }";
      final ClassOfMultipleJsonCreators object = mapper.readValue(json, ClassOfMultipleJsonCreators.class);
      Assert.assertEquals("this is an injection test", object.val);
      Assert.assertEquals(5, object.valLen);
      Assert.assertNotNull(object.injected);
      Assert.assertEquals(PROPERTY_VALUE, object.injected.val);
    }

    @Test
    public void testDeserializeUsingDelegateConstructor() throws JsonProcessingException
    {
      final Properties props = new Properties();
      props.setProperty(PROPERTY_NAME, PROPERTY_VALUE);

      final Injector injector = makeInjectorWithProperties(props);
      final ObjectMapper mapper = makeObjectMapper(injector);
      final String json = "\"this is an injection test\"";
      final ClassOfMultipleJsonCreators object = mapper.readValue(json, ClassOfMultipleJsonCreators.class);
      Assert.assertEquals("this is an injection test", object.val);
      Assert.assertEquals(object.val.length(), object.valLen);
      Assert.assertNull(object.injected);
    }

    private static class ClassOfMultipleJsonCreators
    {
      private final String val;
      private final int valLen;
      @Nullable
      private final InjectedParameter injected;

      @JsonCreator
      public ClassOfMultipleJsonCreators(
          @JsonProperty("val") String val,
          @JsonProperty("valLen") int valLen,
          @JacksonInject @Nullable InjectedParameter injected
      )
      {
        this.val = val;
        this.valLen = valLen;
        this.injected = injected;
      }

      @JsonCreator
      public static ClassOfMultipleJsonCreators create(String val)
      {
        return new ClassOfMultipleJsonCreators(val, val.length(), null);
      }

      @JsonProperty
      public String getVal()
      {
        return val;
      }

      @JsonProperty
      public int getValLen()
      {
        return valLen;
      }
    }
  }

  private static class InjectedParameter
  {
    @JsonProperty
    private String val;
  }

  private static Injector makeInjectorWithProperties(final Properties props)
  {
    return Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            (Module) binder -> {
              binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
              binder.bind(JsonConfigurator.class).in(LazySingleton.class);
              binder.bind(Properties.class).toInstance(props);
              JsonConfigProvider.bind(binder, "druid.injected", InjectedParameter.class);
            }
        )
    );
  }

  private static ObjectMapper makeObjectMapper(Injector injector)
  {
    final ObjectMapper mapper = new ObjectMapper();
    DruidSecondaryModule.setupJackson(injector, mapper);
    return mapper;
  }
}
