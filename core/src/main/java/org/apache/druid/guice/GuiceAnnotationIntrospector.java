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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.introspect.AnnotatedParameter;
import com.fasterxml.jackson.databind.introspect.NopAnnotationIntrospector;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import org.apache.druid.java.util.common.IAE;

import java.lang.annotation.Annotation;
import java.util.Map;

/**
 */
public class GuiceAnnotationIntrospector extends NopAnnotationIntrospector
{
  @Override
  public Object findInjectableValueId(AnnotatedMember m)
  {
    if (m.getAnnotation(JacksonInject.class) == null) {
      return null;
    }

    Annotation guiceAnnotation = null;
    for (Annotation annotation : m.annotations()) {
      if (annotation.annotationType().isAnnotationPresent(BindingAnnotation.class)) {
        guiceAnnotation = annotation;
        break;
      }
    }

    if (guiceAnnotation == null) {
      if (m instanceof AnnotatedMethod) {
        throw new IAE("Annotated methods don't work very well yet...");
      }
      return Key.get(m.getRawType());
    }
    return Key.get(m.getRawType(), guiceAnnotation);
  }

  /**
   * This method is used to find what property to ignore in deserialization. Jackson calls this method
   * per every class and every constructor parameter.
   *
   * This implementation returns a {@link JsonIgnoreProperties.Value#empty()} that allows empty names if
   * the parameters has the {@link JsonProperty} annotation. Otherwise, it returns
   * {@code JsonIgnoreProperties.Value.forIgnoredProperties("")} that does NOT allow empty names.
   * This behavior is to work around a bug in Jackson deserializer (see the below comment for details) and
   * can be removed in the future after the bug is fixed.
   * For example, suppose a constructor like below:
   *
   * <pre>{@code
   * @JsonCreator
   * public ClassWithJacksonInject(
   *   @JsonProperty("val") String val,
   *   @JacksonInject InjectedParameter injected
   * )
   * }</pre>
   *
   * During deserializing a JSON string into this class, this method will be called at least twice,
   * one for {@code val} and another for {@code injected}. It will return {@code Value.empty()} for {@code val},
   * while {Value.forIgnoredProperties("")} for {@code injected} because the later does not have {@code JsonProperty}.
   * As a result, {@code injected} will be ignored during deserialization since it has no name.
   */
  @Override
  public JsonIgnoreProperties.Value findPropertyIgnorals(Annotated ac)
  {
    // We should not allow empty names in any case. However, there is a known bug in Jackson deserializer
    // with ignorals (_arrayDelegateDeserializer is not copied when creating a contextual deserializer.
    // See https://github.com/FasterXML/jackson-databind/issues/3022 for more details), which makes array
    // deserialization failed even when the array is a valid field. To work around this bug, we return
    // an empty ignoral when the given Annotated is a parameter with JsonProperty that needs to be deserialized.
    // This is valid because every property with JsonProperty annoation should have a non-empty name.
    // We can simply remove the below check after the Jackson bug is fixed.
    //
    // This check should be fine for so-called delegate creators that have only one argument without
    // JsonProperty annotation, because this method is not even called for the argument of
    // delegate creators. I'm not 100% sure why it's not called, but guess it's because the argument
    // is some Java type that Jackson already knows how to deserialize. Since there is only one argument,
    // Jackson perhaps is able to just deserialize it without introspection.

    if (ac instanceof AnnotatedParameter) {
      final AnnotatedParameter ap = (AnnotatedParameter) ac;
      if (ap.hasAnnotation(JsonProperty.class)) {
        return JsonIgnoreProperties.Value.empty();
      }
    }

    // A map can have empty string keys e.g. https://github.com/apache/druid/issues/10859. By returning empty ignored
    // list for map, we can allow for empty string keys in a map. A nested class within map
    // can still be annotated with JacksonInject and still be non-deserializable from user input
    // Refer to {@link com.fasterxml.jackson.databind.deser.BasicDeserializerFactory.createMapDeserializer} for details
    // on how the ignored list is passed to map deserializer
    if (ac instanceof AnnotatedClass) {
      final AnnotatedClass aClass = (AnnotatedClass) ac;
      if (Map.class.isAssignableFrom(aClass.getAnnotated())) {
        return JsonIgnoreProperties.Value.empty();
      }
    }

    // We will allow serialization on empty properties. Properties marked with {@code @JacksonInject} are still
    // not serialized if there is no getter marked with {@code @JsonProperty}
    return JsonIgnoreProperties.Value.forIgnoredProperties("").withAllowGetters();
  }
}
