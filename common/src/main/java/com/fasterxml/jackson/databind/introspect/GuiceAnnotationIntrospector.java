/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.fasterxml.jackson.databind.introspect;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.metamx.common.IAE;

import java.lang.annotation.Annotation;

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
    for (Annotation annotation : m.getAllAnnotations()._annotations.values()) {
      if (annotation.annotationType().isAnnotationPresent(BindingAnnotation.class)) {
        guiceAnnotation = annotation;
        break;
      }
    }

    if (guiceAnnotation == null) {
      if (m instanceof AnnotatedMethod) {
        throw new IAE("Annotated methods don't work very well yet...");
      }
      return Key.get(m.getGenericType());
    }
    return Key.get(m.getGenericType(), guiceAnnotation);
  }
}
