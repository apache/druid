package com.fasterxml.jackson.databind.introspect;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;

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
      return Key.get(m.getGenericType());
    }
    return Key.get(m.getGenericType(), guiceAnnotation);
  }
}
