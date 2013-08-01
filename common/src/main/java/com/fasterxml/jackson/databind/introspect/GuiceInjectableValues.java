package com.fasterxml.jackson.databind.introspect;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.google.inject.BindingAnnotation;
import com.google.inject.Injector;
import com.google.inject.Key;

import java.lang.annotation.Annotation;

/**
*/
public class GuiceInjectableValues extends InjectableValues
{
  private final Injector injector;

  public GuiceInjectableValues(Injector injector) {this.injector = injector;}

  @Override
  public Object findInjectableValue(
      Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
  )
  {
    final AnnotatedMember member = forProperty.getMember();
    Annotation guiceAnnotation = null;
    for (Annotation annotation : member.getAllAnnotations()._annotations.values()) {
      if (annotation.annotationType().isAnnotationPresent(BindingAnnotation.class)) {
        guiceAnnotation = annotation;
        break;
      }
    }

    final Key<?> key;
    if (guiceAnnotation == null) {
      key = Key.get(forProperty.getMember().getGenericType());
    }
    else {
      key = Key.get(forProperty.getMember().getGenericType(), guiceAnnotation);
    }
    return injector.getInstance(key);
  }
}
