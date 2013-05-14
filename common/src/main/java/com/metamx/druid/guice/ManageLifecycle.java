package com.metamx.druid.guice;

import com.google.inject.ScopeAnnotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Marks the object to be managed by {@link com.metamx.common.lifecycle.Lifecycle}
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RUNTIME)
@ScopeAnnotation
public @interface ManageLifecycle
{
}
