package com.metamx.druid.index.brita;

import com.google.common.base.Predicate;

/**
 */
public interface ValueMatcherFactory
{
  public ValueMatcher makeValueMatcher(String dimension, String value);
  public ValueMatcher makeValueMatcher(String dimension, Predicate<String> value);
}
