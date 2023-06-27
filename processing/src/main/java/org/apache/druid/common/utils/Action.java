package org.apache.druid.common.utils;

/**
 * A simple function with no input and no output.
 */
@FunctionalInterface
public interface Action
{
  void perform();
}
