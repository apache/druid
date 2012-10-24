package com.metamx.druid.index.brita;

/**
*/
public class BooleanValueMatcher implements ValueMatcher
{
  private final boolean matches;

  public BooleanValueMatcher(final boolean matches) {
    this.matches = matches;
  }

  @Override
  public boolean matches()
  {
    return matches;
  }
}
