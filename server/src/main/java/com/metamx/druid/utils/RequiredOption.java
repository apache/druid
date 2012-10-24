package com.metamx.druid.utils;

import org.apache.commons.cli.Option;

/**
 */
public class RequiredOption extends Option
{
  public RequiredOption(String opt, String description)
      throws IllegalArgumentException
  {
    super(opt, description);
    setRequired(true);
  }

  public RequiredOption(String opt, boolean hasArg, String description)
      throws IllegalArgumentException
  {
    super(opt, hasArg, description);
    setRequired(true);
  }

  public RequiredOption(String opt, String longOpt, boolean hasArg, String description)
      throws IllegalArgumentException
  {
    super(opt, longOpt, hasArg, description);
    setRequired(true);
  }
}
