package com.metamx.druid.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 */
public class CLI
{
  private final Options opts;

  public CLI()
  {
    this.opts = new Options();
  }

  public CLI addOptionGroup(OptionGroup group)
  {
    opts.addOptionGroup(group);
    return this;
  }

  public CLI addOption(String opt, boolean hasArg, String description)
  {
    opts.addOption(opt, hasArg, description);
    return this;
  }

  public CLI addOption(String opt, String longOpt, boolean hasArg, String description)
  {
    opts.addOption(opt, longOpt, hasArg, description);
    return this;
  }

  public CLI addOption(Option opt)
  {
    opts.addOption(opt);
    return this;
  }

  public CLI addRequiredOption(String opt, String longOpt, boolean hasArg, String description)
  {
    opts.addOption(new RequiredOption(opt, longOpt, hasArg, description));
    return this;
  }

  public CommandLine parse(String[] args)
  {
    if (args.length == 0) {
      new HelpFormatter().printHelp("<java invocation>", opts);
      return null;
    }

    CommandLine cli = null;
    try {
      cli = new PosixParser().parse(opts, args, false);
    }
    catch (ParseException e) {
      System.out.println(e.getMessage());
      new HelpFormatter().printHelp("<java invocation>", opts);
      return null;
    }

    if (cli.hasOption("help")) {
      new HelpFormatter().printHelp("<java invocation>", opts);
      return null;
    }

    return cli;
  }
}
