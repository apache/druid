/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

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
