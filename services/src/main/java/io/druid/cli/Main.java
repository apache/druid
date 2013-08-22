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

package io.druid.cli;

import io.airlift.command.Cli;
import io.airlift.command.Help;

/**
 */
public class Main
{
  @SuppressWarnings("unchecked")
  public static void main(String[] args)
  {
    final Cli.CliBuilder<Runnable> builder = Cli.builder("druid");

    builder.withDescription("Druid command-line runner.")
           .withDefaultCommand(Help.class)
           .withCommands(Help.class);

    builder.withGroup("server")
           .withDescription("Run one of the Druid server types.")
           .withDefaultCommand(Help.class)
           .withCommands(CliCoordinator.class, CliHistorical.class, CliBroker.class, CliRealtime.class, CliRealtimeExample.class);

    builder.build().parse(args).run();
  }
}
