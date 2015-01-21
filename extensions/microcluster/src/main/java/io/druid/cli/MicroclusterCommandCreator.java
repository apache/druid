/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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
 *
 */
public class MicroclusterCommandCreator implements CliCommandCreator
{
  @Override
  public void addCommands(Cli.CliBuilder cliBuilder)
  {
    cliBuilder.withGroup("microcluster")
              .withDescription("Run a micro server suite of a historical, broker, and coordinator")
              .withDefaultCommand(Help.class)
              .withCommands(CliMicrocluster.class, CliMicroclusterZk.class, CliMicroclusterDB.class);
  }
}
