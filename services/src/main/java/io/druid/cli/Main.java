/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.command.Cli;
import io.airlift.command.Help;
import io.airlift.command.ParseException;
import io.druid.cli.convert.ConvertProperties;
import io.druid.guice.DruidGuiceExtensions;
import io.druid.guice.DruidSecondaryModule;
import io.druid.guice.JsonConfigProvider;
import io.druid.jackson.JacksonModule;
import io.druid.server.initialization.ConfigModule;
import io.druid.server.initialization.ExtensionsConfig;
import io.druid.server.initialization.Initialization;
import io.druid.server.initialization.PropertiesModule;

import java.util.List;

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
           .withCommands(
               CliCoordinator.class, CliHistorical.class, CliBroker.class,
               CliRealtime.class, CliOverlord.class, CliMiddleManager.class
           );

    builder.withGroup("example")
           .withDescription("Run an example")
           .withDefaultCommand(Help.class)
           .withCommands(CliRealtimeExample.class);

    builder.withGroup("tools")
           .withDescription("Various tools for working with Druid")
           .withDefaultCommand(Help.class)
           .withCommands(ConvertProperties.class);

    builder.withGroup("internal")
           .withDescription("Processes that Druid runs \"internally\", you should rarely use these directly")
           .withDefaultCommand(Help.class)
           .withCommands(CliPeon.class);

    final Injector injector = makeStartupInjector();
    final ExtensionsConfig config = injector.getInstance(ExtensionsConfig.class);
    final List<CliCommandCreator> extensionCommands = Initialization.getFromExtensions(config, CliCommandCreator.class);

    for (CliCommandCreator creator : extensionCommands) {
      creator.addCommands(builder);
    }

    final Cli<Runnable> cli = builder.build();
    try {
      final Runnable command = cli.parse(args);
      injector.injectMembers(command);
      command.run();
    }
    catch (ParseException e) {
      System.out.println("ERROR!!!!");
      System.out.println(e.getMessage());
      System.out.println("===");
      cli.parse(new String[]{"help"}).run();
    }
  }

  public static Injector makeStartupInjector()
  {
    return Guice.createInjector(
        new DruidGuiceExtensions(),
        new JacksonModule(),
        new PropertiesModule("runtime.properties"),
        new ConfigModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(DruidSecondaryModule.class);
            JsonConfigProvider.bind(binder, "druid.extensions", ExtensionsConfig.class);
          }
        }
    );
  }
}
