/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.cli;

import com.google.inject.Injector;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.ParseException;
import io.netty.util.SuppressForbidden;
import org.apache.druid.cli.validate.DruidJsonValidator;
import org.apache.druid.guice.ExtensionsConfig;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.initialization.Initialization;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

/**
 */
public class Main
{
  static {
    ServiceLoader<PropertyChecker> serviceLoader = ServiceLoader.load(PropertyChecker.class);
    for (PropertyChecker propertyChecker : serviceLoader) {
      propertyChecker.checkProperties(System.getProperties());
    }
  }

  @SuppressWarnings("unchecked")
  @SuppressForbidden(reason = "System#out")
  public static void main(String[] args)
  {
    final Cli.CliBuilder<Runnable> builder = Cli.builder("druid");

    builder.withDescription("Druid command-line runner.")
           .withDefaultCommand(Help.class)
           .withCommands(Help.class, Version.class);

    List<Class<? extends Runnable>> serverCommands = Arrays.asList(
        CliCoordinator.class,
        CliHistorical.class,
        CliBroker.class,
        CliOverlord.class,
        CliIndexer.class,
        CliMiddleManager.class,
        CliRouter.class
    );
    builder.withGroup("server")
           .withDescription("Run one of the Druid server types.")
           .withDefaultCommand(Help.class)
           .withCommands(serverCommands);

    List<Class<? extends Runnable>> toolCommands = Arrays.asList(
        DruidJsonValidator.class,
        PullDependencies.class,
        CreateTables.class,
        DumpSegment.class,
        ResetCluster.class,
        ValidateSegments.class,
        ExportMetadata.class
    );
    builder.withGroup("tools")
           .withDescription("Various tools for working with Druid")
           .withDefaultCommand(Help.class)
           .withCommands(toolCommands);

    builder.withGroup("index")
           .withDescription("Run indexing for druid")
           .withDefaultCommand(Help.class)
           .withCommands(CliHadoopIndexer.class);

    builder.withGroup("internal")
           .withDescription("Processes that Druid runs \"internally\", you should rarely use these directly")
           .withDefaultCommand(Help.class)
           .withCommands(CliPeon.class, CliInternalHadoopIndexer.class);

    final Injector injector = GuiceInjectors.makeStartupInjector();
    final ExtensionsConfig config = injector.getInstance(ExtensionsConfig.class);
    final Collection<CliCommandCreator> extensionCommands = Initialization.getFromExtensions(
        config,
        CliCommandCreator.class
    );

    for (CliCommandCreator creator : extensionCommands) {
      creator.addCommands(builder);
    }

    final Cli<Runnable> cli = builder.build();
    try {
      final Runnable command = cli.parse(args);
      if (!(command instanceof Help)) { // Hack to work around Help not liking being injected
        injector.injectMembers(command);
      }
      command.run();
    }
    catch (ParseException e) {
      System.out.println("ERROR!!!!");
      System.out.println(e.getMessage());
      System.out.println("===");
      cli.parse(new String[]{"help"}).run();
      System.exit(1);
    }
  }
}
