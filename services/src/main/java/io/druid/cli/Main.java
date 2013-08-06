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
           .withCommands(CliCoordinator.class, CliHistorical.class, CliBroker.class);

    builder.build().parse(args).run();
  }
}
