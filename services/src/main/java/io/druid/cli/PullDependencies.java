package io.druid.cli;

import io.airlift.command.Command;


@Command(
    name = "pull-deps",
    description = "Pull down dependencies to the local repository specified by druid.extensions.localRepository"
)
public class PullDependencies implements Runnable
{
  @Override
  public void run() {
    // dependencies are pulled down as a side-effect of Guice injection
  }
}
