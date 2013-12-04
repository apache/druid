package io.druid.cli;

import io.airlift.command.Command;
import io.druid.initialization.DruidModule;
import io.druid.initialization.Initialization;
import io.druid.server.initialization.ExtensionsConfig;

@Command(
    name = "version",
    description = "Returns Druid version information"
)
public class Version implements Runnable {
  @Override
  public void run()
  {
    System.out.println("Druid version " + Initialization.class.getPackage().getImplementationVersion());
    System.out.println("Druid API version " + DruidModule.class.getPackage().getImplementationVersion());

    ExtensionsConfig config = Initialization.makeStartupInjector().getInstance(ExtensionsConfig.class);

    System.out.println("");
    System.out.println("Registered Druid Modules");
    for (DruidModule module : Initialization.getFromExtensions(config, DruidModule.class)) {
      String artifact = module.getClass().getPackage().getImplementationTitle();
      String version = module.getClass().getPackage().getImplementationVersion();

      if(artifact != null) {
        System.out.println(
            String.format(
                "  - %s (%s-%s)",
                module.getClass().getCanonicalName(),
                module.getClass().getPackage().getImplementationTitle(),
                module.getClass().getPackage().getImplementationVersion()
            )
        );
      } else {
        System.out.println(
                  String.format(
                      "  - %s",
                      module.getClass().getCanonicalName()
                  )
              );
      }
    }
  }
}
