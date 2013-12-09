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

import io.airlift.command.Command;
import io.druid.initialization.DruidModule;
import io.druid.initialization.Initialization;
import io.druid.server.initialization.ExtensionsConfig;

import java.lang.StringBuilder;

@Command(
    name = "version",
    description = "Returns Druid version information"
)
public class Version implements Runnable
{
  private static final String NL = "\n";
  @Override
  public void run()
  {
    StringBuilder output = new StringBuilder();
    output.append("Druid version ").append(NL);
    output.append(Initialization.class.getPackage().getImplementationVersion()).append(NL).append(NL);

    ExtensionsConfig config = Initialization.makeStartupInjector().getInstance(ExtensionsConfig.class);

    output.append("Registered Druid Modules").append(NL);

    for (DruidModule module : Initialization.getFromExtensions(config, DruidModule.class)) {
      String artifact = module.getClass().getPackage().getImplementationTitle();
      String version = module.getClass().getPackage().getImplementationVersion();

      if (artifact != null) {
        output.append(
            String.format("  - %s (%s-%s)", module.getClass().getCanonicalName(), artifact, version)
        ).append(NL);
      } else {
        output.append(
            String.format("  - %s", module.getClass().getCanonicalName())
        ).append(NL);
      }
    }

    System.out.println(output.toString());
  }
}
