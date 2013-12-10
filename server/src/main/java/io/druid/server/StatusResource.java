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

package io.druid.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Injector;
import io.druid.initialization.DruidModule;
import io.druid.initialization.Initialization;
import io.druid.server.initialization.ExtensionsConfig;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import java.util.ArrayList;
import java.util.List;

/**
 */
@Path("/{a:status|health}")
public class StatusResource
{
  @GET
  @Produces("application/json")
  public Status doGet()
  {
    return getStatus();
  }

  public static Status getStatus()
  {
    return new Status(
        Initialization.class.getPackage().getImplementationVersion(),
        getExtensionVersions(),
        new Memory(Runtime.getRuntime())
    );
  }

  /**
   * Load the unique extensions and return their implementation-versions
   *
   * @return map of extensions loaded with their respective implementation versions.
   */
  private static List<ModuleVersion> getExtensionVersions()
  {
    final Injector injector = Initialization.makeStartupInjector();
    final ExtensionsConfig config = injector.getInstance(ExtensionsConfig.class);
    final List<DruidModule> druidModules = Initialization.getFromExtensions(config, DruidModule.class);

    List<ModuleVersion> moduleVersions = new ArrayList<>();
    for (DruidModule module : druidModules) {

      String artifact = module.getClass().getPackage().getImplementationTitle();
      String version = module.getClass().getPackage().getImplementationVersion();

      ModuleVersion moduleVersion;
      if (artifact != null) {
        moduleVersion = new ModuleVersion(module.getClass().getCanonicalName(), artifact, version);
      } else {
        moduleVersion = new ModuleVersion(module.getClass().getCanonicalName());
      }

      moduleVersions.add(moduleVersion);
    }
    return moduleVersions;
  }

  public static class Status
  {
    final String version;
    final List<ModuleVersion> modules;
    final Memory memory;

    public Status(
        String version, List<ModuleVersion> modules, Memory memory
    )
    {
      this.version = version;
      this.modules = modules;
      this.memory = memory;
    }

    @JsonProperty
    public String getVersion()
    {
      return version;
    }

    @JsonProperty
    public List<ModuleVersion> getModules()
    {
      return modules;
    }

    @JsonProperty
    public Memory getMemory()
    {
      return memory;
    }

    @Override
    public String toString()
    {
      final String NL = "\n";
      StringBuilder output = new StringBuilder();
      output.append(String.format("Druid version - %s", version)).append(NL).append(NL);

      if (modules.size() > 0) {
        output.append("Registered Druid Modules").append(NL);
      } else {
        output.append("No Druid Modules loaded !");
      }

      for (ModuleVersion moduleVersion : modules) {
        output.append(moduleVersion).append(NL);
      }
      return output.toString();
    }
  }

  public static class ModuleVersion
  {
    final String name;
    final String artifact;
    final String version;

    public ModuleVersion(String name)
    {
      this(name, "", "");
    }

    public ModuleVersion(String name, String artifact, String version)
    {
      this.name = name;
      this.artifact = artifact;
      this.version = version;
    }

    @JsonProperty
    public String getName()
    {
      return name;
    }

    @JsonProperty
    public String getArtifact()
    {
      return artifact;
    }

    @JsonProperty
    public String getVersion()
    {
      return version;
    }

    @Override
    public String toString()
    {
      if (artifact.isEmpty()) {
        return String.format("  - %s ", name);
      } else {
        return String.format("  - %s (%s-%s)", name, artifact, version);
      }
    }
  }

  public static class Memory
  {
    final long maxMemory;
    final long totalMemory;
    final long freeMemory;
    final long usedMemory;

    public Memory(Runtime runtime)
    {
      maxMemory = runtime.maxMemory();
      totalMemory = runtime.totalMemory();
      freeMemory = runtime.freeMemory();
      usedMemory = totalMemory - freeMemory;
    }

    @JsonProperty
    public long getMaxMemory()
    {
      return maxMemory;
    }

    @JsonProperty
    public long getTotalMemory()
    {
      return totalMemory;
    }

    @JsonProperty
    public long getFreeMemory()
    {
      return freeMemory;
    }

    @JsonProperty
    public long getUsedMemory()
    {
      return usedMemory;
    }

  }
}
