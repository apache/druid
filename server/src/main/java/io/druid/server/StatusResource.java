/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.initialization.DruidModule;
import io.druid.initialization.Initialization;
import io.druid.java.util.common.StringUtils;
import io.druid.server.http.security.StateResourceFilter;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 */
@Path("/status")
@ResourceFilters(StateResourceFilter.class)
public class StatusResource
{
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Status doGet()
  {
    return new Status(Initialization.getLoadedImplementations(DruidModule.class));
  }

  public static class Status
  {
    final String version;
    final List<ModuleVersion> modules;
    final Memory memory;

    public Status(Collection<DruidModule> modules)
    {
      this.version = getDruidVersion();
      this.modules = getExtensionVersions(modules);
      this.memory = new Memory(Runtime.getRuntime());
    }

    private String getDruidVersion()
    {
      return Status.class.getPackage().getImplementationVersion();
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
      final String NL = System.getProperty("line.separator");
      StringBuilder output = new StringBuilder();
      output.append(StringUtils.format("Druid version - %s", version)).append(NL).append(NL);

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

    /**
     * Load the unique extensions and return their implementation-versions
     *
     * @return map of extensions loaded with their respective implementation versions.
     */
    private List<ModuleVersion> getExtensionVersions(Collection<DruidModule> druidModules)
    {
      List<ModuleVersion> moduleVersions = new ArrayList<>();
      for (DruidModule module : druidModules) {
        String artifact = module.getClass().getPackage().getImplementationTitle();
        String version = module.getClass().getPackage().getImplementationVersion();

        moduleVersions.add(new ModuleVersion(module.getClass().getCanonicalName(), artifact, version));
      }
      return moduleVersions;
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class ModuleVersion
  {
    final String name;
    final String artifact;
    final String version;

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
      if (artifact == null || artifact.isEmpty()) {
        return StringUtils.format("  - %s ", name);
      } else {
        return StringUtils.format("  - %s (%s-%s)", name, artifact, version);
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
