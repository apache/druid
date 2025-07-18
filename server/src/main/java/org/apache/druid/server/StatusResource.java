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

package org.apache.druid.server;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.guice.ExtensionsLoader;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.http.security.StateResourceFilter;
import org.apache.druid.utils.RuntimeInfo;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 *
 */
@Path("/status")
public class StatusResource
{
  private final Properties properties;
  private final DruidServerConfig druidServerConfig;
  private final ExtensionsLoader extnLoader;
  private final RuntimeInfo runtimeInfo;

  @Inject
  public StatusResource(
      final Properties properties,
      final DruidServerConfig druidServerConfig,
      final ExtensionsLoader extnLoader,
      final RuntimeInfo runtimeInfo
  )
  {
    this.properties = properties;
    this.druidServerConfig = druidServerConfig;
    this.extnLoader = extnLoader;
    this.runtimeInfo = runtimeInfo;
  }

  @GET
  @Path("/properties")
  @ResourceFilters(ConfigResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, String> getProperties()
  {
    Map<String, String> allProperties = Maps.fromProperties(properties);
    Set<String> hiddenProperties = druidServerConfig.getHiddenProperties();
    return filterHiddenProperties(hiddenProperties, allProperties);
  }

  /**
   * filter out entries from allProperties with key containing elements in hiddenProperties (case insensitive)
   *
   * for example, if hiddenProperties = ["pwd"] and allProperties = {"foopwd": "secret", "foo": "bar", "my.pwd": "secret"},
   * this method will return {"foo":"bar"}
   *
   * @return map of properties that are not filtered out.
   */
  @Nonnull
  private Map<String, String> filterHiddenProperties(
      Set<String> hiddenProperties,
      Map<String, String> allProperties
  )
  {
    Map<String, String> propertyCopy = new HashMap<>(allProperties);
    allProperties.keySet().forEach(
        (key) -> {
          if (hiddenProperties.stream().anyMatch((hiddenProperty) -> StringUtils.toLowerCase(key).contains(StringUtils.toLowerCase(hiddenProperty)))) {
            propertyCopy.remove(key);
          }
        }
    );
    return propertyCopy;
  }

  @GET
  @ResourceFilters(StateResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Status doGet(
      @Context final HttpServletRequest req
  )
  {
    return new Status(extnLoader.getLoadedModules(), runtimeInfo);
  }

  /**
   * This is an unsecured endpoint, defined as such in UNSECURED_PATHS in the service initiailization files
   * (e.g. CliOverlord, CoordinatorJettyServerInitializer)
   */
  @GET
  @Path("/health")
  @Produces(MediaType.APPLICATION_JSON)
  public boolean getHealth()
  {
    return true;
  }

  public static class Status
  {
    final String version;
    final List<ModuleVersion> modules;
    final Memory memory;

    public Status(Collection<DruidModule> modules, RuntimeInfo runtimeInfo)
    {
      this.version = getDruidVersion();
      this.modules = getExtensionVersions(modules);
      this.memory = new Memory(runtimeInfo);
    }

    private String getDruidVersion()
    {
      return GuavaUtils.firstNonNull(Status.class.getPackage().getImplementationVersion(), "unknown");
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
      StringBuilder output = new StringBuilder();
      String lineSeparator = System.lineSeparator();
      output.append(StringUtils.format("Druid version - %s", version)).append(lineSeparator).append(lineSeparator);

      if (modules.size() > 0) {
        output.append("Registered Druid Modules").append(lineSeparator);
      } else {
        output.append("No Druid Modules loaded !");
      }

      for (ModuleVersion moduleVersion : modules) {
        output.append(moduleVersion).append(lineSeparator);
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
        moduleVersions.add(new ModuleVersion(module.getClass().getName(), artifact, version));
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
    final long directMemory;

    public Memory(RuntimeInfo runtime)
    {
      maxMemory = runtime.getMaxHeapSizeBytes();
      totalMemory = runtime.getTotalHeapSizeBytes();
      freeMemory = runtime.getFreeHeapSizeBytes();
      usedMemory = totalMemory - freeMemory;

      long directMemory = -1;
      try {
        directMemory = runtime.getDirectMemorySizeBytes();
      }
      catch (UnsupportedOperationException ignore) {
        // querying direct memory is not supported
      }
      this.directMemory = directMemory;
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

    @JsonProperty
    public long getDirectMemory()
    {
      return directMemory;
    }
  }
}
