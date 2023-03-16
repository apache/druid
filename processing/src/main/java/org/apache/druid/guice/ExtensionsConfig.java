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

package org.apache.druid.guice;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.util.LinkedHashSet;
import java.util.List;

/**
 * Configuration for Druid extensions.
 */
public class ExtensionsConfig
{
  public static final String PROPERTY_BASE = "druid.extensions";
  public static final String DEFAULT_EXTENSIONS_DIR = "extensions";

  @JsonProperty
  @NotNull
  private boolean searchCurrentClassloader = true;

  /**
   * The location of the "primary" extensions directory. If the path is relative
   * (as in the default), then the path is relative to the Druid directory
   * (assuming the Druid product directory is indicated by the working directory.)
   * <p>
   * The value can be blank to indicate to ignore this value and only use the path.
   * (If the value is omitted from the config file, it defaults to "extensions", so
   * we need a blank value to say to ignore this property.)
   */
  @JsonProperty
  private String directory = DEFAULT_EXTENSIONS_DIR;

  /**
   * Extensions path. Items may be relative to the Druid home directory, or absolute.
   * Extensions are resolved in order along the path: the first match wins. If
   * {@link #directory} is set, it becomes the first element on the full path.
   * A typical use is to let {@code directory} point to {@code $DRUID_HOME/extensions},
   * and use {@link #path} to point to extra extensions mounted into a Docker container.
   * Primarily for testing.
   */
  @JsonProperty
  @Nullable
  private List<String> path;

  @JsonProperty
  private boolean useExtensionClassloaderFirst;

  @JsonProperty
  private String hadoopDependenciesDir = "hadoop-dependencies";

  @JsonProperty
  @Nullable
  private String hadoopContainerDruidClasspath;

  //Only applicable when hadoopContainerDruidClasspath is explicitly specified.
  @JsonProperty
  private boolean addExtensionsToHadoopContainer;

  @JsonProperty
  private LinkedHashSet<String> loadList;

  public boolean searchCurrentClassloader()
  {
    return searchCurrentClassloader;
  }

  public String getDirectory()
  {
    return directory;
  }

  public boolean isUseExtensionClassloaderFirst()
  {
    return useExtensionClassloaderFirst;
  }

  public String getHadoopDependenciesDir()
  {
    return hadoopDependenciesDir;
  }

  public String getHadoopContainerDruidClasspath()
  {
    return hadoopContainerDruidClasspath;
  }

  public boolean getAddExtensionsToHadoopContainer()
  {
    return addExtensionsToHadoopContainer;
  }

  public LinkedHashSet<String> getLoadList()
  {
    return loadList;
  }

  public List<String> getPath()
  {
    return path;
  }

  @Override
  public String toString()
  {
    return "ExtensionsConfig{" +
           "searchCurrentClassloader=" + searchCurrentClassloader +
           ", directory='" + directory + '\'' +
           ", path='" + path + '\'' +
           ", useExtensionClassloaderFirst=" + useExtensionClassloaderFirst +
           ", hadoopDependenciesDir='" + hadoopDependenciesDir + '\'' +
           ", hadoopContainerDruidClasspath='" + hadoopContainerDruidClasspath + '\'' +
           ", addExtensionsToHadoopContainer=" + addExtensionsToHadoopContainer +
           ", loadList=" + loadList +
           '}';
  }

  public static Builder builder()
  {
    return new Builder();
  }

  // Feel free to add a toBuilder() if needed. Not doing so now to avoid
  // unused code warnings.

  /**
   * Builder for testing to avoid the need to create an anonymous class. Such classes
   * break the effective path logic. Only has methods for fields that test exercise
   * to avoid unused code warnings. Add more as needed.
   */
  public static class Builder
  {
    private String directory;
    private List<String> path;
    private LinkedHashSet<String> loadList;

    public Builder()
    {
      this(new ExtensionsConfig());
    }

    public Builder(ExtensionsConfig config)
    {
      this.directory = config.directory;
      this.path = config.path;
      this.loadList = config.loadList;
    }

    public Builder directory(String directory)
    {
      this.directory = directory;
      return this;
    }

    public Builder path(List<String> path)
    {
      this.path = path;
      return this;
    }

    public Builder loadList(LinkedHashSet<String> loadList)
    {
      this.loadList = loadList;
      return this;
    }

    public Builder loadList(List<String> loadList)
    {
      return loadList(Sets.newLinkedHashSet(loadList));
    }

    public ExtensionsConfig build()
    {
      ExtensionsConfig config = new ExtensionsConfig();
      config.directory = directory;
      config.path = path;
      config.loadList = loadList;
      return config;
    }
  }
}
