/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.guice;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 */
public class ExtensionsConfig
{
  public static final String PACKAGE_VERSION = ExtensionsConfig.class.getPackage().getImplementationVersion();

  @JsonProperty
  @NotNull
  private boolean searchCurrentClassloader = true;

  @JsonProperty
  @NotNull
  private List<String> coordinates = ImmutableList.of();

  // default version to use for extensions without version info
  @JsonProperty
  private String defaultVersion;

  @JsonProperty
  @NotNull
  private String localRepository = String.format("%s/%s", System.getProperty("user.home"), ".m2/repository");

  @JsonProperty
  @NotNull
  private List<String> remoteRepositories = ImmutableList.of(
      "https://repo1.maven.org/maven2/",
      "https://metamx.artifactoryonline.com/metamx/pub-libs-releases-local"
  );

  public boolean searchCurrentClassloader()
  {
    return searchCurrentClassloader;
  }

  public List<String> getCoordinates()
  {
    return coordinates;
  }

  public String getDefaultVersion()
  {
    return defaultVersion != null ? defaultVersion : PACKAGE_VERSION;
  }

  public String getLocalRepository()
  {
    return localRepository;
  }

  public List<String> getRemoteRepositories()
  {
    return remoteRepositories;
  }

  @Override
  public String toString()
  {
    return "ExtensionsConfig{" +
           "searchCurrentClassloader=" + searchCurrentClassloader +
           ", coordinates=" + coordinates +
           ", defaultVersion='" + getDefaultVersion() + '\'' +
           ", localRepository='" + localRepository + '\'' +
           ", remoteRepositories=" + remoteRepositories +
           '}';
  }
}
