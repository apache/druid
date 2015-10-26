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

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 */
public class ExtensionsConfig
{
  @JsonProperty
  @NotNull
  private boolean searchCurrentClassloader = true;

  @JsonProperty
  private String directory = "druid_extensions";

  @JsonProperty
  private String hadoopDependenciesDir = "hadoop_druid_dependencies";

  @JsonProperty
  private List<String> loadList;

  public boolean searchCurrentClassloader()
  {
    return searchCurrentClassloader;
  }

  public String getDirectory()
  {
    return directory;
  }

  public String getHadoopDependenciesDir()
  {
    return hadoopDependenciesDir;
  }

  public List<String> getLoadList()
  {
    return loadList;
  }

  @Override
  public String toString()
  {
    return "ExtensionsConfig{" +
           "searchCurrentClassloader=" + searchCurrentClassloader +
           ", directory='" + directory + '\'' +
           ", hadoopDependenciesDir='" + hadoopDependenciesDir + '\'' +
           ", loadList=" + loadList +
           '}';
  }
}
