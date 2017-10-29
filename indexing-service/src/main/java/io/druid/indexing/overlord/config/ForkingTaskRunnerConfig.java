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

package io.druid.indexing.overlord.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.guice.IndexingServiceModuleHelper;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.List;

public class ForkingTaskRunnerConfig
{
  public static final String JAVA_OPTS_PROPERTY = IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX
                                                  + ".javaOpts";
  public static final String JAVA_OPTS_ARRAY_PROPERTY = IndexingServiceModuleHelper.INDEXER_RUNNER_PROPERTY_PREFIX
                                                  + ".javaOptsArray";

  @JsonProperty
  @NotNull
  private String javaCommand = "java";

  /**
   * This is intended for setting -X parameters on the underlying java.  It is used by first splitting on whitespace,
   * so it cannot handle properties that have whitespace in the value.  Those properties should be set via a
   * druid.indexer.fork.property. property instead.
   */
  @JsonProperty
  @NotNull
  private String javaOpts = "";

  @JsonProperty
  @NotNull
  private List<String> javaOptsArray = ImmutableList.of();

  @JsonProperty
  @NotNull
  private String classpath = System.getProperty("java.class.path");

  @JsonProperty
  @Min(1024)
  @Max(65535)
  private int startPort = 8100;

  @JsonProperty
  @Min(1024)
  @Max(65535)
  private int tlsStartPort = 8300;

  @JsonProperty
  @NotNull
  List<String> allowedPrefixes = Lists.newArrayList(
      "com.metamx",
      "druid",
      "io.druid",
      "user.timezone",
      "file.encoding",
      "java.io.tmpdir",
      "hadoop"
  );

  @JsonProperty
  private boolean separateIngestionEndpoint = false;

  public boolean isSeparateIngestionEndpoint()
  {
    return separateIngestionEndpoint;
  }

  public String getJavaCommand()
  {
    return javaCommand;
  }

  public String getJavaOpts()
  {
    return javaOpts;
  }

  public List<String> getJavaOptsArray()
  {
    return javaOptsArray;
  }

  public String getClasspath()
  {
    return classpath;
  }

  public int getStartPort()
  {
    return startPort;
  }

  public int getTlsStartPort()
  {
    return tlsStartPort;
  }

  public List<String> getAllowedPrefixes()
  {
    return allowedPrefixes;
  }
}
