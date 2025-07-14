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

package org.apache.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.druid.common.config.Configs;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class HadoopTaskConfig
{
  private static final String HADOOP_LIB_VERSIONS = "hadoop.indexer.libs.version";
  public static final List<String> DEFAULT_DEFAULT_HADOOP_COORDINATES;

  static {
    try {
      DEFAULT_DEFAULT_HADOOP_COORDINATES =
          ImmutableList.copyOf(Lists.newArrayList(IOUtils.toString(
              TaskConfig.class.getResourceAsStream("/" + HADOOP_LIB_VERSIONS),
              StandardCharsets.UTF_8
          ).split(",")));

    }
    catch (Exception e) {
      throw new ISE(e, "Unable to read file %s from classpath ", HADOOP_LIB_VERSIONS);
    }
  }

  @JsonProperty
  private final String hadoopWorkingPath;

  @JsonProperty
  private final List<String> defaultHadoopCoordinates;

  @JsonCreator
  public HadoopTaskConfig(
      @JsonProperty("hadoopWorkingPath") @Nullable String hadoopWorkingPath,
      @JsonProperty("defaultHadoopCoordinates") @Nullable List<String> defaultHadoopCoordinates
  )
  {
    this.hadoopWorkingPath = Configs.valueOrDefault(hadoopWorkingPath, "/tmp/druid-indexing");
    this.defaultHadoopCoordinates = Configs.valueOrDefault(
        defaultHadoopCoordinates,
        DEFAULT_DEFAULT_HADOOP_COORDINATES
    );
  }


  @JsonProperty
  public String getHadoopWorkingPath()
  {
    return hadoopWorkingPath;
  }

  @JsonProperty
  public List<String> getDefaultHadoopCoordinates()
  {
    return defaultHadoopCoordinates;
  }
}
