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

package org.apache.druid.testsEx.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.testsEx.config.ServiceConfig.DruidConfig;
import org.apache.druid.testsEx.config.ServiceConfig.ZKConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Java representation of the test configuration YAML.
 * <p>
 * This object is primarily de-serialized as the files are written by hand
 * to define a test. It is serialized only for debugging.
 */
public class ClusterConfig
{
  public enum ClusterType
  {
    docker,
    k8s,
    local,
    disabled
  }

  private boolean isResource;
  private String basePath;

  @JsonProperty("type")
  private ClusterType type;
  @JsonProperty("proxyHost")
  private String proxyHost;
  @JsonProperty("include")
  private List<String> include;
  @JsonProperty("readyTimeoutSec")
  private int readyTimeoutSec;
  @JsonProperty("readyPollMs")
  private int readyPollMs;
  @JsonProperty("zk")
  private ZKConfig zk;
  @JsonProperty("metastore")
  private MetastoreConfig metastore;
  @JsonProperty("kafka")
  private KafkaConfig kafka;
  @JsonProperty("druid")
  private Map<String, DruidConfig> druidServices;
  @JsonProperty("settings")
  private Map<String, Object> settings;
  @JsonProperty("properties")
  private Map<String, Object> properties;
  @JsonProperty("metastoreInit")
  private List<MetastoreStmt> metastoreInit;
  @JsonProperty("datasourceSuffix")
  private String datasourceSuffix;

  /**
   * Delay after initializing the DB to wait for the coordinator to notice
   * the changes. This is a pure hack to work around an optimization in
   * the coordinator that would otherwise ignore the changes.
   * Set to a bit longer than the coordinator druid.manager.segments.pollDuration
   * property.
   */
  @JsonProperty("metastoreInitDelaySec")
  private int metastoreInitDelaySec;

  public ClusterConfig()
  {
  }

  public ClusterConfig(ClusterConfig from)
  {
    this.type = from.type;
    this.proxyHost = from.proxyHost;
    this.include = null; // Tell IntelliJ inspections we don't want to copy this.
    this.readyTimeoutSec = from.readyTimeoutSec;
    this.readyPollMs = from.readyPollMs;
    this.isResource = from.isResource;
    this.basePath = from.basePath;
    this.metastoreInitDelaySec = from.metastoreInitDelaySec;
    this.datasourceSuffix = from.datasourceSuffix;
    if (from.include != null) {
      this.include = new ArrayList<>(from.include);
    }
    this.zk = from.zk;
    this.metastore = from.metastore;
    this.kafka = from.kafka;
    if (from.druidServices != null) {
      this.druidServices = new HashMap<>(from.druidServices);
    }
    if (from.properties != null) {
      this.properties = new HashMap<>(from.properties);
    }
    if (from.settings != null) {
      this.settings = new HashMap<>(from.settings);
    }
    if (from.metastoreInit != null) {
      this.metastoreInit = new ArrayList<>(from.metastoreInit);
    }
  }

  public static ClusterConfig loadFromFile(String filePath)
  {
    return loadFromFile(new File(filePath));
  }

  public static ClusterConfig loadFromFile(File configFile)
  {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try {
      ClusterConfig config = mapper.readValue(configFile, ClusterConfig.class);
      config.isResource = false;
      config.basePath = configFile.getParent();
      return config;
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to load config file: " + configFile);
    }
  }

  public static ClusterConfig loadFromResource(String resource)
  {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    try (InputStream is = TestConfigs.class.getResourceAsStream(resource)) {
      if (is == null) {
        throw new ISE("Config resource not found: " + resource);
      }
      ClusterConfig config = mapper.readValue(is, ClusterConfig.class);
      config.isResource = true;
      return config;
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to load config resource: " + resource);
    }
  }

  public ResolvedConfig resolve(String clusterName)
  {
    return new ResolvedConfig(clusterName, resolveIncludes());
  }

  public ClusterConfig resolveIncludes()
  {
    if (include == null || include.isEmpty()) {
      return this;
    }
    ClusterConfig included = null;
    for (String entry : include) {
      ClusterConfig child = loadInclude(entry);
      if (included == null) {
        included = child;
      } else {
        included = included.merge(child);
      }
    }
    return included.merge(this);
  }

  private ClusterConfig loadInclude(String includeName)
  {
    if (isResource) {
      return loadFromResource(includeName);
    } else {
      File file = new File(new File(basePath), includeName);
      return loadFromFile(file);
    }
  }

  @JsonProperty("type")
  @JsonInclude(Include.NON_DEFAULT)
  public ClusterType type()
  {
    return type;
  }

  @JsonProperty("readyTimeoutSec")
  @JsonInclude(Include.NON_DEFAULT)
  public int readyTimeoutSec()
  {
    return readyTimeoutSec;
  }

  @JsonProperty("readyPollMs")
  @JsonInclude(Include.NON_DEFAULT)
  public int readyPollMs()
  {
    return readyPollMs;
  }

  @JsonProperty("proxyHost")
  @JsonInclude(Include.NON_NULL)
  public String proxyHost()
  {
    return proxyHost;
  }

  @JsonProperty("include")
  @JsonInclude(Include.NON_NULL)
  public List<String> include()
  {
    return include;
  }

  @JsonProperty("zk")
  @JsonInclude(Include.NON_NULL)
  public ZKConfig zk()
  {
    return zk;
  }

  @JsonProperty("metastore")
  @JsonInclude(Include.NON_NULL)
  public MetastoreConfig metastore()
  {
    return metastore;
  }

  @JsonProperty("kafka")
  @JsonInclude(Include.NON_NULL)
  public KafkaConfig kafka()
  {
    return kafka;
  }

  @JsonProperty("druid")
  @JsonInclude(Include.NON_NULL)
  public Map<String, DruidConfig> druid()
  {
    return druidServices;
  }

  @JsonProperty("settings")
  @JsonInclude(Include.NON_NULL)
  public Map<String, Object> settings()
  {
    return settings;
  }

  @JsonProperty("properties")
  @JsonInclude(Include.NON_NULL)
  public Map<String, Object> properties()
  {
    return properties;
  }

  @JsonProperty("metastoreInit")
  @JsonInclude(Include.NON_NULL)
  public List<MetastoreStmt> metastoreInit()
  {
    return metastoreInit;
  }

  @JsonProperty("metastoreInitDelaySec")
  @JsonInclude(Include.NON_DEFAULT)
  public int metastoreInitDelaySec()
  {
    return metastoreInitDelaySec;
  }

  @JsonProperty("datasourceSuffix")
  @JsonInclude(Include.NON_NULL)
  public String datasourceSuffix()
  {
    return datasourceSuffix;
  }

  @Override
  public String toString()
  {
    return TestConfigs.toYaml(this);
  }

  public ClusterConfig merge(ClusterConfig overrides)
  {
    ClusterConfig merged = new ClusterConfig(this);
    if (overrides.readyTimeoutSec != 0) {
      merged.readyTimeoutSec = overrides.readyTimeoutSec;
    }
    if (overrides.proxyHost != null) {
      merged.proxyHost = overrides.proxyHost;
    }
    // Includes are already considered.
    if (overrides.zk != null) {
      merged.zk = overrides.zk;
    }
    if (overrides.metastore != null) {
      merged.metastore = overrides.metastore;
    }
    if (overrides.kafka != null) {
      merged.kafka = overrides.kafka;
    }
    if (merged.druidServices == null) {
      merged.druidServices = overrides.druidServices;
    } else if (overrides.druidServices != null) {
      merged.druidServices.putAll(overrides.druidServices);
    }
    if (merged.settings == null) {
      merged.settings = overrides.settings;
    } else if (overrides.settings != null) {
      merged.settings.putAll(overrides.settings);
    }
    if (merged.properties == null) {
      merged.properties = overrides.properties;
    } else if (overrides.properties != null) {
      merged.properties.putAll(overrides.properties);
    }
    if (merged.metastoreInit == null) {
      merged.metastoreInit = overrides.metastoreInit;
    } else if (overrides.metastoreInit != null) {
      merged.metastoreInit.addAll(overrides.metastoreInit);
    }
    return merged;
  }
}
