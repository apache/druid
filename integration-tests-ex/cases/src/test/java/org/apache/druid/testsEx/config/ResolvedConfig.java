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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.curator.CuratorConfig;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.testing.IntegrationTestingConfigProvider;
import org.apache.druid.testsEx.config.ClusterConfig.ClusterType;
import org.apache.druid.testsEx.config.ResolvedService.ResolvedKafka;
import org.apache.druid.testsEx.config.ResolvedService.ResolvedZk;
import org.apache.druid.testsEx.config.ServiceConfig.DruidConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ResolvedConfig
{
  public static final String COORDINATOR = "coordinator";
  public static final String HISTORICAL = "historical";
  public static final String OVERLORD = "overlord";
  public static final String BROKER = "broker";
  public static final String ROUTER = "router";
  public static final String MIDDLEMANAGER = "middlemanager";
  public static final String INDEXER = "indexer";

  public static final int DEFAULT_READY_TIMEOUT_SEC = 120;
  public static final int DEFAULT_READY_POLL_MS = 2000;

  private final String category;
  private final ClusterType type;
  private final String proxyHost;
  private final int readyTimeoutSec;
  private final int readyPollMs;
  final String datasourceNameSuffix;
  private Map<String, Object> properties;
  private Map<String, Object> settings;

  private final ResolvedZk zk;
  private final ResolvedKafka kafka;
  private final ResolvedMetastore metastore;
  private final Map<String, ResolvedDruidService> druidServices = new HashMap<>();

  public ResolvedConfig(String category, ClusterConfig config)
  {
    this.category = category;
    type = config.type() == null ? ClusterType.docker : config.type();
    if (!hasProxy()) {
      proxyHost = null;
    } else if (Strings.isNullOrEmpty(config.proxyHost())) {
      proxyHost = "localhost";
    } else {
      proxyHost = config.proxyHost();
    }
    readyTimeoutSec = config.readyTimeoutSec() > 0 ?
        config.readyTimeoutSec() : DEFAULT_READY_TIMEOUT_SEC;
    readyPollMs = config.readyPollMs() > 0 ? config.readyPollMs() : DEFAULT_READY_POLL_MS;
    if (config.properties() == null) {
      this.properties = ImmutableMap.of();
    } else {
      this.properties = config.properties();
    }
    if (config.settings() == null) {
      this.settings = ImmutableMap.of();
    } else {
      this.settings = config.settings();
    }
    if (config.datasourceSuffix() == null) {
      this.datasourceNameSuffix = "";
    } else {
      this.datasourceNameSuffix = config.datasourceSuffix();
    }

    if (config.zk() == null) {
      this.zk = null;
    } else {
      this.zk = new ResolvedZk(this, config.zk());
    }
    if (config.kafka() == null) {
      this.kafka = null;
    } else {
      this.kafka = new ResolvedKafka(this, config.kafka());
    }
    if (config.metastore() == null) {
      this.metastore = null;
    } else {
      this.metastore = new ResolvedMetastore(this, config.metastore(), config);
    }

    if (config.druid() != null) {
      for (Entry<String, DruidConfig> entry : config.druid().entrySet()) {
        druidServices.put(entry.getKey(),
            new ResolvedDruidService(this, entry.getValue(), entry.getKey()));
      }
    }
  }

  public ClusterType type()
  {
    return type;
  }

  public String proxyHost()
  {
    return proxyHost;
  }

  public int readyTimeoutSec()
  {
    return readyTimeoutSec;
  }

  public int readyPollMs()
  {
    return readyPollMs;
  }

  public boolean isDocker()
  {
    return type == ClusterType.docker;
  }

  public boolean hasProxy()
  {
    switch (type) {
      case docker:
      case k8s:
        return true;
      default:
        return false;
    }
  }

  public ResolvedZk zk()
  {
    return zk;
  }

  public ResolvedMetastore metastore()
  {
    return metastore;
  }

  public ResolvedKafka kafka()
  {
    return kafka;
  }

  public Map<String, Object> settings()
  {
    return settings;
  }

  public Map<String, Object> properties()
  {
    return properties;
  }

  public Map<String, ResolvedDruidService> requireDruid()
  {
    if (druidServices == null) {
      throw new ISE("Please configure Druid services");
    }
    return druidServices;
  }

  public ResolvedZk requireZk()
  {
    if (zk == null) {
      throw new ISE("Please specify the ZooKeeper configuration");
    }
    return zk;
  }

  public ResolvedMetastore requireMetastore()
  {
    if (metastore == null) {
      throw new ISE("Please specify the Metastore configuration");
    }
    return metastore;
  }

  public ResolvedKafka requireKafka()
  {
    if (kafka == null) {
      throw new ISE("Please specify the Kafka configuration");
    }
    return kafka;
  }

  public ResolvedDruidService druidService(String serviceKey)
  {
    return requireDruid().get(serviceKey);
  }

  public ResolvedDruidService requireService(String serviceKey)
  {
    ResolvedDruidService service = druidService(serviceKey);
    if (service == null) {
      throw new ISE("Please configure Druid service " + serviceKey);
    }
    return service;
  }

  public ResolvedDruidService requireCoordinator()
  {
    return requireService(COORDINATOR);
  }

  public ResolvedDruidService requireOverlord()
  {
    return requireService(OVERLORD);
  }

  public ResolvedDruidService requireBroker()
  {
    return requireService(BROKER);
  }

  public ResolvedDruidService requireRouter()
  {
    return requireService(ROUTER);
  }

  public ResolvedDruidService requireMiddleManager()
  {
    return requireService(MIDDLEMANAGER);
  }

  public ResolvedDruidService requireHistorical()
  {
    return requireService(HISTORICAL);
  }

  public String routerUrl()
  {
    return requireRouter().clientUrl();
  }

  public CuratorConfig toCuratorConfig()
  {
    if (zk == null) {
      throw new ISE("ZooKeeper not configured");
    }
    // TODO: Add a builder for other properties
    return CuratorConfig.create(zk.clientHosts());
  }

  /**
   * Map from old-style config file (and settings) name to the
   * corresponding property.
   */
  private static final Map<String, String> SETTINGS_MAP =
      ImmutableMap.<String, String>builder()
        .put("cloud_bucket", "cloudBucket")
        .put("cloud_path", "cloudPath")
        .put("cloud_region", "cloudRegion")
        .put("s3_assume_role_with_external_id", "s3AssumeRoleWithExternalId")
        .put("s3_assume_role_external_id", "s3AssumeRoleExternalId")
        .put("s3_assume_role_without_external_id", "s3AssumeRoleWithoutExternalId")
        .put("stream_endpoint", "streamEndpoint")
        .put("s3_accessKey", "s3AccessKey")
        .put("s3_secretKey", "s3SecretKey")
        .put("azure_account", "azureAccount")
        .put("azure_key", "azureKey")
        .put("azure_container", "azureContainer")
        .put("google_bucket", "googleBucket")
        .put("google_prefix", "googlePrefix")
        .build();

  private static void setDruidProperyVar(Map<String, Object> properties, String key, Object value)
  {
    if (value == null) {
      return;
    }
    if (key.startsWith("druid_")) {
      key = key.substring("druid_".length());
    }
    String mapped = SETTINGS_MAP.get(key);
    key = mapped == null ? key : mapped;
    TestConfigs.putProperty(properties, IntegrationTestingConfigProvider.PROPERTY_BASE, key, value.toString());
  }

  private void loadPropertyFile(Map<String, Object> properties, File file)
  {
    try (BufferedReader in = new BufferedReader(
        new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
      String line;
      while ((line = in.readLine()) != null) {
        if (Strings.isNullOrEmpty(line) || line.startsWith("#")) {
          continue;
        }
        String[] parts = line.split("=");
        if (parts.length != 2) {
          continue;
        }
        setDruidProperyVar(properties, parts[0], parts[1]);
      }
    }
    catch (IOException e) {
      throw new IAE(e, "Cannot read file %s", file.getAbsolutePath());
    }
  }

  /**
   * Convert the config in this structure the the properties
   * used to configure Guice.
   */
  public Map<String, Object> toProperties()
  {
    Map<String, Object> properties = new HashMap<>();
    // druid.test.config.dockerIp is used by some older test code. Remove
    // it when that code is updated.
    TestConfigs.putProperty(properties, "druid.test.config.dockerIp", proxyHost);

    // Start with implicit properties from various sections.
    if (zk != null) {
      properties.putAll(zk.toProperties());
    }
    if (metastore != null) {
      properties.putAll(metastore.toProperties());
    }

    // Add settings, converted to properties. Map both old and
    // "property-style" settings to the full property path.
    // Settings are converted to properties so they can be overridden
    // by environment variables and -D command-line settings.
    for (Map.Entry<String, Object> entry : settings.entrySet()) {
      setDruidProperyVar(properties, entry.getKey(), entry.getValue());
    }

    // Add explicit properties
    if (this.properties != null) {
      properties.putAll(this.properties);
    }

    // Override with a user-specific config file.
    File userEnv = new File(
        new File(
            System.getProperty("user.home"),
            "druid-it"),
        category + ".env");
    if (userEnv.exists()) {
      loadPropertyFile(properties, userEnv);
    }

    // Override with a user-specific config file.
    String overrideEnv = System.getenv("OVERRIDE_ENV");
    if (overrideEnv != null) {
      loadPropertyFile(properties, new File(overrideEnv));
    }

    // Override with any environment variables of the form "druid_"
    for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
      String key = entry.getKey();
      if (!key.startsWith("druid_")) {
        continue;
      }
      setDruidProperyVar(properties, key, entry.getValue());
    }

    // Override with any system properties of the form "druid_"
    for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
      String key = (String) entry.getKey();
      if (!key.startsWith("druid_")) {
        continue;
      }
      setDruidProperyVar(properties, key, entry.getValue());
    }
    return properties;
  }
}
