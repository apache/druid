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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.curator.CuratorConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testsEx.config.ServiceConfig.ZKConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ResolvedService
{
  protected final String service;
  protected final List<ResolvedInstance> instances = new ArrayList<>();

  public ResolvedService(ResolvedConfig root, ServiceConfig config, String name)
  {
    this.service = config.service() == null ? name : config.service();
    for (ServiceInstance instanceConfig : config.instances()) {
      this.instances.add(new ResolvedInstance(root, instanceConfig, this));
    }
  }

  public String service()
  {
    return service;
  }

  public List<ResolvedInstance> requireInstances()
  {
    if (instances.isEmpty()) {
      throw new ISE("Please specify a " + service + " instance");
    }
    return instances;
  }

  public ResolvedInstance instance()
  {
    return requireInstances().get(0);
  }

  public class ResolvedInstance
  {
    private final String container;
    private final String host;
    private final String clientHost;
    private final String tag;
    private final int port;
    private final int clientPort;

    public ResolvedInstance(ResolvedConfig root, ServiceInstance config, ResolvedService service)
    {
      this.tag = config.tag();

      // The actual (cluster) host is...
      if (config.host() != null) {
        // The specified host, if provided
        this.host = config.host();
      } else {
        String baseHost;
        if (root.hasProxy()) {
          // The same as the service, if there is a proxy
          baseHost = service.service;
          // with the tag appended
          if (tag != null) {
            baseHost += "-" + config.tag();
          }
          this.host = baseHost;
        } else {
          // The local host otherwise
          this.host = "localhost";
        }
      }

      if (root.hasProxy()) {
        this.clientHost = root.proxyHost();
      } else {
        this.clientHost = this.host;
      }

      this.container = config.container() != null ? config.container() : service.service;
      if (config.port() == 0) {
        throw new ISE("Must provide port");
      }
      this.port = config.port();
      if (config.proxyPort() != 0) {
        this.clientPort = config.proxyPort();
      } else {
        this.clientPort = this.port;
      }
    }

    public ResolvedService service()
    {
      return ResolvedService.this;
    }

    public String container()
    {
      return container;
    }

    public String host()
    {
      return host;
    }

    public String clientHost()
    {
      return clientHost;
    }

    public String tag()
    {
      return tag;
    }

    public int port()
    {
      return port;
    }

    public int clientPort()
    {
      return clientPort;
    }
  }

  public static class ResolvedZk extends ResolvedService
  {
    private final int startTimeoutSecs;

    public ResolvedZk(ResolvedConfig root, ZKConfig config)
    {
      super(root, config, "zookeeper");
      this.startTimeoutSecs = config.startTimeoutSecs();
    }

    public int startTimeoutSecs()
    {
      return startTimeoutSecs;
    }

    public String clientHosts()
    {
      List<String> hosts = new ArrayList<>();
      for (ResolvedInstance instance : instances) {
        hosts.add(formatHost(instance.clientHost(), instance.clientPort()));
      }
      return String.join(",", hosts);
    }

    public String clusterHosts()
    {
      List<String> hosts = new ArrayList<>();
      for (ResolvedInstance instance : instances) {
        hosts.add(formatHost(instance.host(), instance.port()));
      }
      return String.join(",", hosts);
    }

    private String formatHost(String host, int port)
    {
      return StringUtils.format("%s:%d", host, port);
    }

    public Map<? extends String, ? extends Object> toProperties()
    {
      /*
       * We will use this instead of druid server's CuratorConfig, because CuratorConfig in
       * a test cluster environment sees Zookeeper at localhost even if Zookeeper is elsewhere.
       * We'll take the Zookeeper host from the configuration file instead.
       */
      return ImmutableMap.of(
          CuratorConfig.CONFIG_PREFIX + ".zkHosts",
          clientHosts());
    }
  }

  public static class ResolvedKafka extends ResolvedService
  {
    public ResolvedKafka(ResolvedConfig root, KafkaConfig config)
    {
      super(root, config, "kafka");
    }

    public String clientHost()
    {
      return instance().clientHost();
    }

    public String bootstrap()
    {
      ResolvedInstance instance = instance();
      return StringUtils.format("%s:%d", instance.clientHost(), instance.clientPort());
    }
  }
}
