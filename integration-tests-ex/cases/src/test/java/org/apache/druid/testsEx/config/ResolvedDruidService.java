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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testsEx.config.ServiceConfig.DruidConfig;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ResolvedDruidService extends ResolvedService
{
  public ResolvedDruidService(ResolvedConfig root, DruidConfig config, String serviceKey)
  {
    super(root, config, serviceKey);
  }

  /**
   * Get the URL (visible to the test) of the service.
   */
  public String clientUrl()
  {
    return resolveUrl(instance());
  }

  /**
   * Find an instance given the instance name (tag).
   */
  public ResolvedInstance findInstance(String instanceName)
  {
    for (ResolvedInstance instance : requireInstances()) {
      if (instance.tag() != null && instance.tag().equals(instanceName)) {
        return instance;
      }
    }
    return null;
  }

  /**
   * Find an instance given the instance name (tag). Raises
   * an error (which fails the test) if the tag is not defined.
   */
  public ResolvedInstance requireInstance(String instanceName)
  {
    ResolvedInstance instance = findInstance(instanceName);
    if (instance != null) {
      return instance;
    }
    throw new ISE(
        StringUtils.format(
            "No Druid instance of service %s with name %s is defined",
            service,
            instanceName));
  }

  public String resolveUrl()
  {
    return resolveUrl(instance());
  }

  /**
   * Return the URL for the given instance name (tag) of this service
   * as visible to the test.
   */
  public String resolveUrl(String instanceName)
  {
    return resolveUrl(requireInstance(instanceName));
  }

  /**
   * Return the URL, known to the test, of the given service instance.
   */
  public String resolveUrl(ResolvedInstance instance)
  {
    return StringUtils.format(
        "http://%s:%d",
        instance.clientHost(),
        instance.clientPort());
  }

  /**
   * Return the named service instance. If not found, return the
   * "default" instance. This is used by the somewhat awkward test
   * config object so that if a test asks for "Coordinator one" in
   * a cluster with a single Coordinator, it will get that Coordinator.
   * Same for Overlord.
   */
  public ResolvedInstance tagOrDefault(String tag)
  {
    ResolvedInstance taggedInstance = findInstance(tag);
    return taggedInstance == null ? instance() : taggedInstance;
  }

  /**
   * Returns the "default" host for this service as known to the
   * cluster. The host is that of the only instance and is undefined
   * if there are multiple instances.
   */
  public String resolveHost()
  {
    ResolvedInstance instance = instance();
    if (instances.size() > 1) {
      throw new ISE(
          StringUtils.format("Service %s has %d hosts, default is ambiguous",
              service,
              instances.size()));
    }
    return instance.host();
  }

  public ResolvedInstance findHost(String host)
  {
    Pattern p = Pattern.compile("https?://(.*):(\\d+)");
    Matcher m = p.matcher(host);
    if (!m.matches()) {
      return null;
    }
    String hostName = m.group(1);
    int port = Integer.parseInt(m.group(2));
    for (ResolvedInstance instance : instances) {
      if (instance.host().equals(hostName) && instance.port() == port) {
        return instance;
      }
    }
    return null;
  }
}
