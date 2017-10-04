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

package io.druid.curator.discovery;

public class CuratorServiceUtils
{
  /**
   * Replacing '/' with ':' in service names makes it easier to provide an HTTP interface using
   * <a href="http://curator.apache.org/curator-x-discovery-server/">curator-x-discovery-server</a>
   *
   * This method is marked protected because it should never be used outside of the io.druid.curator.discovery
   * package. If you are tempted to use this method anywhere else you are most likely doing something wrong.
   * Mapping the actual service name to the name used within curator should be left to {@link CuratorServiceAnnouncer}
   * and {@link ServerDiscoveryFactory}
   *
   * @see CuratorServiceAnnouncer
   * @see ServerDiscoveryFactory
   *
   * @param serviceName
   * @return
   */
  protected static String makeCanonicalServiceName(String serviceName)
  {
    return serviceName.replaceAll("/", ":");
  }
}
