/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
   * @see io.druid.curator.discovery.CuratorServiceAnnouncer
   * @see io.druid.curator.discovery.ServerDiscoveryFactory
   *
   * @param serviceName
   * @return
   */
  protected static String makeCanonicalServiceName(String serviceName) {
    return serviceName.replaceAll("/", ":");
  }
}
