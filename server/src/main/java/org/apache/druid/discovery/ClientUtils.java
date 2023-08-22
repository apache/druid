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

package org.apache.druid.discovery;

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.Request;

import javax.annotation.Nullable;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utils class for shared client methods
 */
public class ClientUtils
{
  @Nullable
  public static String pickOneHost(DruidNodeDiscovery druidNodeDiscovery)
  {
    Iterator<DiscoveryDruidNode> iter = druidNodeDiscovery.getAllNodes().iterator();
    List<DiscoveryDruidNode> discoveryDruidNodeList = Lists.newArrayList(iter);
    if (!discoveryDruidNodeList.isEmpty()) {
      DiscoveryDruidNode node = discoveryDruidNodeList.get(ThreadLocalRandom.current().nextInt(discoveryDruidNodeList.size()));
      return StringUtils.format(
          "%s://%s",
          node.getDruidNode().getServiceScheme(),
          node.getDruidNode().getHostAndPortToUse()
      );
    }
    return null;
  }

  public static Request withUrl(Request old, URL url)
  {
    Request req = new Request(old.getMethod(), url);
    req.addHeaderValues(old.getHeaders());
    if (old.hasContent()) {
      req.setContent(old.getContent().copy());
    }
    return req;
  }
}
