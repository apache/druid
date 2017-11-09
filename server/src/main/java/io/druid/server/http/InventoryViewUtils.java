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

package io.druid.server.http;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.client.DruidServer;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.InventoryView;
import io.druid.java.util.common.ISE;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.AuthorizerMapper;

import javax.servlet.http.HttpServletRequest;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.StreamSupport;

public class InventoryViewUtils
{

  public static Set<ImmutableDruidDataSource> getDataSources(InventoryView serverInventoryView)
  {
    final TreeSet<ImmutableDruidDataSource> dataSources = Sets.newTreeSet(
        Comparator.comparing(ImmutableDruidDataSource::getName)
    );
    StreamSupport
        .stream(serverInventoryView.getInventory().spliterator(), false)
        .map(DruidServer::getDataSources)
        .flatMap(Collection::stream)
        .forEach(dataSources::add);
    return dataSources;
  }

  public static Set<ImmutableDruidDataSource> getSecuredDataSources(
      HttpServletRequest request,
      InventoryView inventoryView,
      final AuthorizerMapper authorizerMapper
  )
  {
    if (authorizerMapper == null) {
      throw new ISE("No authorization mapper found");
    }

    return ImmutableSet.copyOf(
        AuthorizationUtils.filterAuthorizedResources(
            request,
            getDataSources(inventoryView),
            datasource -> Lists.newArrayList(
                AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(datasource.getName())
            ),
            authorizerMapper
        )
    );
  }
}
