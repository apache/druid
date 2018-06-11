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

import com.google.common.collect.Lists;
import io.druid.client.DruidDataSource;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.InventoryView;
import io.druid.java.util.common.ISE;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.AuthorizerMapper;

import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

public interface InventoryViewUtils
{
  static Comparator<ImmutableDruidDataSource> comparingByName()
  {
    return Comparator.comparing(ImmutableDruidDataSource::getName);
  }

  static SortedSet<ImmutableDruidDataSource> getDataSources(InventoryView serverInventoryView)
  {
    return serverInventoryView.getInventory()
                              .stream()
                              .flatMap(server -> server.getDataSources().stream())
                              .map(DruidDataSource::toImmutableDruidDataSource)
                              .collect(
                                  () -> new TreeSet<>(comparingByName()),
                                  TreeSet::add,
                                  TreeSet::addAll
                              );
  }

  static SortedSet<ImmutableDruidDataSource> getSecuredDataSources(
      HttpServletRequest request,
      InventoryView inventoryView,
      final AuthorizerMapper authorizerMapper
  )
  {
    if (authorizerMapper == null) {
      throw new ISE("No authorization mapper found");
    }

    Iterable<ImmutableDruidDataSource> filteredResources = AuthorizationUtils.filterAuthorizedResources(
        request,
        getDataSources(inventoryView),
        datasource -> Lists.newArrayList(
            AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(datasource.getName())
        ),
        authorizerMapper
    );
    SortedSet<ImmutableDruidDataSource> set = new TreeSet<>(comparingByName());
    filteredResources.forEach(set::add);
    return Collections.unmodifiableSortedSet(set);
  }
}
