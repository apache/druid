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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceType;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class InventoryViewUtils
{

  public static Set<DruidDataSource> getDataSources(InventoryView serverInventoryView)
  {
    TreeSet<DruidDataSource> dataSources = Sets.newTreeSet(
        new Comparator<DruidDataSource>()
        {
          @Override
          public int compare(DruidDataSource druidDataSource, DruidDataSource druidDataSource1)
          {
            return druidDataSource.getName().compareTo(druidDataSource1.getName());
          }
        }
    );
    dataSources.addAll(
        Lists.newArrayList(
            Iterables.concat(
                Iterables.transform(
                    serverInventoryView.getInventory(),
                    new Function<DruidServer, Iterable<DruidDataSource>>()
                    {
                      @Override
                      public Iterable<DruidDataSource> apply(DruidServer input)
                      {
                        return input.getDataSources();
                      }
                    }
                )
            )
        )
    );
    return dataSources;
  }

  public static Set<DruidDataSource> getSecuredDataSources(
      InventoryView inventoryView,
      final AuthorizationInfo authorizationInfo
  )
  {
    if (authorizationInfo == null) {
      throw new ISE("Invalid to call a secured method with null AuthorizationInfo!!");
    } else {
      final Map<Pair<Resource, Action>, Access> resourceAccessMap = new HashMap<>();
      return ImmutableSet.copyOf(
          Iterables.filter(
              getDataSources(inventoryView),
              new Predicate<DruidDataSource>()
              {
                @Override
                public boolean apply(DruidDataSource input)
                {
                  Resource resource = new Resource(input.getName(), ResourceType.DATASOURCE);
                  Action action = Action.READ;
                  Pair<Resource, Action> key = new Pair<>(resource, action);
                  if (resourceAccessMap.containsKey(key)) {
                    return resourceAccessMap.get(key).isAllowed();
                  } else {
                    Access access = authorizationInfo.isAuthorized(key.lhs, key.rhs);
                    resourceAccessMap.put(key, access);
                    return access.isAllowed();
                  }
                }
              }
          )
      );
    }
  }
}
