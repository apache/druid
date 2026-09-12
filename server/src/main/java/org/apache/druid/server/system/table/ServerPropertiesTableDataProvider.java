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

package org.apache.druid.server.system.table;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Native row supplier for {@code sys.server_properties}. */
public class ServerPropertiesTableDataProvider implements SystemTableDataProvider
{
  private static final List<SystemTablePushdownFilter> PUSHDOWN_FILTERS = List.of(
      new SystemTablePushdownFilter("server", null),
      new SystemTablePushdownFilter("service_name", null)
  );

  private final DruidNode selfNode;
  private final Set<NodeRole> selfNodeRoles;
  private final AuthorizerMapper authorizerMapper;
  private final Properties properties;
  private final DruidServerConfig druidServerConfig;

  @Inject
  public ServerPropertiesTableDataProvider(
      @Self final DruidNode selfNode,
      @Self final Set<NodeRole> selfNodeRoles,
      final AuthorizerMapper authorizerMapper,
      final Properties properties,
      final DruidServerConfig druidServerConfig
  )
  {
    this.selfNode = selfNode;
    this.selfNodeRoles = selfNodeRoles;
    this.authorizerMapper = authorizerMapper;
    this.properties = properties;
    this.druidServerConfig = druidServerConfig;
  }

  @Override
  public List<SystemTablePushdownFilter> getPushdownFilters()
  {
    return PUSHDOWN_FILTERS;
  }

  @Override
  public Iterable<Object[]> getRows(
      final List<DimFilter> filters,
      final AuthenticationResult internalAuthenticationResult
  )
  {
    authorizeServerRead(internalAuthenticationResult);

    String serverFilter = null;
    String serviceNameFilter = null;
    for (final DimFilter filter : filters) {
      if (isStringEqualityFilter(filter, "server")) {
        serverFilter = getFilterValue(filter);
      } else if (isStringEqualityFilter(filter, "service_name")) {
        serviceNameFilter = getFilterValue(filter);
      }
    }

    final String server = selfNode.getHostAndPortToUse();
    if (serverFilter != null && !serverFilter.equals(server)) {
      return Collections.emptyList();
    }
    if (serviceNameFilter != null && !serviceNameFilter.equals(selfNode.getServiceName())) {
      return Collections.emptyList();
    }

    final ServerProperties serverProperties = new ServerProperties(selfNode.getServiceName(), server);
    selfNodeRoles.stream()
                 .map(NodeRole::getJsonName)
                 .sorted()
                 .forEach(serverProperties.nodeRoles::add);

    return buildRows(serverProperties);
  }

  private void authorizeServerRead(final AuthenticationResult authenticationResult)
  {
    final AuthorizationResult authorizationResult = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        authorizerMapper
    );
    if (!authorizationResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(
          "Insufficient permission to view servers: " + authorizationResult.getErrorMessage()
      );
    }
  }

  private List<Object[]> buildRows(final ServerProperties serverProperties)
  {
    final Map<String, String> localProperties = getProperties();
    final String nodeRoles = serverProperties.nodeRoles.toString();
    if (localProperties.isEmpty()) {
      return Collections.singletonList(
          row(
              serverProperties.server,
              serverProperties.serviceName,
              nodeRoles,
              null,
              null,
              null
          )
      );
    }

    return localProperties.entrySet()
                           .stream()
                           .map(entry -> row(
                               serverProperties.server,
                               serverProperties.serviceName,
                               nodeRoles,
                               entry.getKey(),
                               entry.getValue(),
                               null
                           ))
                           .collect(Collectors.toList());
  }

  private Object[] row(
      final String server,
      final String serviceName,
      final String nodeRoles,
      @Nullable final String property,
      @Nullable final String value,
      @Nullable final String error
  )
  {
    return new Object[]{server, serviceName, nodeRoles, property, value, error};
  }

  private Map<String, String> getProperties()
  {
    final Map<String, String> allProperties = Maps.fromProperties(properties);
    final Set<String> hiddenProperties = druidServerConfig.getHiddenProperties();
    final Map<String, String> filteredProperties = new HashMap<>(allProperties);
    filteredProperties.keySet().removeIf(
        key -> hiddenProperties.stream().anyMatch(
            hiddenProperty -> StringUtils.toLowerCase(key).contains(StringUtils.toLowerCase(hiddenProperty))
        )
    );
    return new TreeMap<>(filteredProperties);
  }

  private static String getFilterValue(final DimFilter filter)
  {
    if (filter instanceof SelectorDimFilter) {
      return ((SelectorDimFilter) filter).getValue();
    }
    return (String) ((EqualityFilter) filter).getMatchValue();
  }

  private static boolean isStringEqualityFilter(final DimFilter filter, final String column)
  {
    return filter instanceof SelectorDimFilter selector && column.equals(selector.getDimension())
           || filter instanceof EqualityFilter equality && column.equals(equality.getColumn());
  }

  private static class ServerProperties
  {
    private final String serviceName;
    private final String server;
    private final List<String> nodeRoles = new ArrayList<>();

    private ServerProperties(final String serviceName, final String server)
    {
      this.serviceName = serviceName;
      this.server = server;
    }
  }
}
