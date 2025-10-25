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

package org.apache.druid.sql.calcite.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class SystemPropertiesTable extends AbstractTable implements ScannableTable
{
  public static final String PROPERTIES_TABLE = "properties";

  static final RowSignature PROPERTIES_SIGNATURE = RowSignature
      .builder()
      .add("service", ColumnType.STRING)
      .add("host", ColumnType.STRING)
      .add("server_type", ColumnType.STRING)
      .add("property", ColumnType.STRING)
      .add("value", ColumnType.STRING)
      .build();

  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final AuthorizerMapper authorizerMapper;
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;

  public SystemPropertiesTable(
      DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      AuthorizerMapper authorizerMapper,
      HttpClient httpClient,
      ObjectMapper jsonMapper
  )
  {
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.authorizerMapper = authorizerMapper;
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory)
  {
    return RowSignatures.toRelDataType(PROPERTIES_SIGNATURE, typeFactory);
  }

  @Override
  public Schema.TableType getJdbcTableType()
  {
    return Schema.TableType.SYSTEM_TABLE;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root)
  {
    final AuthenticationResult authenticationResult = (AuthenticationResult) Preconditions.checkNotNull(
        root.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT),
        "authenticationResult in dataContext"
    );
    SystemSchema.checkStateReadAccessForServers(authenticationResult, authorizerMapper);
    final Iterator<DiscoveryDruidNode> druidServers = SystemSchema.getDruidServers(druidNodeDiscoveryProvider);

    final FluentIterable<Object[]> results = FluentIterable
        .from(() -> druidServers)
        .transformAndConcat((DiscoveryDruidNode discoveryDruidNode) -> {
          final DruidNode druidNode = discoveryDruidNode.getDruidNode();
          final Map<String, String> propertiesMap = getProperties(druidNode);
          return propertiesMap.entrySet().stream()
                              .map(entry -> new Object[]{
                                  druidNode.getServiceName(),
                                  druidNode.getHost(),
                                  discoveryDruidNode.getNodeRole().getJsonName(),
                                  entry.getKey(),
                                  entry.getValue()
                              })
                              .collect(Collectors.toList());
        });
    return Linq4j.asEnumerable(results);
  }

  private Map<String, String> getProperties(DruidNode druidNode)
  {
    try {
      final String url = druidNode.getUriToUse().resolve("/status/properties").toString();
      final Request request = new Request(HttpMethod.GET, new URL(url));
      final StringFullResponseHolder response;
      try {
        response = httpClient
            .go(request, new StringFullResponseHandler(StandardCharsets.UTF_8))
            .get();
      }
      catch (ExecutionException e) {
        throw new RE(e, "HTTP request to[%s] failed", request.getUrl());
      }

      if (response.getStatus().getCode() != HttpServletResponse.SC_OK) {
        throw new RE(
            "Failed to get properties from node at [%s]. Error code [%d], description [%s].",
            url,
            response.getStatus().getCode(),
            response.getStatus().getReasonPhrase()
        );
      }
      return jsonMapper.readValue(
          response.getContent(), new TypeReference<Map<String, String>>()
          {
          }
      );
    }
    catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
