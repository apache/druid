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

package io.druid.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.guice.annotations.Json;
import io.druid.query.history.QueryHistoryEntry;
import io.druid.query.history.QueryHistoryManager;
import io.druid.server.http.security.StateResourceFilter;
import io.druid.server.security.Access;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.ForbiddenException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Path("/druid/v2/history/")
public class QueryHistoryResource
{
  private final QueryManager queryManager;
  private final QueryHistoryManager queryHistoryManager;
  private final ObjectMapper jsonMapper;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public QueryHistoryResource(
      QueryManager queryManager,
      QueryHistoryManager queryHistoryManager,
      @Json ObjectMapper jsonMapper,
      AuthorizerMapper authorizerMapper
  )
  {

    this.queryManager = queryManager;
    this.queryHistoryManager = queryHistoryManager;
    this.jsonMapper = jsonMapper;
    this.authorizerMapper = authorizerMapper;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public List<String> getQueryIDHistory(
      @Context final HttpServletRequest req,
      @QueryParam("sql") final String sqlOnly
  )
  {
    if (sqlOnly == null) {
      return queryHistoryManager.fetchQueryIDHistory();
    }
    return queryHistoryManager.fetchSqlQueryIDHistory();
  }

  @GET
  @Path("{queryID}")
  @Produces(MediaType.APPLICATION_JSON)
  public DetailedEntry getDetailedQueryHistoryByQueryID(@PathParam("queryID") String queryID, @Context final HttpServletRequest req)
  {
    DetailedEntry detail = getDetailedQueryHistoryByQueryID0(queryID);
    Access authResult = AuthorizationUtils.authorizeAllResourceActions(
        req,
        Iterables.transform(detail.getDatasources(), AuthorizationUtils.DATASOURCE_WRITE_RA_GENERATOR),
        authorizerMapper
    );
    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
    }
    return detail;
  }

  private DetailedEntry getDetailedQueryHistoryByQueryID0(String queryID)
  {
    List<QueryHistoryEntry> entries = queryHistoryManager.fetchQueryHistoryByQueryID(queryID);
    return createDetail(entries);
  }

  private DetailedEntry createDetail(List<QueryHistoryEntry> entries)
  {
    if (entries.isEmpty()) {
      throw new IllegalArgumentException("empty entry list");
    }
    String queryID = null;
    Map<String, Long> profile = new TreeMap<>();
    String sql = null;
    String createDate = null;
    List<String> datasources = null;

    for (QueryHistoryEntry entry : entries) {
      try {
        Map<String, Object> paylod = jsonMapper.readValue(entry.getPayload(), new TypeReference<Map<String, Object>>() {
        });
        String type = entry.getType();
        switch (type) {
          case QueryHistoryEntry.TYPE_BROKER_TIME:
            queryID = entry.getQueryID();
            createDate = entry.getCreatedDate().toString();
            profile.put("broker", ((Number) paylod.get("time")).longValue());
            break;
          case QueryHistoryEntry.TYPE_NODE_TIME:
            profile.put((String) paylod.get("host"), ((Number) paylod.get("time")).longValue());
            break;
          case QueryHistoryEntry.TYPE_SQL_QUERY_TEXT:
            sql = (String) paylod.get("text");
            break;
          case QueryHistoryEntry.TYPE_DATASOURCES:
            //noinspection unchecked
            datasources = (List<String>) paylod.get("datasources");
            break;
          default:
            throw new IllegalStateException();
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    if (queryID == null) {
      throw new IllegalArgumentException("query id missing: " + entries);
    }
    if (datasources == null) {
      throw new IllegalArgumentException("datasources missing: " + entries);
    }
    boolean running = queryManager.isRunningQuery(queryID);
    String status = running ? "RUNNING" : "FINISHED";
    return new DetailedEntry(
        queryID,
        profile,
        sql,
        createDate,
        status,
        datasources
    );
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class DetailedEntry
  {
    private final String queryID;
    private final Map<String, Long> profile;
    private final String sql;
    private final String createDate;
    private final String status;
    private final List<String> datasources;

    @JsonCreator
    public DetailedEntry(
        @JsonProperty("queryID") String queryID,
        @JsonProperty("profile") Map<String, Long> profile,
        @JsonProperty("sql") String sql,
        @JsonProperty("createDate") String createDate,
        @JsonProperty("status") String status,
        @JsonProperty("datasources") List<String> datasources
    )
    {
      this.queryID = queryID;
      this.profile = profile;
      this.sql = sql;
      this.createDate = createDate;
      this.status = status;
      this.datasources = datasources;
    }

    @JsonProperty
    public String getQueryID()
    {
      return queryID;
    }

    @JsonProperty
    public Map<String, Long> getProfile()
    {
      return profile;
    }

    @JsonProperty
    public String getSql()
    {
      return sql;
    }

    @JsonProperty
    public String getCreateDate()
    {
      return createDate;
    }

    @JsonProperty
    public String getStatus()
    {
      return status;
    }

    @JsonProperty
    public List<String> getDatasources()
    {
      return datasources;
    }
  }
}
