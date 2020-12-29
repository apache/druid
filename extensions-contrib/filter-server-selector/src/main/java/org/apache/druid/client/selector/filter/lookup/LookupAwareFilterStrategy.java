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

package org.apache.druid.client.selector.filter.lookup;

import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.filter.ServerFilterStrategy;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.extraction.CascadeExtractionFn;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.lookup.RegisteredLookupExtractionFn;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LookupAwareFilterStrategy implements ServerFilterStrategy
{
  public static final String LOOKUPS_CONTEXT_KEY = "lookups";
  public static final String QUERY_LOOKUPS_CACHE_KEY = "__lookups";
  private final LookupStatusView lookupStatusView;

  public LookupAwareFilterStrategy(LookupStatusView lookupStatusView)
  {
    this.lookupStatusView = lookupStatusView;
  }

  @Override
  public <T> Set<QueryableDruidServer> filter(Query<T> query, Set<QueryableDruidServer> servers)
  {
    if (query == null) {
      return servers;
    }
    final Set<String> lookups;
    if (query.getContext() != null
        && query.getContext().containsKey(QUERY_LOOKUPS_CACHE_KEY)) {
      lookups = (Set<String>) query.getContext().get(QUERY_LOOKUPS_CACHE_KEY);
    } else {
      lookups = new HashSet<String>();
      findLookupsInQuery(query, lookups);
      if (query.getContext() != null) {
        try {
          query.getContext().put(QUERY_LOOKUPS_CACHE_KEY, lookups);
        }
        catch (UnsupportedOperationException e) {
          // TODO This is for immutable contexts, what to do with it?
        }
      }
    }
    if (lookups.size() == 0) {
      return servers;
    }

    return servers.stream()
        .filter(server -> lookupStatusView.hasAllLookups(server.getServer().getHostAndPort().toString(), lookups))
        .collect(Collectors.toSet());
  }

  private <T> Set<String> findLookupsInQuery(Query<T> query, Set<String> lookupsInQuery)
  {
    if (query.getContext() != null && query.getContext().containsKey(LOOKUPS_CONTEXT_KEY)) {
      Object userLookups = query.getContext().get(LOOKUPS_CONTEXT_KEY);
      if (userLookups instanceof Collection) {
        lookupsInQuery.addAll(((Collection) userLookups));
      }
    }

    findLookupsInDataSources(query.getDataSource(), lookupsInQuery);
    findLookupsInDimensions(query, lookupsInQuery);
    findLookupsInVirtualColumns(query, lookupsInQuery);

    return lookupsInQuery;
  }

  public <T> void findLookupsInVirtualColumns(Query<T> query, Set<String> lookupsInQuery)
  {
    VirtualColumns virts = query.getVirtualColumns();
    for (VirtualColumn vc : virts.getVirtualColumns()) {
      if (vc instanceof ExpressionVirtualColumn) {
        ExpressionVirtualColumn evc = (ExpressionVirtualColumn) vc;
        Expr expr = evc.getParsedExpression().get();
        Matcher m = Pattern.compile("lookup\\(.*?, '(.*?)'\\)").matcher(expr.stringify());
        if (m.find()) {
          lookupsInQuery.add(m.group(1));
        }
      }
    }
  }

  public void findLookupsInDataSources(DataSource dataSource, Set<String> lookupsInQuery)
  {
    if (dataSource instanceof LookupDataSource) {
      lookupsInQuery.add(((LookupDataSource) dataSource).getLookupName());
    }
    if (dataSource instanceof JoinDataSource) {
      JoinDataSource jds = (JoinDataSource) dataSource;
      findLookupsInDataSources(jds.getLeft(), lookupsInQuery);
      findLookupsInDataSources(jds.getRight(), lookupsInQuery);
    }
  }

  public <T> void findLookupsInDimensions(Query<T> query, Set<String> lookupsInQuery)
  {
    if (query instanceof TopNQuery) {
      TopNQuery tnq = (TopNQuery) (query);
      addLookupsFromDimension(tnq.getDimensionSpec(), lookupsInQuery);
    } else if (query instanceof GroupByQuery) {
      GroupByQuery gbq = (GroupByQuery) (query);
      for (DimensionSpec d : gbq.getDimensions()) {
        addLookupsFromDimension(d, lookupsInQuery);
      }
    } else if (query instanceof SearchQuery) {
      SearchQuery gbq = (SearchQuery) (query);
      for (DimensionSpec d : gbq.getDimensions()) {
        addLookupsFromDimension(d, lookupsInQuery);
      }
    }
  }

  private void addLookupsFromDimension(DimensionSpec dimensionSpec, Set<String> lookupsInQuery)
  {
    if (dimensionSpec instanceof ExtractionDimensionSpec) {
      addLookupsFromExtractionFunction(((ExtractionDimensionSpec) dimensionSpec).getExtractionFn(), lookupsInQuery);
    }
  }

  private void addLookupsFromExtractionFunction(ExtractionFn fn, Set<String> lookupsInQuery)
  {
    if (fn instanceof RegisteredLookupExtractionFn) {
      lookupsInQuery.add(((RegisteredLookupExtractionFn) fn).getLookup());
    }
    if (fn instanceof CascadeExtractionFn) {
      for (ExtractionFn subExpr : ((CascadeExtractionFn) fn).getExtractionFns()) {
        addLookupsFromExtractionFunction(subExpr, lookupsInQuery);
      }
    }
  }
}
