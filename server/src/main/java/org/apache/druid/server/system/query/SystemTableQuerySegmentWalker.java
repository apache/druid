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

package org.apache.druid.server.system.query;

import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.planning.ExecutionVertex;
import org.apache.druid.server.LocalQuerySegmentWalker;
import org.apache.druid.server.SetAndVerifyContextQueryRunner;
import org.apache.druid.server.initialization.ServerConfig;
import org.joda.time.Interval;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Runs queries on system tables on services whose own walker reads only segments, such as Historicals and the
 * Coordinator, and passes every other query to that walker.
 *
 * <p>A query on a system table reads this service's rows of the table through {@link LocalQuerySegmentWalker}, which
 * gets them from the system-table segment wrangler. The Broker doesn't need this walker: its
 * {@link org.apache.druid.server.ClientQuerySegmentWalker} already runs queries on non-table datasources locally.</p>
 */
public class SystemTableQuerySegmentWalker implements QuerySegmentWalker
{
  /**
   * Binds the walker that this walker passes queries to when they don't read a system table.
   */
  @BindingAnnotation
  @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Delegate
  {
  }

  private final QuerySegmentWalker delegate;
  private final LocalQuerySegmentWalker localWalker;
  private final ServerConfig serverConfig;

  @Inject
  public SystemTableQuerySegmentWalker(
      @Delegate final QuerySegmentWalker delegate,
      final LocalQuerySegmentWalker localWalker,
      final ServerConfig serverConfig
  )
  {
    this.delegate = delegate;
    this.localWalker = localWalker;
    this.serverConfig = serverConfig;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    if (!SystemTableDataSource.readsSystemTable(query.getDataSource())) {
      return delegate.getQueryRunnerForIntervals(query, intervals);
    }
    if (readsUnauthorizedSystemTable(query.getDataSource())) {
      throw DruidException.defensive("System tables in query[%s] were not authorized before execution", query.getId());
    }
    if (!ExecutionVertex.of(query).canRunQueryUsingLocalWalker()) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.UNSUPPORTED)
                          .build(
                              "This service can't run a query on dataSource[%s]. Query the system table directly, "
                              + "or use SQL.",
                              query.getDataSource()
                          );
    }
    // LocalQuerySegmentWalker doesn't apply the server's timeout and scatter-gather limits, unlike the walkers that
    // read segments, so apply them here.
    return new SetAndVerifyContextQueryRunner<>(serverConfig, localWalker.getQueryRunnerForIntervals(query, intervals));
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    if (SystemTableDataSource.readsSystemTable(query.getDataSource())) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.UNSUPPORTED)
                          .build("System tables can't be queried for specific segments");
    }
    return delegate.getQueryRunnerForSegments(query, specs);
  }

  private static boolean readsUnauthorizedSystemTable(final DataSource dataSource)
  {
    if (dataSource instanceof SystemTableDataSource systemTable) {
      return systemTable.getAccess() == null;
    }
    for (final DataSource child : dataSource.getChildren()) {
      if (readsUnauthorizedSystemTable(child)) {
        return true;
      }
    }
    return false;
  }
}
