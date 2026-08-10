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

package org.apache.druid.server.system.handler;

import com.google.inject.Inject;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.segment.InlineSegmentWrangler;
import org.apache.druid.segment.Segment;
import org.apache.druid.server.DataSourceQueryHandler;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.system.table.SystemTableDataProvider;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTablePushdownFilter;

import java.util.Map;

/** Resolves one node-local system table and returns its rows through the standard native Scan stack. */
public class SystemTableQueryHandler implements DataSourceQueryHandler
{
  private final Map<String, SystemTableDataProvider> dataSuppliers;
  private final Map<String, SystemTableDescriptor> tableDescriptors;
  private final ScanQueryEngine scanQueryEngine;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public SystemTableQueryHandler(
      final Map<String, SystemTableDataProvider> dataSuppliers,
      final Map<String, SystemTableDescriptor> tableDescriptors,
      final ScanQueryEngine scanQueryEngine,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.dataSuppliers = dataSuppliers;
    this.tableDescriptors = tableDescriptors;
    this.scanQueryEngine = scanQueryEngine;
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public <T> QueryRunner<T> createRunner(
      final Query<T> query,
      final AuthenticationResult requestAuthenticationResult,
      final boolean executeLocally
  )
  {
    if (!(query instanceof ScanQuery)) {
      throw new IAE("Local system table queries must be scan queries");
    }

    final SystemTableDataSource dataSource = (SystemTableDataSource) query.getDataSource();
    final SystemTableDataProvider dataSupplier = dataSuppliers.get(dataSource.getTable());
    if (dataSupplier == null) {
      throw new ISE("System table[%s] is not served by this node", dataSource.getTable());
    }
    final SystemTableDescriptor descriptor = tableDescriptors.get(dataSource.getTable());
    if (descriptor == null) {
      throw new ISE("No descriptor is registered for system table[%s]", dataSource.getTable());
    }

    return (queryPlus, responseContext) -> {
      final Iterable<Object[]> suppliedRows = () -> dataSupplier.getRows(
          SystemTablePushdownFilter.extract(query, dataSupplier.getPushdownFilters()),
          requestAuthenticationResult
      ).iterator();
      final Iterable<Object[]> authorizedRows = descriptor.getRowAuthorizer().filterAuthorizedRows(
          suppliedRows,
          requestAuthenticationResult,
          authorizerMapper
      );
      final ScanQuery resolvedQuery = Druids.ScanQueryBuilder.copy((ScanQuery) query)
                                                       .dataSource(
                                                           InlineDataSource.fromIterable(
                                                               authorizedRows,
                                                               descriptor.getRowSignature()
                                                           )
                                                       )
                                                       .build();
      final Segment inlineSegment = new InlineSegmentWrangler()
          .getSegmentsForIntervals(resolvedQuery.getDataSource(), resolvedQuery.getIntervals())
          .iterator()
          .next();

      return runScan(
          scanQueryEngine,
          resolvedQuery,
          inlineSegment,
          queryPlus,
          responseContext
      );
    };
  }

  @SuppressWarnings("unchecked")
  private static <T> Sequence<T> runScan(
      final ScanQueryEngine scanQueryEngine,
      final ScanQuery query,
      final Segment segment,
      final QueryPlus<T> queryPlus,
      final ResponseContext responseContext
  )
  {
    ScanQuery.verifyOrderByForNativeExecution(query);
    initializeTimeout(query, responseContext);
    return (Sequence<T>) (Sequence<?>) scanQueryEngine.process(
        query,
        segment,
        responseContext,
        queryPlus.getQueryMetrics()
    );
  }

  private static void initializeTimeout(final ScanQuery query, final ResponseContext responseContext)
  {
    final Long existingTimeoutAt = responseContext.getTimeoutTime();
    if (existingTimeoutAt != null && existingTimeoutAt != 0L) {
      return;
    }

    final long failTime = query.context().getLong(DirectDruidClient.QUERY_FAIL_TIME, 0L);
    final long timeoutAt;
    if (failTime > 0L) {
      timeoutAt = failTime;
    } else if (query.context().hasTimeout()) {
      timeoutAt = System.currentTimeMillis() + query.context().getTimeout();
    } else {
      timeoutAt = JodaUtils.MAX_INSTANT;
    }
    responseContext.putTimeoutTime(timeoutAt);
  }

}
