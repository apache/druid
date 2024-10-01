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

package org.apache.druid.query.union;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

public class RealUnionQueryRunner implements QueryRunner<RowsAndColumns>
{
  private UnionQuery unionQuery;
  private QuerySegmentWalker walker;

  public RealUnionQueryRunner(UnionQuery unionQuery, QuerySegmentWalker walker)
  {
    this.unionQuery = unionQuery;
    this.walker = walker;

  }

  @Override
  public Sequence<RowsAndColumns> run(QueryPlus<RowsAndColumns> queryPlus, ResponseContext responseContext)
  {
    queryPlus.unwrap(UnionQuery.class);
    UnionQuery query = (UnionQuery) queryPlus.getQuery();
    if (!(query instanceof ScanQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", query.getClass(), ScanQuery.class);
    }

    ScanQuery.verifyOrderByForNativeExecution((ScanQuery) query);

    // it happens in unit tests
    final Long timeoutAt = responseContext.getTimeoutTime();
    if (timeoutAt == null || timeoutAt == 0L) {
      responseContext.putTimeoutTime(JodaUtils.MAX_INSTANT);
    }
    return engine.process((ScanQuery) query, segment, responseContext, queryPlus.getQueryMetrics());
  }



}
