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
package org.apache.druid.client;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;

import java.util.Map;

public class SimpleQueryRunner implements QueryRunner<Object>
{
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final QueryableIndexSegment segment;

  public SimpleQueryRunner(QueryRunnerFactoryConglomerate conglomerate, String segmentId, QueryableIndex queryableIndex)
  {
    this.conglomerate = conglomerate;
    this.segment = new QueryableIndexSegment(segmentId, queryableIndex);
  }

  @Override
  public Sequence<Object> run(QueryPlus<Object> queryPlus, Map<String, Object> responseContext)
  {
    final QueryRunnerFactory factory = conglomerate.findFactory(queryPlus.getQuery());
    //noinspection unchecked
    return factory.getToolchest().preMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            new BySegmentQueryRunner<>(
                segment.getIdentifier(),
                segment.getDataInterval().getStart(),
                factory.createRunner(segment)
            ),
            factory.getToolchest()
        )
    ).run(queryPlus, responseContext);
  }
}
