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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.rel.RelRoot;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.NativeQueryMakerFactory;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.QueryMakerFactory;
import org.apache.druid.sql.calcite.table.RowSignatures;

public class TestQueryMakerFactory implements QueryMakerFactory
{
  private final QueryLifecycleFactory queryLifecycleFactory;
  private final ObjectMapper jsonMapper;

  TestQueryMakerFactory(
      final QueryLifecycleFactory queryLifecycleFactory,
      final ObjectMapper jsonMapper
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public QueryMaker buildForSelect(RelRoot relRoot, PlannerContext plannerContext)
  {
    return new NativeQueryMakerFactory(queryLifecycleFactory, jsonMapper).buildForSelect(relRoot, plannerContext);
  }

  @Override
  public QueryMaker buildForInsert(String targetDataSource, RelRoot relRoot, PlannerContext plannerContext)
  {
    final RowSignature signature = RowSignatures.fromRelDataType(
        relRoot.validatedRowType.getFieldNames(),
        relRoot.validatedRowType
    );

    return new TestInsertQueryMaker(relRoot.rel.getCluster().getTypeFactory(), targetDataSource, signature);
  }
}
