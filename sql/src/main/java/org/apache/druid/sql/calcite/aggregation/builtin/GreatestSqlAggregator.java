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

package org.apache.druid.sql.calcite.aggregation.builtin;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.LongGreatestPostAggregator;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.List;

public class GreatestSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new GreatestSqlAggFunction();
  private static final String NAME = "GREATEST";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final VirtualColumnRegistry virtualColumnRegistry,
      final RexBuilder rexBuilder,
      final String name,
      final AggregateCall aggregateCall,
      final Project project,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
  )
  {
    // TODO: implement
    List<PostAggregator> postAggregators = Lists.newArrayList(
            new ConstantPostAggregator(
                "roku2", 2L
            ), new ConstantPostAggregator(
                "roku3", 3L
            )
        );

    return Aggregation.create(new LongGreatestPostAggregator(name, postAggregators));
  }

  private static class GreatestSqlAggFunction extends SqlAggFunction
  {
    GreatestSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.GREATEST,
          ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
          null,
          OperandTypes.SAME_VARIADIC,
          SqlFunctionCategory.SYSTEM,
          false,
          false
      );
    }
  }
}
