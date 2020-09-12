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

package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.druid.sql.calcite.planner.OffsetLimit;

/**
 * Collapses two adjacent Sort operations together. Useful for queries like
 * {@code SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2) LIMIT 10}.
 */
public class SortCollapseRule extends RelOptRule
{
  private static final SortCollapseRule INSTANCE = new SortCollapseRule();

  public SortCollapseRule()
  {
    super(operand(Sort.class, operand(Sort.class, any())));
  }

  public static SortCollapseRule instance()
  {
    return INSTANCE;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Sort outerSort = call.rel(0);
    final Sort innerSort = call.rel(1);

    if (outerSort.collation.getFieldCollations().isEmpty()
        || outerSort.collation.getFieldCollations().equals(innerSort.collation.getFieldCollations())) {
      final OffsetLimit offsetLimit = OffsetLimit.fromSort(innerSort).andThen(OffsetLimit.fromSort(outerSort));
      final Sort combined = innerSort.copy(
          innerSort.getTraitSet(),
          innerSort.getInput(),
          innerSort.getCollation(),
          offsetLimit.getOffsetAsRexNode(call.builder().getRexBuilder()),
          offsetLimit.getLimitAsRexNode(call.builder().getRexBuilder())
      );

      call.transformTo(combined);
      call.getPlanner().setImportance(outerSort, 0.0);
    }
  }
}
