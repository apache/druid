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

package io.druid.sql.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;

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
    // First is the inner sort, second is the outer sort.
    final Sort first = call.rel(1);
    final Sort second = call.rel(0);

    if (second.collation.getFieldCollations().isEmpty()) {
      // Add up the offsets.
      final int firstOffset = (first.offset != null ? RexLiteral.intValue(first.offset) : 0);
      final int secondOffset = (second.offset != null ? RexLiteral.intValue(second.offset) : 0);

      final int offset = firstOffset + secondOffset;
      final int fetch;

      if (first.fetch == null && second.fetch == null) {
        // Neither has a limit => no limit overall.
        fetch = -1;
      } else if (first.fetch == null) {
        // Outer limit only.
        fetch = RexLiteral.intValue(second.fetch);
      } else if (second.fetch == null) {
        // Inner limit only.
        fetch = Math.max(0, RexLiteral.intValue(first.fetch) - secondOffset);
      } else {
        fetch = Math.max(
            0,
            Math.min(
                RexLiteral.intValue(first.fetch) - secondOffset,
                RexLiteral.intValue(second.fetch)
            )
        );
      }

      final Sort combined = first.copy(
          first.getTraitSet(),
          first.getInput(),
          first.getCollation(),
          offset == 0 ? null : call.builder().literal(offset),
          call.builder().literal(fetch)
      );

      call.transformTo(combined);
      call.getPlanner().setImportance(second, 0.0);
    }
  }
}
