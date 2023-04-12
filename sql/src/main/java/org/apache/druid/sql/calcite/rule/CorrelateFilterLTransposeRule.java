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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;

/**
 * Rule that pulls a {@link Filter} from the left-hand side of a {@link Correlate} above the Correlate.
 * Allows subquery elimination.
 *
 */
public class CorrelateFilterLTransposeRule extends RelOptRule
{
  private static final CorrelateFilterLTransposeRule INSTANCE = new CorrelateFilterLTransposeRule();

  public CorrelateFilterLTransposeRule()
  {
    super(
        operand(
            Correlate.class,
            operand(Filter.class, any()),
            operand(RelNode.class, any())
        ));
  }

  public static CorrelateFilterLTransposeRule instance()
  {
    return INSTANCE;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Correlate correlate = call.rel(0);
    final Filter left = call.rel(1);
    final RelNode right = call.rel(2);

    call.transformTo(
        call.builder()
            .push(correlate.copy(correlate.getTraitSet(), ImmutableList.of(left.getInput(), right)))
            .filter(left.getCondition())
            .build()
    );
  }
}
