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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

public class DruidAggregateRemoveRule extends AggregateRemoveRule
{
  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Aggregate aggregate = call.rel(0);
    boolean status = isSortUnderAggregate(call, aggregate);
    if (status) {
      return;
    }
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      final SqlAggFunction aggregation = aggCall.getAggregation();
      if (!(aggregation.getKind() == SqlKind.SUM || aggregation.getKind() == SqlKind.COUNT)) {
        // Only allow AVG to trigger Calcite's AggregateRemoveRule
        // intercept others from here and return
        return;
      }
    }
    super.onMatch(call);
  }

  private static boolean isSortUnderAggregate(RelOptRuleCall call, Aggregate aggregate)
  {
    final RelNode input = aggregate.getInput();
    final RelMetadataQuery mq = call.getMetadataQuery();
    List<RelCollation> collations = mq.collations(input);
    assert collations != null;
    boolean status = false;
    // checking if the input of the aggregate has a sort
    // do not invoke Calcite's AggregateRemove if aggregate has a sort underneath
    for (RelCollation r : collations) {
      final List<RelFieldCollation> fieldCollations = r.getFieldCollations();
      if (!fieldCollations.isEmpty()) {
        status = true;
      }
    }
    return status;
  }

  public static DruidAggregateRemoveRule toRule()
  {
    return new DruidAggregateRemoveRule(Config.DEFAULT);
  }

  protected DruidAggregateRemoveRule(Config config)
  {
    super(config);
  }
}
