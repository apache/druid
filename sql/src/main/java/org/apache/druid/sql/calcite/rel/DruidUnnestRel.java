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

package org.apache.druid.sql.calcite.rel;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;

/**
 * Captures the unnest (i.e. {@link Uncollect}) part of a correlated unnesting join.
 *
 * This rel cannot be executed directly. It is a holder of information for {@link DruidCorrelateUnnestRel}.
 *
 * Unnest on literal values, without correlated join, is handled directly by
 * {@link org.apache.druid.sql.calcite.rule.DruidUnnestRule}. is applied without a correlated join, This covers the case where an unnest has an
 * input table, this rel resolves the unnest part and delegates the rel to be consumed by other
 * rule ({@link org.apache.druid.sql.calcite.rule.DruidCorrelateUnnestRule}
 */
public class DruidUnnestRel extends DruidRel<DruidUnnestRel>
{
  private static final TableDataSource DUMMY_DATA_SOURCE = new TableDataSource("__unnest__");

  /**
   * An {@link Uncollect} on top of a {@link Project} that generates an expression to be unnested. The underlying
   * {@link Project} is not expected to reference any bits of its input; instead it references either a constant or
   * a correlation variable through a {@link org.apache.calcite.rex.RexFieldAccess}.
   */
  private final Uncollect uncollect;

  private DruidUnnestRel(
      final RelOptCluster cluster,
      final RelTraitSet traits,
      final Uncollect uncollect,
      final PlannerContext plannerContext
  )
  {
    super(cluster, traits, plannerContext);
    this.uncollect = uncollect;

    if (!(uncollect.getInputs().get(0) instanceof Project)) {
      // Validate that the Uncollect reads from a Project.
      throw new ISE(
          "Uncollect must reference Project, but child was [%s]",
          uncollect.getInputs().get(0)
      );
    }
  }

  public static DruidUnnestRel create(final Uncollect uncollect, final PlannerContext plannerContext)
  {
    return new DruidUnnestRel(
        uncollect.getCluster(),
        uncollect.getTraitSet(),
        uncollect,
        plannerContext
    );
  }

  /**
   * Uncollect (unnest) operation that references the {@link #getUnnestProject()}.
   */
  public Uncollect getUncollect()
  {
    return uncollect;
  }

  /**
   * Project that generates the expression to be unnested.
   */
  public Project getUnnestProject()
  {
    return (Project) uncollect.getInputs().get(0);
  }

  @Override
  public PartialDruidQuery getPartialDruidQuery()
  {
    return null;
  }

  @Override
  public DruidUnnestRel withPartialQuery(PartialDruidQuery newQueryBuilder)
  {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns a new rel with the {@link #getUnnestProject()} replaced.
   */
  public DruidUnnestRel withUnnestProject(final Project newUnnestProject)
  {
    return new DruidUnnestRel(
        getCluster(),
        getTraitSet(),
        (Uncollect) uncollect.copy(
            uncollect.getTraitSet(),
            newUnnestProject
        ),
        getPlannerContext()
    );
  }

  @Override
  public DruidQuery toDruidQuery(boolean finalizeAggregations)
  {
    // DruidUnnestRel is a holder for info for DruidCorrelateUnnestRel. It cannot be executed on its own.
    throw new CannotBuildQueryException("Cannot execute UNNEST directly");
  }

  @Override
  public DruidQuery toDruidQueryForExplaining()
  {
    return PartialDruidQuery
        .create(uncollect)
        .build(
            DUMMY_DATA_SOURCE,
            RowSignatures.fromRelDataType(
                uncollect.getRowType().getFieldNames(),
                uncollect.getRowType()
            ),
            getPlannerContext(),
            getCluster().getRexBuilder(),
            false
        );
  }

  @Nullable
  @Override
  public DruidUnnestRel asDruidConvention()
  {
    return new DruidUnnestRel(
        getCluster(),
        getTraitSet().replace(DruidConvention.instance()),
        uncollect,
        getPlannerContext()
    );
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    final String queryString;
    final DruidQuery druidQuery = toDruidQueryForExplaining();

    try {
      queryString = getPlannerContext().getJsonMapper().writeValueAsString(druidQuery.getQuery());
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return pw.item("unnestProject", getUnnestProject())
             .item("uncollect", getUncollect())
             .item("query", queryString)
             .item("signature", druidQuery.getOutputRowSignature());
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    return Collections.emptySet();
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return uncollect.getRowType();
  }
}
