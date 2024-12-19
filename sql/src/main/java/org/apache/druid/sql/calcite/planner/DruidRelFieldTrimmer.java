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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.druid.sql.calcite.rule.logical.LogicalUnnest;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * DruidRelFieldTrimmer is a subclass of {@link RelFieldTrimmer} that provides additional support for Druid specific RelNodes.
 *
 * It is used to trim fields from Druid specific RelNodes like {@link LogicalUnnest}.
 */
public class DruidRelFieldTrimmer extends RelFieldTrimmer
{
  private final RelBuilder relBuilder;

  public DruidRelFieldTrimmer(@Nullable SqlValidator validator, RelBuilder relBuilder)
  {
    super(validator, relBuilder);
    this.relBuilder = relBuilder;
  }

  @Override
  protected TrimResult dummyProject(int fieldCount, RelNode input)
  {
    return makeIdentityMapping(input);
  }

  @Override
  protected TrimResult dummyProject(int fieldCount, RelNode input,
      @Nullable RelNode originalRelNode)
  {
    if (fieldCount != 0) {
      return super.dummyProject(fieldCount, input, originalRelNode);
    }
    // workaround to support fieldCount == 0 projections
    final Mapping mapping = Mappings.create(MappingType.INVERSE_SURJECTION, fieldCount, 0);
    if (input.getRowType().getFieldCount() == 0) {
      // there is no need to do anything
      return result(input, mapping);
    }
    relBuilder.push(input);
    relBuilder.project(Collections.emptyList(), Collections.emptyList());
    RelNode newProject = relBuilder.build();
    if (originalRelNode != null) {
      newProject = RelOptUtil.propagateRelHints(originalRelNode, newProject);
    }
    return result(newProject, mapping);
  }

  private TrimResult makeIdentityMapping(RelNode input)
  {
    Mapping mapping = Mappings.createIdentity(input.getRowType().getFieldCount());
    return result(input, mapping);
  }

  /**
   * Should be unnecesarry in versions having CALCITE-6715
   */
  public TrimResult trimFields(LogicalCorrelate correlate,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields)
  {
    if (!extraFields.isEmpty()) {
      // bail out with generic trim
      return trimFields((RelNode) correlate, fieldsUsed, extraFields);
    }

    fieldsUsed = fieldsUsed.union(correlate.getRequiredColumns());

    List<RelNode> newInputs = new ArrayList<>();
    List<Mapping> inputMappings = new ArrayList<>();
    int changeCount = 0;
    int offset = 0;
    for (RelNode input : correlate.getInputs()) {
      final RelDataType inputRowType = input.getRowType();
      final int inputFieldCount = inputRowType.getFieldCount();

      ImmutableBitSet currentInputFieldsUsed = fieldsUsed
          .intersect(ImmutableBitSet.range(offset, offset + inputFieldCount))
          .shift(-offset);

      TrimResult trimResult;
      try {
        trimResult = dispatchTrimFields(input, currentInputFieldsUsed, extraFields);
      }
      catch (RuntimeException e) {
        throw e;
      }

      newInputs.add(trimResult.left);
      if (trimResult.left != input) {
        changeCount++;
      }

      final Mapping inputMapping = trimResult.right;
      inputMappings.add(inputMapping);

      offset += inputFieldCount;
    }

    if (changeCount == 0) {
      return result(correlate, Mappings.createIdentity(correlate.getRowType().getFieldCount()));
    }

    Mapping mapping = makeMapping(inputMappings);
    RexBuilder rexBuilder = correlate.getCluster().getRexBuilder();

    final LogicalCorrelate newCorrelate = correlate.copy(
        correlate.getTraitSet(),
        newInputs.get(0),
        newInputs.get(1).accept(
            new RexRewritingRelShuttle(
                new RexCorrelVariableMapShuttle(correlate.getCorrelationId(), newInputs.get(0).getRowType(), mapping, rexBuilder)
            )
        ),
        correlate.getCorrelationId(),
        correlate.getRequiredColumns().permute(mapping),
        correlate.getJoinType()
    );

    return result(newCorrelate, mapping);
  }

  public TrimResult trimFields(LogicalUnnest correlate,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields)
  {
    if (!extraFields.isEmpty()) {
      // bail out with generic trim
      return trimFields((RelNode) correlate, fieldsUsed, extraFields);
    }
    RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(extraFields);

    correlate.getUnnestExpr().accept(inputFinder);

    ImmutableBitSet finderFields = inputFinder.build();

    ImmutableBitSet inputFieldsUsed = ImmutableBitSet.builder()
        .addAll(fieldsUsed.clear(correlate.getRowType().getFieldCount() - 1))
        .addAll(finderFields)
        .build();

    RelNode input = correlate.getInput();

    // Create input with trimmed columns.
    TrimResult trimResult = trimChild(correlate, input, inputFieldsUsed, extraFields);

    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    if (newInput == input) {
      return result(correlate, Mappings.createIdentity(correlate.getRowType().getFieldCount()));
    }

    Mapping mapping = makeMapping(ImmutableList.of(inputMapping, Mappings.createIdentity(1)));

    final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle(inputMapping, newInput);

    RexNode newUnnestExpr = correlate.getUnnestExpr().accept(shuttle);
    RexNode newFilterExpr = correlate.getFilter();

    final LogicalUnnest newCorrelate = correlate.copy(
        correlate.getTraitSet(),
        newInput,
        newUnnestExpr,
        newFilterExpr
    );

    return result(newCorrelate, mapping);
  }

  /**
   * Concatenates multiple mapping.
   *
   * <pre>
   * [ 1:0, 2:1] // sourceCount:100
   * [ 1:0, 2:1] // sourceCount:100
   * output:
   * [ 1:0, 2:1, 101:2, 102:3 ] ; sourceCount:200
   * </pre>
   */
  private Mapping makeMapping(List<Mapping> inputMappings)
  {
    int fieldCount = 0;
    int newFieldCount = 0;
    for (Mapping mapping : inputMappings) {
      fieldCount += mapping.getSourceCount();
      newFieldCount += mapping.getTargetCount();
    }

    Mapping mapping = Mappings.create(
        MappingType.INVERSE_SURJECTION,
        fieldCount,
        newFieldCount
    );
    int offset = 0;
    int newOffset = 0;
    for (int i = 0; i < inputMappings.size(); i++) {
      Mapping inputMapping = inputMappings.get(i);
      for (IntPair pair : inputMapping) {
        mapping.set(pair.source + offset, pair.target + newOffset);
      }
      offset += inputMapping.getSourceCount();
      newOffset += inputMapping.getTargetCount();
    }
    return mapping;
  }

  static class RexCorrelVariableMapShuttle extends RexShuttle
  {
    private final CorrelationId correlationId;
    private final Mapping mapping;
    private final RelDataType newCorrelRowType;
    private final RexBuilder rexBuilder;

    public RexCorrelVariableMapShuttle(final CorrelationId correlationId, RelDataType newCorrelRowType, Mapping mapping, RexBuilder rexBuilder)
    {
      this.correlationId = correlationId;
      this.newCorrelRowType = newCorrelRowType;
      this.mapping = mapping;
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitFieldAccess(final RexFieldAccess fieldAccess)
    {
      if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
        RexCorrelVariable referenceExpr = (RexCorrelVariable) fieldAccess.getReferenceExpr();
        final RexCorrelVariable encounteredCorrelationId = referenceExpr;
        if (encounteredCorrelationId.id.equals(correlationId)) {
          int sourceIndex = fieldAccess.getField().getIndex();
          return rexBuilder.makeFieldAccess(map(referenceExpr), mapping.getTarget(sourceIndex));
        }
      }
      return super.visitFieldAccess(fieldAccess);
    }

    private RexNode map(RexCorrelVariable referenceExpr)
    {
      return rexBuilder.makeCorrel(newCorrelRowType, referenceExpr.id);
    }
  }

  static class RexRewritingRelShuttle extends RelHomogeneousShuttle
  {
    private final RexShuttle rexVisitor;

    RexRewritingRelShuttle(RexShuttle rexVisitor)
    {
      this.rexVisitor = rexVisitor;
    }

    @Override
    public RelNode visit(RelNode other)
    {
      RelNode next = super.visit(other);
      return next.accept(rexVisitor);
    }
  }
}
