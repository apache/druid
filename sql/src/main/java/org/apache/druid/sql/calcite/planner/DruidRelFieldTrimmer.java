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
import org.apache.calcite.rel.core.TableScan;
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
import java.util.List;
import java.util.Set;

public class DruidRelFieldTrimmer extends RelFieldTrimmer
{
  private RelBuilder relBuilder;
  private boolean trimTableScan;

  public DruidRelFieldTrimmer(@Nullable SqlValidator validator, RelBuilder relBuilder, boolean trimTableScan)
  {
    super(validator, relBuilder);
    this.relBuilder = relBuilder;
    this.trimTableScan = trimTableScan;
  }

  public TrimResult trimFields(
      final TableScan tableAccessRel,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields)
  {
    if (trimTableScan) {
      return super.trimFields(tableAccessRel, fieldsUsed, extraFields);
    } else {
      Mapping mapping = Mappings.createIdentity(tableAccessRel.getRowType().getFieldCount());
      return result(tableAccessRel, mapping, tableAccessRel);
    }
  }

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
    @Deprecated
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
    final LogicalCorrelate newCorrelate = correlate.copy(
        correlate.getTraitSet(),
        newInputs.get(0),
        newInputs.get(1).accept(
            new RelVistatorRelShuttle(
                new CorrelateExprMapping(correlate.getCorrelationId(), newInputs.get(0).getRowType(), mapping)
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
    if (correlate.getFilter() != null) {
      correlate.getFilter().accept(inputFinder);
    }

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
    if (correlate.getFilter() != null) {
      newFilterExpr = correlate.getFilter().accept(shuttle);
    }

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
   * [ 1:0, 2:1] // sourceCount:100 [ 1:0, 2:1] // sourceCount:100 output: [
   * 1:0, 2:1, 101:2, 102:3 ] ; sourceCount:200
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

  class CorrelateExprMapping extends RexShuttle
  {
    private final CorrelationId correlationId;
    private Mapping mapping;
    private RelDataType newCorrelRowType;

    public CorrelateExprMapping(final CorrelationId correlationId, RelDataType newCorrelRowType, Mapping mapping)
    {
      this.correlationId = correlationId;
      this.newCorrelRowType = newCorrelRowType;
      this.mapping = mapping;
    }

    @Override
    public RexNode visitFieldAccess(final RexFieldAccess fieldAccess)
    {
      RexNode referenceExpr1 = fieldAccess.getReferenceExpr();
      if (referenceExpr1 instanceof RexCorrelVariable) {
        RexCorrelVariable referenceExpr = (RexCorrelVariable) referenceExpr1;
        final RexCorrelVariable encounteredCorrelationId = referenceExpr;
        if (encounteredCorrelationId.id.equals(correlationId)) {

          RexBuilder rb = relBuilder.getRexBuilder();
          int sourceIndex = fieldAccess.getField().getIndex();
          ;
          RexFieldAccess a = (RexFieldAccess) rb.makeFieldAccess(map(referenceExpr), mapping.getTarget(sourceIndex));

          return a;
        }
      }
      return super.visitFieldAccess(fieldAccess);
    }

    private RexNode map(RexCorrelVariable referenceExpr)
    {
      RexBuilder rb = relBuilder.getRexBuilder();
      return rb.makeCorrel(newCorrelRowType, referenceExpr.id);

    }
  }

  public class RelVistatorRelShuttle extends RelHomogeneousShuttle
  {
    private final RexShuttle rexVisitor;

    RelVistatorRelShuttle(RexShuttle rexVisitor)
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
