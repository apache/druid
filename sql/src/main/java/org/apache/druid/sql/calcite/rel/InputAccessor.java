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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Enables simpler access to input expressions.
 *
 * In case of aggregates it provides the constants transparently for aggregates.
 */
public class InputAccessor
{
  private final RelNode relNode;
  @Nullable
  private final Project flattenedProject;
  private final RowSignature inputRowSignature;
  @Nullable
  private final ImmutableList<RexLiteral> constants;
  private final RelNode inputRelNode;
  private final RelDataType inputRelRowType;
  private final RelOptPredicateList predicates;
  private final int inputFieldCount;
  private final RelDataType inputDruidRowType;

  public static InputAccessor buildFor(
      RelNode relNode,
      @Nullable Project flattenedProject,
      RowSignature rowSignature)
  {
    return new InputAccessor(
        relNode,
        flattenedProject,
        rowSignature
    );
  }

  private InputAccessor(
      RelNode relNode,
      Project flattenedProject,
      RowSignature rowSignature)
  {
    this.relNode = relNode;
    this.constants = getConstants(relNode);
    this.inputRelNode = relNode.getInput(0).stripped();
    this.flattenedProject = flattenedProject;
    this.inputRowSignature = rowSignature;
    this.inputRelRowType = inputRelNode.getRowType();
    this.predicates = relNode.getCluster().getMetadataQuery().getPulledUpPredicates(inputRelNode);
    this.inputFieldCount = inputRelRowType.getFieldCount();
    this.inputDruidRowType = RowSignatures.toRelDataType(inputRowSignature, getRexBuilder().getTypeFactory());
  }

  private ImmutableList<RexLiteral> getConstants(RelNode relNode)
  {
    if (relNode instanceof Window) {
      return ((Window) relNode).constants;
    }
    return null;
  }

  public RexNode getField(int argIndex)
  {
    if (argIndex < inputFieldCount) {
      RexInputRef inputRef = RexInputRef.of(argIndex, inputRelRowType);
      RexNode constant = predicates.constantMap.get(inputRef);
      if (constant != null) {
        return constant;
      }
      if (flattenedProject != null) {
        return flattenedProject.getProjects().get(argIndex);
      } else {
        return RexInputRef.of(argIndex, inputDruidRowType);
      }
    } else {
      return constants.get(argIndex - inputFieldCount);
    }
  }

  public List<RexNode> getFields(List<Integer> argList)
  {
    return argList
        .stream()
        .map(i -> getField(i))
        .collect(Collectors.toList());
  }

  public @Nullable Project getProject()
  {
    return flattenedProject;
  }

  public RexBuilder getRexBuilder()
  {
    return relNode.getCluster().getRexBuilder();
  }

  public RowSignature getInputRowSignature()
  {
    return inputRowSignature;
  }

}
