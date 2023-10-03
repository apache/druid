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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.Expressions;

import javax.annotation.Nullable;

/**
 * Enables simpler access to input expressions.
 *
 * In case of aggregates it provides the constants transparently for aggregates.
 */
public class InputAccessor
{
  private final Project project;
  private final ImmutableList<RexLiteral> constants;
  private final RexBuilder rexBuilder;
  private final RowSignature sourceRowSignature;

  public static InputAccessor buildFor(
      RexBuilder rexBuilder,
      RowSignature sourceRowSignature,
      @Nullable Project project,
      @Nullable ImmutableList<RexLiteral> constants)
  {
    return new InputAccessor(rexBuilder, sourceRowSignature, project, constants);
  }

  private InputAccessor(
      RexBuilder rexBuilder,
      RowSignature sourceRowSignature,
      Project project,
      ImmutableList<RexLiteral> constants)
  {
    this.rexBuilder = rexBuilder;
    this.sourceRowSignature = sourceRowSignature;
    this.project = project;
    this.constants = constants;

  }

  public RexNode getField(int argIndex)
  {
    if (argIndex < sourceRowSignature.size()) {
      return Expressions.fromFieldAccess(
          rexBuilder.getTypeFactory(),
          sourceRowSignature,
          project,
          argIndex);
    } else {
      return constants.get(argIndex - sourceRowSignature.size());
    }

  }

  @Deprecated
  public Project getProject()
  {
    return project;
  }

}
