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

package org.apache.druid.msq.logical.stages;

import org.apache.druid.msq.logical.LogicalInputSpec;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Represents an unbuilt physical stage - which can be built.
 *
 * Implementations of this interface will keep track of the inputs needed to
 * build the physical stage.
 */
public interface LogicalStage
{
  /**
   * Attempts to extend the current stage with an additional node.
   *
   * @return null if the current stage cannot be extended
   */
  @Nullable
  LogicalStage extendWith(DruidNodeStack stack);

  /**
   * Real row signature this stage will return.
   *
   * This might have been reordered and may have additional technical columns.
   */
  RowSignature getRowSignature();

  /**
   * Logical row signature this node supposed to be producing.
   */
  RowSignature getLogicalRowSignature();


  /**
   * Returns the inputs of this stage.
   */
  List<LogicalInputSpec> getInputSpecs();
}
