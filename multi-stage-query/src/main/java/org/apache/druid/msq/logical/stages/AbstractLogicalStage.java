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

import com.google.common.collect.ImmutableList;
import org.apache.druid.msq.logical.LogicalInputSpec;
import org.apache.druid.segment.column.RowSignature;
import java.util.Collections;
import java.util.List;

/**
 * Common stage implementation.
 */
public abstract class AbstractLogicalStage implements LogicalStage
{
  protected final List<LogicalInputSpec> inputSpecs;
  protected final RowSignature signature;

  public AbstractLogicalStage(RowSignature signature, LogicalInputSpec input)
  {
    this(signature, Collections.singletonList(input));
  }

  public AbstractLogicalStage(RowSignature signature, List<LogicalInputSpec> inputs)
  {
    this.inputSpecs = ImmutableList.copyOf(inputs);
    this.signature = signature;
  }

  @Override
  public RowSignature getLogicalRowSignature()
  {
    return signature;
  }

  @Override
  public final RowSignature getRowSignature()
  {
    return signature;
  }

  @Override
  public final List<LogicalInputSpec> getInputSpecs()
  {
    return inputSpecs;
  }
}
