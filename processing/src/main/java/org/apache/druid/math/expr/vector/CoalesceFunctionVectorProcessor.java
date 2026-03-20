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

package org.apache.druid.math.expr.vector;

import org.apache.druid.math.expr.ExpressionType;

public abstract class CoalesceFunctionVectorProcessor<T> implements ExprVectorProcessor<T>
{
  final ExpressionType outputType;
  final ExprVectorProcessor<T>[] processors;
  final FilteredVectorInputBinding inputBindingFilterer;

  public CoalesceFunctionVectorProcessor(
      ExpressionType outputType,
      ExprVectorProcessor<T>[] processors
  )
  {
    this.outputType = outputType;
    this.processors = processors;
    this.inputBindingFilterer = new FilteredVectorInputBinding(processors[0].maxVectorSize());
  }

  @Override
  public ExpressionType getOutputType()
  {
    return outputType;
  }

  @Override
  public int maxVectorSize()
  {
    return processors[0].maxVectorSize();
  }
}
