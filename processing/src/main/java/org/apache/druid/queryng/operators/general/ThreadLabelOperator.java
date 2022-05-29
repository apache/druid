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

package org.apache.druid.queryng.operators.general;

import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.WrappingOperator;

/**
 * Operator which relabels its thread during its execution.
 *
 * @see {@link org.apache.druid.query.spec.SpecificSegmentQueryRunner}
 */
public class ThreadLabelOperator<T> extends WrappingOperator<T>
{
  private final String label;
  private String originalLabel;

  public ThreadLabelOperator(
      final FragmentContext context,
      final String label,
      final Operator<T> child)
  {
    super(context, child);
    this.label = label;
  }

  @Override
  public void onOpen()
  {
    final Thread currThread = Thread.currentThread();
    originalLabel = currThread.getName();
    currThread.setName(label);
  }

  @Override
  public void onClose()
  {
    if (originalLabel != null) {
      Thread.currentThread().setName(originalLabel);
      originalLabel = null;
    }
  }
}
