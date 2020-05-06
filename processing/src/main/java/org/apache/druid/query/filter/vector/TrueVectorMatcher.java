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

package org.apache.druid.query.filter.vector;

import org.apache.druid.segment.vector.VectorSizeInspector;

public class TrueVectorMatcher implements VectorValueMatcher
{
  private final VectorSizeInspector vectorSizeInspector;

  public TrueVectorMatcher(VectorSizeInspector vectorSizeInspector)
  {
    this.vectorSizeInspector = vectorSizeInspector;
  }

  @Override
  public ReadableVectorMatch match(ReadableVectorMatch mask)
  {
    // The given mask is all true for its valid selections.
    return mask;
  }

  @Override
  public int getMaxVectorSize()
  {
    return vectorSizeInspector.getMaxVectorSize();
  }

  @Override
  public int getCurrentVectorSize()
  {
    return vectorSizeInspector.getCurrentVectorSize();
  }
}
