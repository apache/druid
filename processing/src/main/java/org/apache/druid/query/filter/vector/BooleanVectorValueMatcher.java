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

public class BooleanVectorValueMatcher extends BaseVectorValueMatcher
{
  private final VectorSizeInspector selector;
  private final boolean matches;

  private BooleanVectorValueMatcher(final VectorSizeInspector selector, final boolean matches)
  {
    super(selector);
    this.selector = selector;
    this.matches = matches;
  }

  public static BooleanVectorValueMatcher of(final VectorSizeInspector selector, final boolean matches)
  {
    return new BooleanVectorValueMatcher(selector, matches);
  }

  @Override
  public int getCurrentVectorSize()
  {
    return selector.getCurrentVectorSize();
  }

  @Override
  public int getMaxVectorSize()
  {
    return selector.getCurrentVectorSize();
  }

  @Override
  public ReadableVectorMatch match(final ReadableVectorMatch mask)
  {
    if (matches) {
      assert mask.isValid(mask);
      return mask;
    } else {
      return VectorMatch.allFalse();
    }
  }
}
