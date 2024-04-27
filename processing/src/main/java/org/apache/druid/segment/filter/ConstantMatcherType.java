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

package org.apache.druid.segment.filter;

import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.BooleanVectorValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.vector.VectorSizeInspector;

public enum ConstantMatcherType
{
  /**
   * Constant matcher that is always true
   */
  ALL_TRUE {
    @Override
    public ValueMatcher asValueMatcher()
    {
      return ValueMatchers.allTrue();
    }

    @Override
    public VectorValueMatcher asVectorMatcher(VectorSizeInspector inspector)
    {
      return BooleanVectorValueMatcher.of(inspector, this);
    }
  },
  /**
   * Constant matcher that is always false
   */
  ALL_FALSE {
    @Override
    public ValueMatcher asValueMatcher()
    {
      return ValueMatchers.allFalse();
    }

    @Override
    public VectorValueMatcher asVectorMatcher(VectorSizeInspector inspector)
    {
      return BooleanVectorValueMatcher.of(inspector, this);
    }
  },
  /**
   * Constant matcher that is always null
   */
  ALL_UNKNOWN {
    @Override
    public ValueMatcher asValueMatcher()
    {
      return ValueMatchers.allUnknown();
    }

    @Override
    public VectorValueMatcher asVectorMatcher(VectorSizeInspector inspector)
    {
      return BooleanVectorValueMatcher.of(inspector, this);
    }
  };

  public abstract ValueMatcher asValueMatcher();

  public abstract VectorValueMatcher asVectorMatcher(VectorSizeInspector inspector);
}
