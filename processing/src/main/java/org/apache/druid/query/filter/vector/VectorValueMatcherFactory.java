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

import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;

public interface VectorValueMatcherFactory
{
  VectorValueMatcher makeMatcher(@Nullable String value);

  VectorValueMatcher makeMatcher(DruidPredicateFactory predicateFactory);

  default VectorValueMatcher makeNullValueMatcher(VectorValueSelector selector)
  {
    return new BaseVectorValueMatcher(selector)
    {
      final VectorMatch match = VectorMatch.wrap(new int[selector.getMaxVectorSize()]);

      @Override
      public ReadableVectorMatch match(final ReadableVectorMatch mask)
      {
        final boolean[] nullVector = selector.getNullVector();

        if (nullVector == null) {
          return VectorMatch.allFalse();
        }

        final int[] selection = match.getSelection();

        int numRows = 0;

        for (int i = 0; i < mask.getSelectionSize(); i++) {
          final int rowNum = mask.getSelection()[i];
          if (nullVector[rowNum]) {
            selection[numRows++] = rowNum;
          }
        }

        match.setSelectionSize(numRows);
        assert match.isValid(mask);
        return match;
      }
    };
  }
}
