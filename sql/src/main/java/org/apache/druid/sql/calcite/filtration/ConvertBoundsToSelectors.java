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

package org.apache.druid.sql.calcite.filtration;

import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.sql.calcite.expression.SimpleExtraction;
import org.apache.druid.sql.calcite.table.RowSignature;

public class ConvertBoundsToSelectors extends BottomUpTransform
{
  private final RowSignature rowSignature;

  private ConvertBoundsToSelectors(final RowSignature rowSignature)
  {
    this.rowSignature = rowSignature;
  }

  public static ConvertBoundsToSelectors create(final RowSignature rowSignature)
  {
    return new ConvertBoundsToSelectors(rowSignature);
  }

  @Override
  public DimFilter process(DimFilter filter)
  {
    if (filter instanceof BoundDimFilter) {
      final BoundDimFilter bound = (BoundDimFilter) filter;
      final StringComparator comparator = rowSignature.naturalStringComparator(
          SimpleExtraction.of(bound.getDimension(), bound.getExtractionFn())
      );

      if (bound.hasUpperBound()
          && bound.hasLowerBound()
          && bound.getUpper().equals(bound.getLower())
          && !bound.isUpperStrict()
          && !bound.isLowerStrict()
          && bound.getOrdering().equals(comparator)) {
        return new SelectorDimFilter(
            bound.getDimension(),
            bound.getUpper(),
            bound.getExtractionFn()
        );
      } else {
        return filter;
      }
    } else {
      return filter;
    }
  }
}
