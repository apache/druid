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

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.expression.SimpleExtraction;
import org.apache.druid.sql.calcite.table.RowSignatures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConvertSelectorsToIns extends BottomUpTransform
{
  private final RowSignature sourceRowSignature;

  private ConvertSelectorsToIns(final RowSignature sourceRowSignature)
  {
    this.sourceRowSignature = sourceRowSignature;
  }

  public static ConvertSelectorsToIns create(final RowSignature sourceRowSignature)
  {
    return new ConvertSelectorsToIns(sourceRowSignature);
  }

  @Override
  public DimFilter process(DimFilter filter)
  {
    if (filter instanceof OrDimFilter) {
      // Copy children list
      final List<DimFilter> children = Lists.newArrayList(((OrDimFilter) filter).getFields());

      // Group filters by dimension and extractionFn.
      final Map<BoundRefKey, List<SelectorDimFilter>> selectors = new HashMap<>();
      final Map<RangeRefKey, List<EqualityFilter>> equality = new HashMap<>();

      for (DimFilter child : children) {
        if (child instanceof SelectorDimFilter) {
          final SelectorDimFilter selector = (SelectorDimFilter) child;
          final BoundRefKey boundRefKey = BoundRefKey.from(
              selector,
              RowSignatures.getNaturalStringComparator(
                  sourceRowSignature,
                  SimpleExtraction.of(selector.getDimension(), selector.getExtractionFn())
              )
          );
          List<SelectorDimFilter> filterList = selectors.computeIfAbsent(boundRefKey, k -> new ArrayList<>());
          filterList.add(selector);
        } else if (child instanceof EqualityFilter) {
          final EqualityFilter equals = (EqualityFilter) child;
          if (!equals.getMatchValueType().is(ValueType.STRING)) {
            // skip non-string equality filters since InDimFilter uses a sorted string set, which is a different sort
            // than numbers or other types might use
            continue;
          }
          final RangeRefKey rangeRefKey = RangeRefKey.from(equals);
          List<EqualityFilter> filterList = equality.computeIfAbsent(rangeRefKey, k -> new ArrayList<>());
          filterList.add(equals);
        }
      }

      // Emit IN filters for each group of size > 1.
      for (Map.Entry<BoundRefKey, List<SelectorDimFilter>> entry : selectors.entrySet()) {
        final List<SelectorDimFilter> filterList = entry.getValue();
        if (filterList.size() > 1) {
          // We found a simplification. Remove the old filters and add new ones.
          final InDimFilter.ValuesSet values = new InDimFilter.ValuesSet();

          for (final SelectorDimFilter selector : filterList) {
            values.add(selector.getValue());
            if (!children.remove(selector)) {
              // Don't expect this to happen, but include it as a sanity check.
              throw new ISE("Tried to remove selector but couldn't");
            }
          }

          children.add(new InDimFilter(entry.getKey().getDimension(), values, entry.getKey().getExtractionFn(), null));
        }
      }

      // Emit IN filters for each group of size > 1
      // right now we only do this for string types, since the value set is sorted in string order
      // someday we might want to either allow numbers after ensuring that all value set indexes can handle value
      // sets which are not in the correct sorted order, or make a cooler in filter that retains the match value type
      // and can sort the values in match value native order
      for (Map.Entry<RangeRefKey, List<EqualityFilter>> entry : equality.entrySet()) {
        final List<EqualityFilter> filterList = entry.getValue();
        if (filterList.size() > 1) {
          // We found a simplification. Remove the old filters and add new ones.
          final InDimFilter.ValuesSet values = new InDimFilter.ValuesSet();

          for (final EqualityFilter equals : filterList) {
            values.add(
                ExprEval.ofType(ExpressionType.fromColumnType(equals.getMatchValueType()), equals.getMatchValue())
                        .castTo(ExpressionType.STRING)
                        .asString()
            );
            if (!children.remove(equals)) {
              // Don't expect this to happen, but include it as a sanity check.
              throw new ISE("Tried to remove equals but couldn't");
            }
          }

          children.add(new InDimFilter(entry.getKey().getColumn(), values, null, null));
        }
      }

      if (!children.equals(((OrDimFilter) filter).getFields())) {
        return children.size() == 1 ? children.get(0) : new OrDimFilter(children);
      } else {
        return filter;
      }
    } else {
      return filter;
    }
  }
}
