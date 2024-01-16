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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.expression.SimpleExtraction;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

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
      List<DimFilter> children = Lists.newArrayList(((OrDimFilter) filter).getFields());

      // Process "selector" filters, which are used when "sqlUseBoundAndSelectors" is true.
      children = new CollectSelectors(children, sourceRowSignature).collect();

      // Process "equality" filters, which are used when "sqlUseBoundAndSelectors" is false.
      children = new CollectEqualities(children).collect();

      if (!children.equals(((OrDimFilter) filter).getFields())) {
        return children.size() == 1 ? children.get(0) : new OrDimFilter(children);
      } else {
        return filter;
      }
    } else {
      return filter;
    }
  }

  /**
   * Split a filter into a filter of type T, and other filters that should be ANDed with it. Returns null if the
   * provided filter is neither a T itself, nor an AND that contains T.
   *
   * @param filter      filter to split
   * @param filterClass filter class to return as left-hand side
   * @param preference  given two filters of filterClass in and AND, which one should be preferred to be returned
   *                    as the left-hand side of the Pair? "Greater than" means more preferred.
   */
  @Nullable
  private static <T extends DimFilter> Pair<T, List<DimFilter>> splitAnd(
      final DimFilter filter,
      final Class<T> filterClass,
      final Comparator<T> preference
  )
  {
    if (filter instanceof AndDimFilter) {
      final List<DimFilter> children = ((AndDimFilter) filter).getFields();
      T found = null;

      for (final DimFilter child : children) {
        if (filterClass.isAssignableFrom(child.getClass())) {
          final T childFilter = filterClass.cast(child);
          if (found == null || preference.compare(childFilter, found) > 0) {
            found = childFilter;
          }
        }
      }

      if (found == null) {
        return null;
      }

      final List<DimFilter> others = new ArrayList<>(children.size() - 1);
      for (final DimFilter child : children) {
        //noinspection ObjectEquality
        if (child != found) {
          others.add(child);
        }
      }

      return Pair.of(found, others);
    } else if (filterClass.isAssignableFrom(filter.getClass())) {
      return Pair.of(filterClass.cast(filter), Collections.emptyList());
    } else {
      return null;
    }
  }

  /**
   * Helper for collecting {@link SelectorDimFilter} into {@link InDimFilter}.
   */
  private static class CollectSelectors
      extends CollectComparisons<DimFilter, SelectorDimFilter, InDimFilter, BoundRefKey>
  {
    private final RowSignature sourceRowSignature;

    public CollectSelectors(final List<DimFilter> orExprs, final RowSignature sourceRowSignature)
    {
      super(orExprs);
      this.sourceRowSignature = sourceRowSignature;
    }

    @Nullable
    @Override
    protected Pair<SelectorDimFilter, List<DimFilter>> getCollectibleComparison(DimFilter filter)
    {
      return ConvertSelectorsToIns.splitAnd(
          filter,
          SelectorDimFilter.class,

          // Prefer extracting nonnull vs null comparisons when ANDed, as nonnull comparisons are more likely to
          // find companions in other ORs.
          Comparator.comparing(selector -> selector.getValue() == null ? 0 : 1)
      );
    }

    @Nullable
    @Override
    protected BoundRefKey getCollectionKey(SelectorDimFilter selector)
    {
      return BoundRefKey.from(
          selector,
          RowSignatures.getNaturalStringComparator(
              sourceRowSignature,
              SimpleExtraction.of(selector.getDimension(), selector.getExtractionFn())
          )
      );
    }

    @Override
    protected Set<String> getMatchValues(SelectorDimFilter selector)
    {
      return Collections.singleton(selector.getValue());
    }

    @Nullable
    @Override
    protected InDimFilter makeCollectedComparison(BoundRefKey boundRefKey, InDimFilter.ValuesSet values)
    {
      if (values.size() > 1) {
        return new InDimFilter(boundRefKey.getDimension(), values, boundRefKey.getExtractionFn(), null);
      } else {
        return null;
      }
    }

    @Override
    protected DimFilter makeAnd(List<DimFilter> exprs)
    {
      return new AndDimFilter(exprs);
    }
  }

  /**
   * Helper for collecting {@link EqualityFilter} into {@link InDimFilter}.
   */
  private static class CollectEqualities extends CollectComparisons<DimFilter, EqualityFilter, InDimFilter, RangeRefKey>
  {
    public CollectEqualities(final List<DimFilter> orExprs)
    {
      super(orExprs);
    }

    @Nullable
    @Override
    protected Pair<EqualityFilter, List<DimFilter>> getCollectibleComparison(DimFilter filter)
    {
      return ConvertSelectorsToIns.splitAnd(
          filter,
          EqualityFilter.class,

          // Prefer extracting nonnull vs null comparisons when ANDed, as nonnull comparisons are more likely to
          // find companions in other ORs.
          Comparator.comparing(equality -> equality.getMatchValue() == null ? 0 : 1)
      );
    }

    @Nullable
    @Override
    protected RangeRefKey getCollectionKey(EqualityFilter selector)
    {
      if (!selector.getMatchValueType().is(ValueType.STRING)) {
        // skip non-string equality filters since InDimFilter uses a sorted string set, which is a different sort
        // than numbers or other types might use
        return null;
      }

      return RangeRefKey.from(selector);
    }

    @Override
    protected Set<String> getMatchValues(EqualityFilter selector)
    {
      return Collections.singleton(
          ExprEval.ofType(ExpressionType.fromColumnType(selector.getMatchValueType()), selector.getMatchValue())
                  .castTo(ExpressionType.STRING)
                  .asString()
      );
    }

    @Nullable
    @Override
    protected InDimFilter makeCollectedComparison(RangeRefKey rangeRefKey, InDimFilter.ValuesSet values)
    {
      if (values.size() > 1) {
        return new InDimFilter(rangeRefKey.getColumn(), values, null, null);
      } else {
        return null;
      }
    }

    @Override
    protected DimFilter makeAnd(List<DimFilter> exprs)
    {
      return new AndDimFilter(exprs);
    }
  }
}
