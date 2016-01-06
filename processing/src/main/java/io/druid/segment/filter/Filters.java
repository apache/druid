/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.guava.FunctionalIterable;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.Filter;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.data.Indexed;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 */
public class Filters
{
  /**
   * Convert a list of DimFilters to a list of Filters.
   *
   * @param dimFilters list of DimFilters, should all be non-null
   *
   * @return list of Filters
   */
  public static List<Filter> toFilters(List<DimFilter> dimFilters)
  {
    return ImmutableList.copyOf(
        FunctionalIterable
            .create(dimFilters)
            .transform(
                new Function<DimFilter, Filter>()
                {
                  @Override
                  public Filter apply(DimFilter input)
                  {
                    return input.toFilter();
                  }
                }
            )
    );
  }

  /**
   * Convert a DimFilter to a Filter.
   *
   * @param dimFilter dimFilter
   *
   * @return converted filter, or null if input was null
   */
  public static Filter toFilter(DimFilter dimFilter)
  {
    return dimFilter == null ? null : dimFilter.toFilter();
  }

  /**
   * Return the union of bitmaps for all values matching a particular predicate.
   *
   * @param dimension dimension to look at
   * @param selector  bitmap selector
   * @param predicate predicate to use
   *
   * @return bitmap of matching rows
   */
  public static ImmutableBitmap matchPredicate(
      final String dimension,
      final BitmapIndexSelector selector,
      final Predicate<String> predicate
  )
  {
    Preconditions.checkNotNull(dimension, "dimension");
    Preconditions.checkNotNull(selector, "selector");
    Preconditions.checkNotNull(predicate, "predicate");

    // Missing dimension -> match all rows if the predicate matches null; match no rows otherwise
    final Indexed<String> dimValues = selector.getDimensionValues(dimension);
    if (dimValues == null || dimValues.size() == 0) {
      if (predicate.apply(null)) {
        return selector.getBitmapFactory().complement(
            selector.getBitmapFactory().makeEmptyImmutableBitmap(),
            selector.getNumRows()
        );
      } else {
        return selector.getBitmapFactory().makeEmptyImmutableBitmap();
      }
    }

    // Apply predicate to all dimension values and union the matching bitmaps
    final BitmapIndex bitmapIndex = selector.getBitmapIndex(dimension);
    return selector.getBitmapFactory().union(
        new Iterable<ImmutableBitmap>()
        {
          @Override
          public Iterator<ImmutableBitmap> iterator()
          {
            return new Iterator<ImmutableBitmap>()
            {
              int currIndex = 0;

              @Override
              public boolean hasNext()
              {
                return currIndex < bitmapIndex.getCardinality();
              }

              @Override
              public ImmutableBitmap next()
              {
                while (currIndex < bitmapIndex.getCardinality() && !predicate.apply(dimValues.get(currIndex))) {
                  currIndex++;
                }

                if (currIndex == bitmapIndex.getCardinality()) {
                  return bitmapIndex.getBitmapFactory().makeEmptyImmutableBitmap();
                }

                return bitmapIndex.getBitmap(currIndex++);
              }

              @Override
              public void remove()
              {
                throw new UnsupportedOperationException();
              }
            };
          }
        }
    );
  }

  public static Filter[] partitionWithBitmapSupport(Filter current)
  {
    return partitionFilterWith(
        current, new Predicate<Filter>()
        {
          @Override
          public boolean apply(Filter input)
          {
            return input.supportsBitmap();
          }
        }
    );
  }

  private static Filter[] partitionFilterWith(Filter current, Predicate<Filter> predicate)
  {
    if (current == null) {
      return null;
    }
    current = pushDownNot(current);
    current = flatten(current);
    current = convertToCNF(current);
    current = flatten(current);

    List<Filter> bitmapIndexSupported = Lists.newArrayList();
    List<Filter> bitmapIndexNotSupported = Lists.newArrayList();

    traverse(current, predicate, bitmapIndexSupported, bitmapIndexNotSupported);

    return new Filter[]{andFilter(bitmapIndexSupported), andFilter(bitmapIndexNotSupported)};
  }

  private static void traverse(
      Filter current,
      Predicate<Filter> predicate,
      List<Filter> support,
      List<Filter> notSupport
  )
  {
    if (current instanceof AndFilter) {
      for (Filter child : ((AndFilter) current).getChildren()) {
        traverse(child, predicate, support, notSupport);
      }
    } else {
      if (predicate.apply(current)) {
        support.add(current);
      } else {
        notSupport.add(current);
      }
    }
  }

  private static Filter andFilter(List<Filter> filters)
  {
    return filters.isEmpty() ? null : filters.size() == 1 ? filters.get(0) : new AndFilter(filters);
  }

  // copied from apache hive
  private static Filter pushDownNot(Filter current)
  {
    if (current instanceof NotFilter) {
      Filter child = ((NotFilter) current).getBaseFilter();
      if (child instanceof NotFilter) {
        return pushDownNot(((NotFilter) child).getBaseFilter());
      }
      if (child instanceof AndFilter) {
        List<Filter> children = Lists.newArrayList();
        for (Filter grandChild : ((AndFilter) child).getChildren()) {
          children.add(pushDownNot(new NotFilter(grandChild)));
        }
        return new OrFilter(children);
      }
      if (child instanceof OrFilter) {
        List<Filter> children = Lists.newArrayList();
        for (Filter grandChild : ((OrFilter) child).getChildren()) {
          children.add(pushDownNot(new NotFilter(grandChild)));
        }
        return new AndFilter(children);
      }
    }
    if (current instanceof AndFilter) {
      List<Filter> children = Lists.newArrayList();
      for (Filter child : ((AndFilter) current).getChildren()) {
        children.add(pushDownNot(child));
      }
      return new AndFilter(children);
    }
    if (current instanceof OrFilter) {
      List<Filter> children = Lists.newArrayList();
      for (Filter child : ((OrFilter) current).getChildren()) {
        children.add(pushDownNot(child));
      }
      return new OrFilter(children);
    }
    return current;
  }

  // copied from apache hive
  private static Filter convertToCNF(Filter current)
  {
    if (current instanceof NotFilter) {
      return new NotFilter(convertToCNF(((NotFilter) current).getBaseFilter()));
    }
    if (current instanceof AndFilter) {
      List<Filter> children = Lists.newArrayList();
      for (Filter child : ((AndFilter) current).getChildren()) {
        children.add(convertToCNF(child));
      }
      return new AndFilter(children);
    }
    if (current instanceof OrFilter) {
      // a list of leaves that weren't under AND expressions
      List<Filter> nonAndList = new ArrayList<Filter>();
      // a list of AND expressions that we need to distribute
      List<Filter> andList = new ArrayList<Filter>();
      for (Filter child : ((OrFilter) current).getChildren()) {
        if (child instanceof AndFilter) {
          andList.add(child);
        } else if (child instanceof OrFilter) {
          // pull apart the kids of the OR expression
          for (Filter grandChild : ((OrFilter) child).getChildren()) {
            nonAndList.add(grandChild);
          }
        } else {
          nonAndList.add(child);
        }
      }
      if (!andList.isEmpty()) {
        List<Filter> result = Lists.newArrayList();
        generateAllCombinations(result, andList, nonAndList);
        return new AndFilter(result);
      }
    }
    return current;
  }

  private static Filter flatten(Filter root)
  {
    if (root instanceof Filter.Relational) {
      List<Filter> children = ((Filter.Relational) root).getChildren();
      // iterate through the index, so that if we add more children,
      // they don't get re-visited
      for (int i = 0; i < children.size(); ++i) {
        Filter child = flatten(children.get(i));
        // do we need to flatten?
        if (child.getClass() == root.getClass() && !(child instanceof NotFilter)) {
          boolean first = true;
          List<Filter> grandKids = ((Filter.Relational) child).getChildren();
          for (Filter grandkid : grandKids) {
            // for the first grandkid replace the original parent
            if (first) {
              first = false;
              children.set(i, grandkid);
            } else {
              children.add(++i, grandkid);
            }
          }
        } else {
          children.set(i, child);
        }
      }
      // if we have a singleton AND or OR, just return the child
      if (children.size() == 1 && (root instanceof AndFilter || root instanceof OrFilter)) {
        return children.get(0);
      }
    }
    return root;
  }

  // copied from apache hive
  private static void generateAllCombinations(
      List<Filter> result,
      List<Filter> andList,
      List<Filter> nonAndList
  )
  {
    List<Filter> children = ((AndFilter) andList.get(0)).getChildren();
    if (result.isEmpty()) {
      for (Filter child : children) {
        List<Filter> a = Lists.newArrayList(nonAndList);
        a.add(child);
        result.add(new OrFilter(a));
      }
    } else {
      List<Filter> work = new ArrayList<Filter>(result);
      result.clear();
      for (Filter child : children) {
        for (Filter or : work) {
          List<Filter> a = Lists.newArrayList((((OrFilter) or).getChildren()));
          a.add(child);
          result.add(new OrFilter(a));
        }
      }
    }
    if (andList.size() > 1) {
      generateAllCombinations(
          result, andList.subList(1, andList.size()),
          nonAndList
      );
    }
  }
}
