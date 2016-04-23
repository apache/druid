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

package io.druid.hive;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 */
public class Ranges
{
  public static final Predicate<Range> VALID = new Predicate<Range>()
  {
    @Override
    public boolean apply(Range input)
    {
      return !input.isEmpty();
    }
  };

  public static final Function<List<Range>, List<Range>> COMPACT = new Function<List<Range>, List<Range>>()
  {
    @Override
    public List<Range> apply(List<Range> input)
    {
      return Ranges.condenseRanges(input);
    }
  };

  public static List<Range> condenseRanges(List<Range> ranges)
  {
    if (ranges.size() <= 1) {
      return ranges;
    }

    Comparator<Range> startThenEnd = new Comparator<Range>()
    {
      @Override
      public int compare(Range lhs, Range rhs)
      {
        int compare = 0;
        if (lhs.hasLowerBound() && rhs.hasLowerBound()) {
          compare = lhs.lowerEndpoint().compareTo(rhs.lowerEndpoint());
        } else if (!lhs.hasLowerBound() && rhs.hasLowerBound()) {
          compare = -1;
        } else if (lhs.hasLowerBound() && !rhs.hasLowerBound()) {
          compare = 1;
        }
        if (compare != 0) {
          return compare;
        }
        if (lhs.hasUpperBound() && rhs.hasUpperBound()) {
          compare = lhs.upperEndpoint().compareTo(rhs.upperEndpoint());
        } else if (!lhs.hasUpperBound() && rhs.hasUpperBound()) {
          compare = -1;
        } else if (lhs.hasUpperBound() && !rhs.hasUpperBound()) {
          compare = 1;
        }
        return compare;
      }
    };

    TreeSet<Range> sortedIntervals = Sets.newTreeSet(startThenEnd);
    sortedIntervals.addAll(ranges);

    List<Range> retVal = Lists.newArrayList();

    Iterator<Range> intervalsIter = sortedIntervals.iterator();
    Range currInterval = intervalsIter.next();
    while (intervalsIter.hasNext()) {
      Range next = intervalsIter.next();
      if (currInterval.encloses(next)) {
        continue;
      }
      if (mergeable(currInterval, next)) {
        currInterval = currInterval.span(next);
      } else {
        retVal.add(currInterval);
        currInterval = next;
      }
    }
    retVal.add(currInterval);

    return retVal;
  }

  public static boolean mergeable(Range range1, Range range2)
  {
    Comparable x1 = range1.upperEndpoint();
    Comparable x2 = range2.lowerEndpoint();
    int compare = x1.compareTo(x2);
    return compare > 0 || (compare == 0
                           && range1.upperBoundType() == BoundType.CLOSED
                           && range2.lowerBoundType() == BoundType.CLOSED);
  }
}
