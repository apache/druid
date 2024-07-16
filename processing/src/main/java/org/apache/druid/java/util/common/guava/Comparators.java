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

package org.apache.druid.java.util.common.guava;

import com.google.common.collect.Ordering;
import org.joda.time.DateTimeComparator;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Comparator;

/**
 */
public class Comparators
{
  private static final Ordering<Object> ALWAYS_EQUAL = new Ordering<Object>()
  {
    @SuppressWarnings("ComparatorMethodParameterNotUsed")
    @Override
    public int compare(@Nullable Object left, @Nullable Object right)
    {
      return 0;
    }
  };

  //CHECKSTYLE.OFF: Regexp
  // Ordering.natural().nullsFirst() is generally prohibited, but we need a single exception.
  private static final Ordering NATURAL_NULLS_FIRST = Ordering.natural().nullsFirst();
  //CHECKSTYLE.ON: Regexp

  @SuppressWarnings("unchecked")
  public static <T> Ordering<T> alwaysEqual()
  {
    return (Ordering<T>) ALWAYS_EQUAL;
  }

  @SuppressWarnings("unchecked")
  public static <T extends Comparable<? super T>> Ordering<T> naturalNullsFirst()
  {
    return NATURAL_NULLS_FIRST;
  }

  private static final Comparator<Interval> INTERVAL_BY_START_THEN_END = new Comparator<Interval>()
  {
    private final DateTimeComparator dateTimeComp = DateTimeComparator.getInstance();

    @Override
    public int compare(Interval lhs, Interval rhs)
    {
      if (lhs.getChronology().equals(rhs.getChronology())) {
        int compare = Long.compare(lhs.getStartMillis(), rhs.getStartMillis());
        if (compare == 0) {
          return Long.compare(lhs.getEndMillis(), rhs.getEndMillis());
        }
        return compare;
      }
      int retVal = dateTimeComp.compare(lhs.getStart(), rhs.getStart());
      if (retVal == 0) {
        retVal = dateTimeComp.compare(lhs.getEnd(), rhs.getEnd());
      }
      return retVal;
    }
  };

  private static final Comparator<Interval> INTERVAL_BY_END_THEN_START = new Comparator<Interval>()
  {
    private final DateTimeComparator dateTimeComp = DateTimeComparator.getInstance();

    @Override
    public int compare(Interval lhs, Interval rhs)
    {
      if (lhs.getChronology().equals(rhs.getChronology())) {
        int compare = Long.compare(lhs.getEndMillis(), rhs.getEndMillis());
        if (compare == 0) {
          return Long.compare(lhs.getStartMillis(), rhs.getStartMillis());
        }
        return compare;
      }
      int retVal = dateTimeComp.compare(lhs.getEnd(), rhs.getEnd());
      if (retVal == 0) {
        retVal = dateTimeComp.compare(lhs.getStart(), rhs.getStart());
      }
      return retVal;
    }
  };

  @Deprecated
  public static Comparator<Interval> intervals()
  {
    return intervalsByStartThenEnd();
  }

  public static Comparator<Interval> intervalsByStartThenEnd()
  {
    return INTERVAL_BY_START_THEN_END;
  }

  public static Comparator<Interval> intervalsByEndThenStart()
  {
    return INTERVAL_BY_END_THEN_START;
  }

}
