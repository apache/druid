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

package io.druid.java.util.common.guava;

import org.joda.time.DateTimeComparator;
import org.joda.time.Interval;

import java.util.Comparator;

/**
 */
public class Comparators
{
  /**
   * This is a "reverse" comparator.  Positive becomes negative, negative becomes positive and 0 (equal) stays the same.
   * This was poorly named as "inverse" as it's not really inverting a true/false relationship
   * 
   * @param baseComp
   * @param <T>
   * @return
   */
  public static <T> Comparator<T> inverse(final Comparator<T> baseComp)
  {
    return new Comparator<T>()
    {
      @Override
      public int compare(T t, T t1)
      {
        return baseComp.compare(t1, t);
      }
    };
  }

  /**
   * Use Guava Ordering.natural() instead
   *
   * @param <T>
   * @return
   */
  @Deprecated
  public static <T extends Comparable> Comparator<T> comparable()
  {
    return new Comparator<T>()
    {
      @Override
      public int compare(T t, T t1)
      {
        return t.compareTo(t1);
      }
    };
  }

  private static final Comparator<Interval> INTERVAL_BY_START_THEN_END = new Comparator<Interval>()
  {
    private final DateTimeComparator dateTimeComp = DateTimeComparator.getInstance();

    @Override
    public int compare(Interval lhs, Interval rhs)
    {
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
