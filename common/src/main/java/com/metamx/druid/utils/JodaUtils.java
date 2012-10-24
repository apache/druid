package com.metamx.druid.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Comparators;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

/**
 */
public class JodaUtils
{
  public static ArrayList<Interval> condenseIntervals(Iterable<Interval> intervals)
  {
    ArrayList<Interval> retVal = Lists.newArrayList();

    TreeSet<Interval> sortedIntervals = Sets.newTreeSet(Comparators.intervalsByStartThenEnd());
    for (Interval interval : intervals) {
      sortedIntervals.add(interval);
    }
    
    if (sortedIntervals.isEmpty()) {
      return Lists.newArrayList();
    }

    Iterator<Interval> intervalsIter = sortedIntervals.iterator();
    Interval currInterval = intervalsIter.next();
    while (intervalsIter.hasNext()) {
      Interval next = intervalsIter.next();

      if (currInterval.overlaps(next) || currInterval.abuts(next)) {
        currInterval = new Interval(currInterval.getStart(), next.getEnd());
      } else {
        retVal.add(currInterval);
        currInterval = next;
      }
    }
    retVal.add(currInterval);

    return retVal;
  }

  public static DateTime minDateTime(DateTime... times)
  {
    if (times == null) {
      return null;
    }

    switch (times.length) {
      case 0: return null;
      case 1: return times[0];
      default:
        DateTime min = times[0];
        for (int i = 1; i < times.length; ++i) {
          min = min.isBefore(times[i]) ? min : times[i];
        }
        return min;
    }
  }

  public static DateTime maxDateTime(DateTime... times)
  {
    if (times == null) {
      return null;
    }

    switch (times.length) {
      case 0: return null;
      case 1: return times[0];
      default:
        DateTime max = times[0];
        for (int i = 1; i < times.length; ++i) {
          max = max.isAfter(times[i]) ? max : times[i];
        }
        return max;
    }
  }
}
