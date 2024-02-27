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

package org.apache.druid.java.util.common.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class Granularity implements Cacheable
{
  public static final Comparator<Granularity> IS_FINER_THAN = new Comparator<Granularity>()
  {
    @Override
    /**
     * Decide whether this granularity is finer than the other granularity
     *
     * @param left The left granularity
     * @param right The right granularity
     * @return -1 if left granularity is finer, 0 if it is the same, 1 if it is greater
     */
    public int compare(Granularity left, Granularity right)
    {
      long leftDuration = left.bucket(DateTimes.EPOCH).toDurationMillis();
      long rightDuration = right.bucket(DateTimes.EPOCH).toDurationMillis();
      if (leftDuration < rightDuration) {
        return -1;
      } else if (leftDuration == rightDuration) {
        return 0;
      } else {
        return 1;
      }
    }
  };
  /**
   * Default patterns for parsing paths.
   */
  private static final Pattern DEFAULT_PATH_PATTERN =
      Pattern.compile(
          "^.*[Yy]=(\\d{4})/(?:[Mm]=(\\d{2})/(?:[Dd]=(\\d{2})/(?:[Hh]=(\\d{2})/(?:[Mm]=(\\d{2})/(?:[Ss]=(\\d{2})/)?)?)?)?)?.*$"
      );
  private static final Pattern HIVE_PATH_PATTERN =
      Pattern.compile("^.*dt=(\\d{4})(?:-(\\d{2})(?:-(\\d{2})(?:-(\\d{2})(?:-(\\d{2})(?:-(\\d{2})?)?)?)?)?)?/.*$");

  @JsonCreator
  public static Granularity fromString(String str)
  {
    return GranularityType.valueOf(StringUtils.toUpperCase(str)).getDefaultGranularity();
  }

  /**
   * simple merge strategy on query granularity that checks if all are equal or else
   * returns null. this can be improved in future but is good enough for most use-cases.
   */
  public static Granularity mergeGranularities(List<Granularity> toMerge)
  {
    if (toMerge == null || toMerge.size() == 0) {
      return null;
    }

    Granularity result = toMerge.get(0);
    for (int i = 1; i < toMerge.size(); i++) {
      if (!Objects.equals(result, toMerge.get(i))) {
        return null;
      }
    }

    return result;
  }

  /**
   * Returns a list of standard granularities that are equal to, or finer than, a provided granularity.
   *
   * ALL will not be returned unless the provided granularity is ALL. NONE will never be returned, even if the
   * provided granularity is NONE. This is because the main usage of this function in production is segment
   * allocation, and we do not wish to generate NONE-granular segments.
   *
   * The list of granularities returned contains WEEK only if the requested granularity is WEEK.
   */
  public static List<Granularity> granularitiesFinerThan(final Granularity gran0)
  {
    final List<Granularity> retVal = new ArrayList<>();
    final DateTime origin = (gran0 instanceof PeriodGranularity) ? ((PeriodGranularity) gran0).getOrigin() : null;
    final DateTimeZone tz = (gran0 instanceof PeriodGranularity) ? ((PeriodGranularity) gran0).getTimeZone() : null;
    for (GranularityType gran : GranularityType.values()) {
      // Exclude ALL, unless we're looking for granularities finer than ALL; always exclude NONE.
      if ((gran == GranularityType.ALL && !gran0.equals(Granularities.ALL)) || gran == GranularityType.NONE) {
        continue;
      }
      if (gran == GranularityType.WEEK && !gran0.equals(Granularities.WEEK)) {
        continue;
      }
      final Granularity segmentGranularity = gran.create(origin, tz);
      final long segmentGranularityDurationMillis = segmentGranularity.bucket(DateTimes.EPOCH).toDurationMillis();
      final long gran0DurationMillis = gran0.bucket(DateTimes.EPOCH).toDurationMillis();
      if (segmentGranularityDurationMillis <= gran0DurationMillis) {
        retVal.add(segmentGranularity);
      }
    }
    retVal.sort((g1, g2) -> {
      long duration1 = g2.bucket(DateTimes.EPOCH).toDurationMillis();
      long duration2 = g1.bucket(DateTimes.EPOCH).toDurationMillis();
      return Longs.compare(duration1, duration2);
    });
    return retVal;
  }

  public abstract DateTimeFormatter getFormatter(Formatter type);

  public abstract long increment(long time);

  public abstract DateTime increment(DateTime time);

  public abstract long bucketStart(long time);

  public abstract DateTime bucketStart(DateTime time);

  public abstract DateTime toDate(String filePath, Formatter formatter);

  /**
   * Return true only if the time chunks populated by this granularity includes the given interval time chunk. The
   * interval must fit exactly into the scheme of the granularity for this to return true
   */
  public abstract boolean isAligned(Interval interval);

  public DateTimeZone getTimeZone()
  {
    return DateTimeZone.UTC;
  }

  public DateTime bucketEnd(DateTime time)
  {
    return increment(bucketStart(time));
  }

  public DateTime toDateTime(long offset)
  {
    return DateTimes.utc(offset);
  }

  public DateTime toDate(String filePath)
  {
    return toDate(filePath, Formatter.DEFAULT);
  }

  public final String toPath(DateTime time)
  {
    return getFormatter(Formatter.DEFAULT).print(time);
  }

  /**
   * Return a granularity-sized Interval containing a particular DateTime.
   */
  public final Interval bucket(DateTime t)
  {
    DateTime start = bucketStart(t);
    return new Interval(start, increment(start));
  }

  // Used by the toDate implementations.
  final Integer[] getDateValues(String filePath, Formatter formatter)
  {
    Pattern pattern = DEFAULT_PATH_PATTERN;
    switch (formatter) {
      case DEFAULT:
      case LOWER_DEFAULT:
        break;
      case HIVE:
        pattern = HIVE_PATH_PATTERN;
        break;
      default:
        throw new IAE("Format %s not supported", formatter);
    }

    Matcher matcher = pattern.matcher(filePath);

    // The size is "7" b/c this array contains standard
    // datetime field values namely:
    // year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute,
    // and index 0 is unused.
    Integer[] vals = new Integer[7];
    if (matcher.matches()) {
      for (int i = 1; i <= matcher.groupCount(); i++) {
        vals[i] = (matcher.group(i) != null) ? Integer.parseInt(matcher.group(i)) : null;
      }
    }

    return vals;
  }

  /**
   * Decides whether this granularity is finer than the other granularity
   *
   * @return true if this {@link Granularity} is finer than the passed one
   */
  public boolean isFinerThan(Granularity g)
  {
    return IS_FINER_THAN.compare(this, g) < 0;
  }

  /**
   * Return an iterable of granular buckets that overlap a particular interval.
   *
   * In cases where the number of granular buckets is very large, the Iterable returned by this method will take
   * an excessive amount of time to compute, and materializing it into a collection will take an excessive amount
   * of memory. For example, this happens in the extreme case of an input interval of
   * {@link org.apache.druid.java.util.common.Intervals#ETERNITY} and any granularity other than
   * {@link Granularities#ALL}, as well as cases like an input interval of ten years with {@link Granularities#SECOND}.
   *
   * To avoid issues stemming from large numbers of buckets, this method should be avoided, and code that uses
   * this method should be rewritten to use some other approach. For example: rather than computing all possible
   * buckets in a wide time range, only process buckets related to actual data points that appear.
   */
  public Iterable<Interval> getIterable(final Interval input)
  {
    return new IntervalIterable(input);
  }

  public enum Formatter
  {
    DEFAULT,
    HIVE,
    LOWER_DEFAULT
  }

  private class IntervalIterable implements Iterable<Interval>
  {
    private final Interval inputInterval;

    private IntervalIterable(Interval inputInterval)
    {
      this.inputInterval = inputInterval;
    }

    @Override
    public Iterator<Interval> iterator()
    {
      return new IntervalIterator(inputInterval);
    }
  }

  private class IntervalIterator implements Iterator<Interval>
  {
    private final Interval inputInterval;

    private long currStart;
    private long currEnd;

    private IntervalIterator(Interval inputInterval)
    {
      this.inputInterval = inputInterval;

      currStart = bucketStart(inputInterval.getStartMillis());
      currEnd = increment(currStart);
    }

    @Override
    public boolean hasNext()
    {
      return currStart < inputInterval.getEndMillis();
    }

    @Override
    public Interval next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException("There are no more intervals");
      }
      Interval retVal = new Interval(currStart, currEnd, getTimeZone());

      currStart = currEnd;
      currEnd = increment(currStart);

      return retVal;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }
}
