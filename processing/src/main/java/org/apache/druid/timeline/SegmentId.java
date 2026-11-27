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

package org.apache.druid.timeline;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.stream.IntStream;

/**
 * Identifier of {@link DataSegment}.
 */
@PublicApi
public final class SegmentId implements Comparable<SegmentId>
{
  /*
   * Implementation note: this class must be optimized for resident memory footprint, because segment data consumes
   * a lot of heap memory on Druid Broker and Coordinator nodes.
   *
   * This class is separate from org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec
   * because in a lot of places segment ids are transmitted as "segment id strings" that don't contain enough
   * information to deconstruct the ShardSpec. Also, even a single extra field is important for SegmentIds, because it
   * adds to the memory footprint considerably.
   *
   * The difference between this class and {@link org.apache.druid.query.SegmentDescriptor} is that the latter is
   * a "light" version of SegmentId, that only contains the interval, version, and partition number. It's used where the
   * data source, another essential part of SegmentId is determined by the context (e. g. in {@link
   * org.apache.druid.client.CachingClusteredClient}, where SegmentDescriptor is used when Brokers tell data servers
   * which segments to include for a particular query) and where having lean JSON representations is important, because
   * it's actively transferred detween Druid nodes. It's also for this reason that the JSON field names of
   * SegmentDescriptor are abbreviated.
   *
   * API design note: "SegmentId" is chosen as the name for this class instead of more verbose "SegmentIdentifier" or
   * "DataSegmentIdentifier" because it's used very frequently and a long class name adds noticeable clutter. Variables
   * of SegmentId type are recommended to be named "segmentId" rather than "identifier" or "segmentIdentifier".
   */

  /**
   * {@link #dataSource} field values are stored as canonical strings to decrease memory required for large numbers of
   * segment identifiers.
   */
  private static final Interner<String> STRING_INTERNER = Interners.newWeakInterner();

  /**
   * Store Intervals since creating them each time before returning is an expensive operation
   * To decrease the memory required for storing intervals, intern them, since the number of distinct values is "low"
   */
  private static final Interner<Interval> INTERVAL_INTERNER = Interners.newWeakInterner();

  private static final char DELIMITER = '_';
  private static final Splitter DELIMITER_SPLITTER = Splitter.on(DELIMITER);
  private static final Joiner DELIMITER_JOINER = Joiner.on(DELIMITER);

  private static final int DATE_TIME_SIZE_UPPER_LIMIT = "yyyy-MM-ddTHH:mm:ss.SSS+00:00".length();

  public static SegmentId of(String dataSource, Interval interval, String version, int partitionNum)
  {
    return new SegmentId(dataSource, interval, version, partitionNum);
  }

  public static SegmentId of(String dataSource, Interval interval, String version, @Nullable ShardSpec shardSpec)
  {
    return of(dataSource, interval, version, shardSpec != null ? shardSpec.getPartitionNum() : 0);
  }

  /**
   * Tries to parse a segment id from the given String representation, or returns null on failure. If returns a non-null
   * {@code SegmentId} object, calling {@link #toString()} on the latter is guaranteed to return a string equal to the
   * argument string of the {@code tryParse()} call.
   *
   * It is possible that this method may incorrectly parse a segment id, for example if the dataSource name in the
   * segment id contains a DateTime parseable string such as 'datasource_2000-01-01T00:00:00.000Z' and dataSource was
   * provided as 'datasource'. The desired behavior in this case would be to return null since the identifier does not
   * actually belong to the provided dataSource but a non-null result would be returned. This is an edge case that would
   * currently only affect paged select queries with a union dataSource of two similarly-named dataSources as in the
   * given example.
   *
   * Another source of ambiguity is the end of a segment id like '_123' - it could always be interpreted either as the
   * partitionNum of the segment id, or as the end of the version, with the implicit partitionNum of 0. This method
   * prefers the first iterpretation. To iterate all possible parsings of a segment id, use {@link
   * #iteratePossibleParsingsWithDataSource}.
   *
   * @param dataSource the dataSource corresponding to this segment id
   * @param segmentId segment id
   * @return a {@link SegmentId} object if the segment id could be parsed, null otherwise
   */
  @Nullable
  public static SegmentId tryParse(String dataSource, String segmentId)
  {
    List<SegmentId> possibleParsings = iteratePossibleParsingsWithDataSource(dataSource, segmentId);
    return possibleParsings.isEmpty() ? null : possibleParsings.get(0);
  }

  /**
   * Returns a (potentially empty) lazy iteration of all possible valid parsings of the given segment id string into
   * {@code SegmentId} objects.
   *
   * Warning: most of the parsing work is repeated each time {@link Iterable#iterator()} of this iterable is consumed,
   * so it should be consumed only once if possible.
   */
  public static Iterable<SegmentId> iterateAllPossibleParsings(String segmentId)
  {
    List<String> splits = DELIMITER_SPLITTER.splitToList(segmentId);
    String probableDataSource = tryExtractMostProbableDataSource(segmentId);
    // Iterate parsings with the most probably data source first to allow the users of iterateAllPossibleParsings() to
    // break from the iteration earlier with higher probability.
    if (probableDataSource != null) {
      List<SegmentId> probableParsings = iteratePossibleParsingsWithDataSource(probableDataSource, segmentId);
      Iterable<SegmentId> otherPossibleParsings = () -> IntStream
          .range(1, splits.size() - 3)
          .mapToObj(dataSourceDelimiterOrder -> DELIMITER_JOINER.join(splits.subList(0, dataSourceDelimiterOrder)))
          .filter(dataSource -> dataSource.length() != probableDataSource.length())
          .flatMap(dataSource -> iteratePossibleParsingsWithDataSource(dataSource, segmentId).stream())
          .iterator();
      return Iterables.concat(probableParsings, otherPossibleParsings);
    } else {
      return () -> IntStream
          .range(1, splits.size() - 3)
          .mapToObj(dataSourceDelimiterOrder -> {
            String dataSource = DELIMITER_JOINER.join(splits.subList(0, dataSourceDelimiterOrder));
            return iteratePossibleParsingsWithDataSource(dataSource, segmentId);
          })
          .flatMap(List::stream)
          .iterator();
    }
  }

  /**
   * Returns a list of either 0, 1 or 2 elements containing possible parsings if the given segment id String
   * representation with the given data source name. Returns an empty list when parsing into a valid {@code SegmentId}
   * object is impossible. Returns a list of a single element when the given segment id doesn't end with
   * '_[any positive number]', that means that the implicit partitionNum is 0. Otherwise the end of the segment id
   * is interpreted in two ways: with the explicit partitionNum (the first element in the returned list), and with the
   * implicit partitionNum of 0 and the version that ends with '_[any positive number]' (the second element in the
   * returned list).
   */

  public static List<SegmentId> iteratePossibleParsingsWithDataSource(String dataSource, String segmentId)
  {
    if (!segmentId.startsWith(dataSource) || segmentId.charAt(dataSource.length()) != DELIMITER) {
      return Collections.emptyList();
    }

    final int remainingStart = dataSource.length() + 1;

    final int firstDelimiter = segmentId.indexOf(DELIMITER, remainingStart);
    if (firstDelimiter < 0) {
      return Collections.emptyList();
    }
    final int secondDelimiter = segmentId.indexOf(DELIMITER, firstDelimiter + 1);
    if (secondDelimiter < 0) {
      return Collections.emptyList();
    }

    DateTime start;
    DateTime end;
    try {
      start = parseSegmentDateTime(segmentId.substring(remainingStart, firstDelimiter));
      end = parseSegmentDateTime(segmentId.substring(firstDelimiter + 1, secondDelimiter));
    }
    catch (IllegalArgumentException e) {
      return Collections.emptyList();
    }
    if (start.compareTo(end) >= 0) {
      return Collections.emptyList();
    }

    final List<SegmentId> possibleParsings = new ArrayList<>(2);
    final int lastDelimiter = segmentId.lastIndexOf(DELIMITER);
    if (lastDelimiter <= secondDelimiter) {
      // Exactly 3 parts after dataSource: start, end, version. No trailing partition-number candidate.
      String version = segmentId.substring(secondDelimiter + 1);
      possibleParsings.add(of(dataSource, new Interval(start, end), version, 0));
    } else {
      // More than 3 parts: the last part is a candidate partition number, and the ambiguous case where it's
      // actually part of the version is also returned (matching the original Splitter/Joiner-based behavior).
      String version = segmentId.substring(secondDelimiter + 1, lastDelimiter);
      String trail = segmentId.substring(lastDelimiter + 1);
      Integer possiblePartitionNum = Ints.tryParse(trail);
      if (possiblePartitionNum != null && possiblePartitionNum > 0) {
        possibleParsings.add(of(dataSource, new Interval(start, end), version, possiblePartitionNum));
      }
      possibleParsings.add(of(dataSource, new Interval(start, end), version + '_' + trail, 0));
    }
    return possibleParsings;
  }

  /**
   * Parses one interval endpoint from a segment id string. Fast path for the canonical UTC timestamp format Druid
   * always writes when serializing a SegmentId ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", exactly 24 characters): all digit
   * extraction and validation is inlined here, with no helper-method calls, so the JIT has a single flat method to
   * compile rather than a chain of small call frames. Falls back to the slower, more lenient
   * {@link DateTimes#ISO_DATE_TIME} parser for any input that doesn't match this exact shape (no millis, a
   * non-'Z' offset, date-only, etc.), so behavior for non-canonical input is unchanged.
   */
  private static DateTime parseSegmentDateTime(String s)
  {
    if (s.length() == 24
        && s.charAt(4) == '-'
        && s.charAt(7) == '-'
        && s.charAt(10) == 'T'
        && s.charAt(13) == ':'
        && s.charAt(16) == ':'
        && s.charAt(19) == '.'
        && s.charAt(23) == 'Z') {
      final char c0 = s.charAt(0);
      final char c1 = s.charAt(1);
      final char c2 = s.charAt(2);
      final char c3 = s.charAt(3);
      final char c5 = s.charAt(5);
      final char c6 = s.charAt(6);
      final char c8 = s.charAt(8);
      final char c9 = s.charAt(9);
      final char c11 = s.charAt(11);
      final char c12 = s.charAt(12);
      final char c14 = s.charAt(14);
      final char c15 = s.charAt(15);
      final char c17 = s.charAt(17);
      final char c18 = s.charAt(18);
      final char c20 = s.charAt(20);
      final char c21 = s.charAt(21);
      final char c22 = s.charAt(22);
      if (c0 >= '0' && c0 <= '9'
          && c1 >= '0' && c1 <= '9'
          && c2 >= '0' && c2 <= '9'
          && c3 >= '0' && c3 <= '9'
          && c5 >= '0' && c5 <= '9'
          && c6 >= '0' && c6 <= '9'
          && c8 >= '0' && c8 <= '9'
          && c9 >= '0' && c9 <= '9'
          && c11 >= '0' && c11 <= '9'
          && c12 >= '0' && c12 <= '9'
          && c14 >= '0' && c14 <= '9'
          && c15 >= '0' && c15 <= '9'
          && c17 >= '0' && c17 <= '9'
          && c18 >= '0' && c18 <= '9'
          && c20 >= '0' && c20 <= '9'
          && c21 >= '0' && c21 <= '9'
          && c22 >= '0' && c22 <= '9') {
        final int year = (c0 - '0') * 1000 + (c1 - '0') * 100 + (c2 - '0') * 10 + (c3 - '0');
        final int month = (c5 - '0') * 10 + (c6 - '0');
        final int day = (c8 - '0') * 10 + (c9 - '0');
        final int hour = (c11 - '0') * 10 + (c12 - '0');
        final int minute = (c14 - '0') * 10 + (c15 - '0');
        final int second = (c17 - '0') * 10 + (c18 - '0');
        final int millis = (c20 - '0') * 100 + (c21 - '0') * 10 + (c22 - '0');
        try {
          return new DateTime(year, month, day, hour, minute, second, millis, ISOChronology.getInstanceUTC());
        }
        catch (IllegalArgumentException e) {
          // Out-of-range field (e.g. month 13): fall through to the lenient parser below.
        }
      }
    }
    return DateTimes.ISO_DATE_TIME.parse(s);
  }

  /**
   * Heuristically tries to extract the most probable data source from a String segment id representation, or returns
   * null on failure.
   *
   * This method is not guaranteed to return a non-null data source given a valid String segment id representation.
   */
  @VisibleForTesting
  @Nullable
  static String tryExtractMostProbableDataSource(String segmentId)
  {
    Matcher dateTimeMatcher = DateTimes.COMMON_DATE_TIME_PATTERN.matcher(segmentId);
    while (true) {
      if (!dateTimeMatcher.find()) {
        return null;
      }
      int dataSourceEnd = dateTimeMatcher.start() - 1;
      if (segmentId.charAt(dataSourceEnd) != DELIMITER) {
        continue;
      }
      return segmentId.substring(0, dataSourceEnd);
    }
  }


  /**
   * Creates a merged SegmentId for the given data source, interval and partition number. Used when segments are
   * merged.
   */
  public static SegmentId merged(String dataSource, Interval interval, int partitionNum)
  {
    return of(dataSource, interval, "merged", partitionNum);
  }

  /**
   * Creates a dummy SegmentId with the given data source. This method is useful in benchmark and test code.
   */
  public static SegmentId dummy(String dataSource)
  {
    return of(dataSource, Intervals.ETERNITY, "dummy_version", 0);
  }

  /**
   * Creates a dummy SegmentId with the given data source and partition number.
   * This method is useful in benchmark and test code.
   */
  public static SegmentId dummy(String dataSource, int partitionNum)
  {
    return of(dataSource, Intervals.ETERNITY, "dummy_version", partitionNum);
  }

  private final String dataSource;
  private final Interval interval;
  private final String version;
  private final int partitionNum;

  /**
   * Cache the hash code eagerly, because SegmentId is almost always expected to be used as a map key or
   * for map lookup.
   */
  private final int hashCode;

  private SegmentId(String dataSource, Interval interval, String version, int partitionNum)
  {
    this.dataSource = STRING_INTERNER.intern(Objects.requireNonNull(dataSource));
    this.interval = INTERVAL_INTERNER.intern(Objects.requireNonNull(interval));
    // Versions are timestamp-based Strings, interning of them doesn't make sense. If this is not the case, interning
    // could be conditionally allowed via a system property.
    this.version = Objects.requireNonNull(version);
    this.partitionNum = partitionNum;
    this.hashCode = computeHashCode();
  }

  private int computeHashCode()
  {
    // Start with partitionNum and version hash codes, because they are often little sequential numbers. If they are
    // added in the end of the chain, resulting hashCode of SegmentId could have worse distribution.
    int hashCode = partitionNum;
    // 1000003 is a constant used in Google AutoValue, provides a little better distribution than 31
    hashCode = hashCode * 1000003 + version.hashCode();

    hashCode = hashCode * 1000003 + dataSource.hashCode();
    hashCode = hashCode * 1000003 + interval.hashCode();
    return hashCode;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public DateTime getIntervalStart()
  {
    return new DateTime(interval.getStartMillis(), interval.getChronology());
  }

  public DateTime getIntervalEnd()
  {
    return new DateTime(interval.getEndMillis(), interval.getChronology());
  }

  public Interval getInterval()
  {
    return interval;
  }

  public String getVersion()
  {
    return version;
  }

  public int getPartitionNum()
  {
    return partitionNum;
  }

  public SegmentId withInterval(Interval newInterval)
  {
    return of(dataSource, newInterval, version, partitionNum);
  }

  /**
   * Returns a descriptor that references the entire time range of the segment.
   */
  public SegmentDescriptor toDescriptor()
  {
    return new SegmentDescriptor(
        Intervals.utc(interval.getStartMillis(), interval.getEndMillis()),
        version,
        partitionNum
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SegmentId)) {
      return false;
    }
    SegmentId that = (SegmentId) o;
    // Compare hashCode instead of partitionNum: break the chain quicker if the objects are not equal. If the hashCodes
    // are equal as well as all other fields used to compute them, the partitionNums are also guaranteed to be equal.
    return hashCode == that.hashCode &&
           dataSource.equals(that.dataSource) &&
           interval.equals(that.interval) &&
           version.equals(that.version);
  }

  @Override
  public int hashCode()
  {
    return hashCode;
  }

  @Override
  public int compareTo(SegmentId o)
  {
    int result = dataSource.compareTo(o.dataSource);
    if (result != 0) {
      return result;
    }
    result = Long.compare(interval.getStartMillis(), o.interval.getStartMillis());
    if (result != 0) {
      return result;
    }
    result = Long.compare(interval.getEndMillis(), o.interval.getEndMillis());
    if (result != 0) {
      return result;
    }
    result = version.compareTo(o.version);
    if (result != 0) {
      return result;
    }
    return Integer.compare(partitionNum, o.partitionNum);
  }

  @JsonValue
  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(safeUpperLimitOfStringSize());

    sb.append(dataSource).append(DELIMITER)
      .append(getIntervalStart()).append(DELIMITER)
      .append(getIntervalEnd()).append(DELIMITER)
      .append(version);

    if (partitionNum != 0) {
      sb.append(DELIMITER).append(partitionNum);
    }

    return sb.toString();
  }

  public int safeUpperLimitOfStringSize()
  {
    int delimiters = 4;
    int partitionNumSizeUpperLimit = 3; // less than 1000 partitions
    return dataSource.length() +
           version.length() +
           (DATE_TIME_SIZE_UPPER_LIMIT * 2) + // interval start and end
           delimiters +
           partitionNumSizeUpperLimit;
  }
}
