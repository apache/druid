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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
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

    String remaining = segmentId.substring(dataSource.length() + 1);
    List<String> splits = DELIMITER_SPLITTER.splitToList(remaining);
    if (splits.size() < 3) {
      return Collections.emptyList();
    }

    DateTime start;
    DateTime end;
    try {
      start = DateTimes.ISO_DATE_TIME.parse(splits.get(0));
      end = DateTimes.ISO_DATE_TIME.parse(splits.get(1));
    }
    catch (IllegalArgumentException e) {
      return Collections.emptyList();
    }
    if (start.compareTo(end) >= 0) {
      return Collections.emptyList();
    }
    List<SegmentId> possibleParsings = new ArrayList<>(2);
    String version = DELIMITER_JOINER.join(splits.subList(2, Math.max(splits.size() - 1, 3)));
    String trail = splits.size() > 3 ? splits.get(splits.size() - 1) : null;
    if (trail != null) {
      Integer possiblePartitionNum = Ints.tryParse(trail);
      if (possiblePartitionNum != null && possiblePartitionNum > 0) {
        possibleParsings.add(of(dataSource, new Interval(start, end), version, possiblePartitionNum));
      }
      version = version + '_' + trail;
    }
    possibleParsings.add(of(dataSource, new Interval(start, end), version, 0));
    return possibleParsings;
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

  public static Function<String, Interval> makeIntervalExtractor(final String dataSource)
  {
    return identifier -> {
      SegmentId segmentIdentifierParts = tryParse(dataSource, identifier);
      if (segmentIdentifierParts == null) {
        throw new IAE("Invalid identifier [%s]", identifier);
      }

      return segmentIdentifierParts.getInterval();
    };
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
  /**
   * {@code intervalStartMillis}, {@link #intervalEndMillis} and {@link #intervalChronology} are the three fields of
   * an {@link Interval}. Storing them directly to flatten the structure and reduce the heap space consumption.
   */
  private final long intervalStartMillis;
  private final long intervalEndMillis;
  @Nullable
  private final Chronology intervalChronology;
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
    this.intervalStartMillis = interval.getStartMillis();
    this.intervalEndMillis = interval.getEndMillis();
    this.intervalChronology = interval.getChronology();
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
    hashCode = hashCode * 1000003 + Long.hashCode(intervalStartMillis);
    hashCode = hashCode * 1000003 + Long.hashCode(intervalEndMillis);
    hashCode = hashCode * 1000003 + Objects.hashCode(intervalChronology);
    return hashCode;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public DateTime getIntervalStart()
  {
    return new DateTime(intervalStartMillis, intervalChronology);
  }

  public DateTime getIntervalEnd()
  {
    return new DateTime(intervalEndMillis, intervalChronology);
  }

  public Interval getInterval()
  {
    return new Interval(intervalStartMillis, intervalEndMillis, intervalChronology);
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

  public SegmentDescriptor toDescriptor()
  {
    return new SegmentDescriptor(Intervals.utc(intervalStartMillis, intervalEndMillis), version, partitionNum);
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
           intervalStartMillis == that.intervalStartMillis &&
           intervalEndMillis == that.intervalEndMillis &&
           Objects.equals(intervalChronology, that.intervalChronology) &&
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
    result = Long.compare(intervalStartMillis, o.intervalStartMillis);
    if (result != 0) {
      return result;
    }
    result = Long.compare(intervalEndMillis, o.intervalEndMillis);
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

  private int safeUpperLimitOfStringSize()
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
