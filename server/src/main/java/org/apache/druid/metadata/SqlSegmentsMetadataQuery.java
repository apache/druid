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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.segment.cache.SegmentSchemaRecord;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.SQLStatement;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An object that is used to query the segments table in the metadata store.
 * Each instance of this class is scoped to a single {@link Handle} and is meant
 * to be short-lived.
 */
public class SqlSegmentsMetadataQuery
{
  private static final Logger log = new Logger(SqlSegmentsMetadataQuery.class);

  /**
   * Maximum number of intervals to consider for a batch.
   * This is similar to {@link IndexerSQLMetadataStorageCoordinator#MAX_NUM_SEGMENTS_TO_ANNOUNCE_AT_ONCE}, but imposed
   * on the intervals size.
   */
  private static final int MAX_INTERVALS_PER_BATCH = 100;

  private final Handle handle;
  private final SQLMetadataConnector connector;
  private final MetadataStorageTablesConfig dbTables;
  private final ObjectMapper jsonMapper;

  private SqlSegmentsMetadataQuery(
      final Handle handle,
      final SQLMetadataConnector connector,
      final MetadataStorageTablesConfig dbTables,
      final ObjectMapper jsonMapper
  )
  {
    this.handle = handle;
    this.connector = connector;
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
  }

  /**
   * Create a query object. This instance is scoped to a single handle and is meant to be short-lived. It is okay
   * to use it for more than one query, though.
   */
  public static SqlSegmentsMetadataQuery forHandle(
      final Handle handle,
      final SQLMetadataConnector connector,
      final MetadataStorageTablesConfig dbTables,
      final ObjectMapper jsonMapper
  )
  {
    return new SqlSegmentsMetadataQuery(handle, connector, dbTables, jsonMapper);
  }

  /**
   * Create a DateTime object from a string. If the string is null or empty, return null.
   */
  @Nullable
  public static DateTime nullAndEmptySafeDate(String date)
  {
    return Strings.isNullOrEmpty(date) ? null : DateTimes.of(date);
  }

  /**
   * Retrieves segments for a given datasource that are marked used (i.e. published) in the metadata store, and that
   * *overlap* any interval in a particular collection of intervals. If the collection of intervals is empty, this
   * method will retrieve all used segments.
   * <p>
   * You cannot assume that segments returned by this call are actually active. Because there is some delay between
   * new segment publishing and the marking-unused of older segments, it is possible that some segments returned
   * by this call are overshadowed by other segments. To check for this, use
   * {@link SegmentTimeline#forSegments(Iterable)}.
   * <p>
   * This call does not return any information about realtime segments.
   *
   * @return a closeable iterator. You should close it when you are done.
   */
  public CloseableIterator<DataSegment> retrieveUsedSegments(
      final String dataSource,
      final Collection<Interval> intervals
  )
  {
    return retrieveUsedSegments(dataSource, intervals, null);
  }

  /**
   * Similar to {@link #retrieveUsedSegments}, but with an additional {@code versions} argument. When {@code versions}
   * is specified, all used segments in the specified {@code intervals} and {@code versions} are retrieved.
   */
  public CloseableIterator<DataSegment> retrieveUsedSegments(
      final String dataSource,
      final Collection<Interval> intervals,
      final List<String> versions
  )
  {
    return retrieveSegments(
        dataSource,
        intervals,
        versions,
        IntervalMode.OVERLAPS,
        true,
        null,
        null,
        null,
        null
    );
  }

  public CloseableIterator<DataSegmentPlus> retrieveUsedSegmentsPlus(
      String dataSource,
      Collection<Interval> intervals
  )
  {
    return retrieveSegmentsPlus(
        dataSource,
        intervals, null, IntervalMode.OVERLAPS, true, null, null, null, null
    );
  }

  /**
   * Retrieves the ID of the unused segment that has the highest partition
   * number amongst all unused segments that exactly match the given interval
   * and version.
   *
   * @return null if no unused segment exists for the given parameters.
   */
  @Nullable
  public SegmentId retrieveHighestUnusedSegmentId(
      String datasource,
      Interval interval,
      String version
  )
  {
    final Set<String> unusedSegmentIds =
        retrieveUnusedSegmentIdsForExactIntervalAndVersion(datasource, interval, version);
    log.debug(
        "Found [%,d] unused segments for datasource[%s] for interval[%s] and version[%s].",
        unusedSegmentIds.size(), datasource, interval, version
    );

    SegmentId unusedMaxId = null;
    int maxPartitionNum = -1;
    for (String id : unusedSegmentIds) {
      final SegmentId segmentId = IdUtils.getValidSegmentId(datasource, id);
      int partitionNum = segmentId.getPartitionNum();
      if (maxPartitionNum < partitionNum) {
        maxPartitionNum = partitionNum;
        unusedMaxId = segmentId;
      }
    }
    return unusedMaxId;
  }

  private Set<String> retrieveUnusedSegmentIdsForExactIntervalAndVersion(
      String dataSource,
      Interval interval,
      String version
  )
  {
    final String sql = StringUtils.format(
        "SELECT id FROM %1$s"
        + " WHERE used = :used"
        + " AND dataSource = :dataSource"
        + " AND version = :version"
        + " AND start = :start AND %2$send%2$s = :end",
        dbTables.getSegmentsTable(), connector.getQuoteString()
    );

    final Query<Map<String, Object>> query = handle
        .createQuery(sql)
        .setFetchSize(connector.getStreamingFetchSize())
        .bind("used", false)
        .bind("dataSource", dataSource)
        .bind("version", version)
        .bind("start", interval.getStart().toString())
        .bind("end", interval.getEnd().toString());

    try (final ResultIterator<String> iterator = query.map(StringMapper.FIRST).iterator()) {
      return ImmutableSet.copyOf(iterator);
    }
  }

  /**
   * Retrieves segments and their associated metadata for a given datasource that are marked unused and that are
   * *fully contained by* an optionally specified interval. If the interval specified is null, this method will
   * retrieve all unused segments.
   *
   * This call does not return any information about realtime segments.
   *
   * @param datasource      The name of the datasource
   * @param interval        an optional interval to search over.
   * @param limit           an optional maximum number of results to return. If none is specified, the results are
   *                        not limited.
   * @param lastSegmentId an optional last segment id from which to search for results. All segments returned are >
   *                      this segment lexigraphically if sortOrder is null or  {@link SortOrder#ASC}, or < this
   *                      segment lexigraphically if sortOrder is {@link SortOrder#DESC}. If none is specified, no
   *                      such filter is used.
   * @param sortOrder an optional order with which to return the matching segments by id, start time, end time. If
   *                  none is specified, the order of the results is not guarenteed.

   * Returns an iterable.
   */
  public List<DataSegmentPlus> iterateAllUnusedSegmentsForDatasource(
      final String datasource,
      @Nullable final Interval interval,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder
  )
  {
    final List<Interval> intervals = interval == null ? Intervals.ONLY_ETERNITY : List.of(interval);
    try (final CloseableIterator<DataSegmentPlus> iterator =
             retrieveUnusedSegmentsPlus(datasource, intervals, null, limit, lastSegmentId, sortOrder, null)) {
      return ImmutableList.copyOf(iterator);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Retrieves unused segments that are fully contained within the given interval.
   *
   * @param interval       Returned segments must be fully contained within this
   *                       interval
   * @param versions       Optional list of segment versions. If passed as null,
   *                       all segment versions are eligible.
   * @param limit          Maximum number of segments to return. If passed as null,
   *                       all segments are returned.
   * @param maxUpdatedTime Returned segments must have a {@code used_status_last_updated}
   *                       which is either null or earlier than this value.
   */
  public List<DataSegment> findUnusedSegments(
      String dataSource,
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUpdatedTime
  )
  {
    try (
        final CloseableIterator<DataSegment> iterator =
            retrieveUnusedSegments(dataSource, List.of(interval), versions, limit, null, null, maxUpdatedTime)
    ) {
      return ImmutableList.copyOf(iterator);
    }
    catch (IOException e) {
      throw InternalServerError.exception(e, "Error while reading unused segments");
    }
  }

  /**
   * Retrieves segments for a given datasource that are marked unused and that are <b>fully contained by</b> any interval
   * in a particular collection of intervals. If the collection of intervals is empty, this method will retrieve all
   * unused segments.
   * <p>
   * This call does not return any information about realtime segments.
   * </p>
   *
   * @param dataSource    The name of the datasource
   * @param intervals     The intervals to search over
   * @param versions      An optional list of unused segment versions to retrieve in the given {@code intervals}.
   *                      If unspecified, all versions of unused segments in the {@code intervals} must be retrieved. If an
   *                      empty list is passed, no segments are retrieved.
   * @param limit         The limit of segments to return
   * @param lastSegmentId the last segment id from which to search for results. All segments returned are >
   *                      this segment lexigraphically if sortOrder is null or ASC, or < this segment
   *                      lexigraphically if sortOrder is DESC.
   * @param sortOrder     Specifies the order with which to return the matching segments by start time, end time.
   *                      A null value indicates that order does not matter.
   * @param maxUsedStatusLastUpdatedTime The maximum {@code used_status_last_updated} time. Any unused segment in {@code intervals}
   *                                     with {@code used_status_last_updated} no later than this time will be included in the
   *                                     iterator. Segments without {@code used_status_last_updated} time (due to an upgrade
   *                                     from legacy Druid) will have {@code maxUsedStatusLastUpdatedTime} ignored
   *
   * @return a closeable iterator. You should close it when you are done.
   *
   */
  public CloseableIterator<DataSegment> retrieveUnusedSegments(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return retrieveSegments(
        dataSource,
        intervals,
        versions,
        IntervalMode.CONTAINS,
        false,
        limit,
        lastSegmentId,
        sortOrder,
        maxUsedStatusLastUpdatedTime
    );
  }

  /**
   * Similar to {@link #retrieveUnusedSegments}, but also retrieves associated metadata for the segments for a given
   * datasource that are marked unused and that are <b>fully contained by</b> any interval in a particular collection of
   * intervals. If the collection of intervals is empty, this method will retrieve all unused segments.
   *
   * This call does not return any information about realtime segments.
   *
   * @param dataSource    The name of the datasource
   * @param intervals     The intervals to search over
   * @param limit         The limit of segments to return
   * @param lastSegmentId the last segment id from which to search for results. All segments returned are >
   *                      this segment lexigraphically if sortOrder is null or ASC, or < this segment
   *                      lexigraphically if sortOrder is DESC.
   * @param sortOrder     Specifies the order with which to return the matching segments by start time, end time.
   *                      A null value indicates that order does not matter.
   * @param maxUsedStatusLastUpdatedTime The maximum {@code used_status_last_updated} time. Any unused segment in {@code intervals}
   *                                   with {@code used_status_last_updated} no later than this time will be included in the
   *                                   iterator. Segments without {@code used_status_last_updated} time (due to an upgrade
   *                                   from legacy Druid) will have {@code maxUsedStatusLastUpdatedTime} ignored

   * @return a closeable iterator. You should close it when you are done.
   */
  public CloseableIterator<DataSegmentPlus> retrieveUnusedSegmentsPlus(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return retrieveSegmentsPlus(
        dataSource,
        intervals,
        versions,
        IntervalMode.CONTAINS,
        false,
        limit,
        lastSegmentId,
        sortOrder,
        maxUsedStatusLastUpdatedTime
    );
  }

  /**
   * Retrieves IDs of used segments that belong to the datasource and overlap
   * the given interval.
   */
  public Set<SegmentId> retrieveUsedSegmentIds(
      final String dataSource,
      final Interval interval
  )
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("SELECT id FROM %s WHERE used = :used AND dataSource = :dataSource");

    // If the interval supports comparing as a string, bake it into the SQL
    final boolean compareAsString = Intervals.canCompareEndpointsAsStrings(interval);
    if (compareAsString) {
      sb.append(
          getConditionForIntervalsAndMatchMode(
              Collections.singletonList(interval),
              IntervalMode.OVERLAPS,
              connector.getQuoteString()
          )
      );
    }

    final Query<Map<String, Object>> sql = handle
        .createQuery(StringUtils.format(sb.toString(), dbTables.getSegmentsTable()))
        .setFetchSize(connector.getStreamingFetchSize())
        .bind("used", true)
        .bind("dataSource", dataSource);

    if (compareAsString) {
      bindIntervalsToQuery(sql, Collections.singletonList(interval));
    }

    final Set<SegmentId> segmentIds = new HashSet<>();
    try (final ResultIterator<String> iterator = sql.map(StringMapper.FIRST).iterator()) {
      while (iterator.hasNext()) {
        final String id = iterator.next();
        final SegmentId segmentId = SegmentId.tryParse(dataSource, id);
        if (segmentId == null) {
          throw DruidException.defensive(
              "Failed to parse SegmentId for id[%s] and dataSource[%s].",
              id, dataSource
          );
        }
        if (IntervalMode.OVERLAPS.apply(interval, segmentId.getInterval())) {
          segmentIds.add(segmentId);
        }
      }
    }
    return segmentIds;

  }

  /**
   * Retrieves segments for the given segment IDs from the metadata store.
   */
  public List<DataSegmentPlus> retrieveSegmentsById(
      String datasource,
      Set<SegmentId> segmentIds
  )
  {
    try (CloseableIterator<DataSegmentPlus> iterator
             = retrieveSegmentsByIdIterator(datasource, segmentIds, false)) {
      return ImmutableList.copyOf(iterator);
    }
    catch (IOException e) {
      throw DruidException.defensive(e, "Error while retrieving segments from metadata store");
    }
  }

  /**
   * Retrieves segments for the specified IDs in batches of a small size.
   *
   * @param includeSchemaInfo If true, additional metadata info such as number
   *                          of rows and schema fingerprint is also retrieved
   * @return CloseableIterator over the retrieved segments which must be closed
   * once the result has been handled. If the iterator is closed while reading
   * a batch of segments, queries for subsequent batches are not fired.
   */
  public CloseableIterator<DataSegmentPlus> retrieveSegmentsByIdIterator(
      final String datasource,
      final Set<SegmentId> segmentIds,
      final boolean includeSchemaInfo
  )
  {
    final List<String> ids = segmentIds.stream().map(SegmentId::toString).collect(Collectors.toList());
    final List<List<String>> partitionedSegmentIds = Lists.partition(ids, 100);

    // CloseableIterator to query segments in batches
    return new CloseableIterator<>()
    {
      // Start with a dummy empty batch. Only one result set is open at any point.
      CloseableIterator<DataSegmentPlus> currentBatch
          = CloseableIterators.withEmptyBaggage(Collections.emptyIterator());
      int currentBatchIndex = -1;

      @Override
      public void close() throws IOException
      {
        currentBatch.close();
      }

      @Override
      public boolean hasNext()
      {
        if (currentBatch.hasNext()) {
          return true;
        } else if (++currentBatchIndex < partitionedSegmentIds.size()) {
          // Close the current result set as it has been exhausted
          CloseableUtils.closeAndWrapExceptions(currentBatch);

          // Create a new result set for the next batch of segments
          currentBatch = retrieveSegmentBatchById(
              datasource,
              partitionedSegmentIds.get(currentBatchIndex),
              includeSchemaInfo
          );

          // If the currentBatch is empty, check subsequent ones recursively
          return hasNext();
        } else {
          return false;
        }
      }

      @Override
      public DataSegmentPlus next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        } else {
          return currentBatch.next();
        }
      }
    };
  }

  /**
   * Retrieves segments with additional metadata such as number of rows and
   * schema fingerprint.
   */
  public List<DataSegmentPlus> retrieveSegmentsWithSchemaById(
      String datasource,
      Set<SegmentId> segmentIds
  )
  {
    try (CloseableIterator<DataSegmentPlus> iterator
             = retrieveSegmentsByIdIterator(datasource, segmentIds, true)) {
      return ImmutableList.copyOf(iterator);
    }
    catch (IOException e) {
      throw DruidException.defensive(e, "Error while retrieving segments with schema from metadata store.");
    }
  }

  private CloseableIterator<DataSegmentPlus> retrieveSegmentBatchById(
      String datasource,
      List<String> segmentIds,
      boolean includeSchemaInfo
  )
  {
    if (segmentIds.isEmpty()) {
      return CloseableIterators.withEmptyBaggage(Collections.emptyIterator());
    }

    ResultIterator<DataSegmentPlus> resultIterator;
    if (includeSchemaInfo) {
      final Query<Map<String, Object>> query = handle.createQuery(
          StringUtils.format(
              "SELECT payload, used, schema_fingerprint, num_rows,"
              + " upgraded_from_segment_id, used_status_last_updated"
              + " FROM %s WHERE dataSource = :dataSource %s",
              dbTables.getSegmentsTable(), getParameterizedInConditionForColumn("id", segmentIds)
          )
      );

      bindColumnValuesToQueryWithInCondition("id", segmentIds, query);

      resultIterator = query
          .bind("dataSource", datasource)
          .setFetchSize(connector.getStreamingFetchSize())
          .map(
              (index, r, ctx) -> {
                String schemaFingerprint = (String) r.getObject(3);
                Long numRows = (Long) r.getObject(4);
                return new DataSegmentPlus(
                    JacksonUtils.readValue(jsonMapper, r.getBytes(1), DataSegment.class),
                    null,
                    nullAndEmptySafeDate(r.getString(6)),
                    r.getBoolean(2),
                    schemaFingerprint,
                    numRows,
                    r.getString(5)
                );
              }
          )
          .iterator();
    } else {
      final Query<Map<String, Object>> query = handle.createQuery(
          StringUtils.format(
              "SELECT payload, used, upgraded_from_segment_id, used_status_last_updated, created_date"
              + " FROM %s WHERE dataSource = :dataSource %s",
              dbTables.getSegmentsTable(), getParameterizedInConditionForColumn("id", segmentIds)
          )
      );

      bindColumnValuesToQueryWithInCondition("id", segmentIds, query);

      resultIterator = query
          .bind("dataSource", datasource)
          .setFetchSize(connector.getStreamingFetchSize())
          .map(
              (index, r, ctx) -> new DataSegmentPlus(
                  JacksonUtils.readValue(jsonMapper, r.getBytes(1), DataSegment.class),
                  DateTimes.of(r.getString(5)),
                  nullAndEmptySafeDate(r.getString(4)),
                  r.getBoolean(2),
                  null,
                  null,
                  r.getString(3)
              )
          )
          .iterator();
    }

    return CloseableIterators.wrap(resultIterator, resultIterator);
  }

  /**
   * Retrieves all used schema fingerprints present in the metadata store.
   */
  public Set<String> retrieveAllUsedSegmentSchemaFingerprints()
  {
    final String sql = StringUtils.format(
        "SELECT fingerprint FROM %s WHERE version = %s AND used = true",
        dbTables.getSegmentSchemasTable(), CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
    );
    return Set.copyOf(
        handle.createQuery(sql)
              .setFetchSize(connector.getStreamingFetchSize())
              .mapTo(String.class)
              .list()
    );
  }

  /**
   * Retrieves all used segment schemas present in the metadata store irrespective
   * of their last updated time.
   */
  public List<SegmentSchemaRecord> retrieveAllUsedSegmentSchemas()
  {
    final String sql = StringUtils.format(
        "SELECT fingerprint, payload FROM %s"
        + " WHERE version = %s AND used = true",
        dbTables.getSegmentSchemasTable(), CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
    );
    return retrieveValidSchemaRecordsWithQuery(handle.createQuery(sql));
  }

  /**
   * Retrieves segment schemas from the metadata store for the given fingerprints.
   */
  public List<SegmentSchemaRecord> retrieveUsedSegmentSchemasForFingerprints(
      Set<String> schemaFingerprints
  )
  {
    final List<List<String>> fingerprintBatches = Lists.partition(
        List.copyOf(schemaFingerprints),
        MAX_INTERVALS_PER_BATCH
    );

    final List<SegmentSchemaRecord> records = new ArrayList<>();
    for (List<String> fingerprintBatch : fingerprintBatches) {
      records.addAll(
          retrieveBatchOfSegmentSchemas(fingerprintBatch)
      );
    }

    return records;
  }

  /**
   * Retrieves a batch of segment schema records for the given fingerprints.
   */
  private List<SegmentSchemaRecord> retrieveBatchOfSegmentSchemas(List<String> schemaFingerprints)
  {
    final String sql = StringUtils.format(
        "SELECT fingerprint, payload FROM %s"
        + " WHERE version = %s AND used = true"
        + " %s",
        dbTables.getSegmentSchemasTable(),
        CentralizedDatasourceSchemaConfig.SCHEMA_VERSION,
        getParameterizedInConditionForColumn("fingerprint", schemaFingerprints)
    );

    final Query<Map<String, Object>> query = handle.createQuery(sql);
    bindColumnValuesToQueryWithInCondition("fingerprint", schemaFingerprints, query);

    return retrieveValidSchemaRecordsWithQuery(query);
  }

  private List<SegmentSchemaRecord> retrieveValidSchemaRecordsWithQuery(
      Query<Map<String, Object>> query
  )
  {
    return query.setFetchSize(connector.getStreamingFetchSize())
                .map((index, r, ctx) -> mapToSchemaRecord(r))
                .list()
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
  }

  /**
   * Marks the given segment IDs as used.
   *
   * @param segmentIds Segment IDs to update. For better performance, ensure that
   *                   these segment IDs are not already marked as used.
   * @param updateTime Updated segments will have their used_status_last_updated
   *                   column set to this value
   * @return Number of segments updated in the metadata store.
   */
  public int markSegmentsAsUsed(Set<SegmentId> segmentIds, DateTime updateTime)
  {
    return markSegments(segmentIds, true, updateTime);
  }

  /**
   * Marks the given segment IDs as unused.
   *
   * @param segmentIds Segment IDs to update. For better performance, ensure that
   *                   these segment IDs are not already marked as unused.
   * @param updateTime Updated segments will have their used_status_last_updated
   *                   column set to this value
   * @return Number of segments updated in the metadata store.
   */
  public int markSegmentsAsUnused(Set<SegmentId> segmentIds, DateTime updateTime)
  {
    return markSegments(segmentIds, false, updateTime);
  }

  /**
   * Marks the given segments as either used or unused.
   *
   * @return the number of segments actually modified.
   */
  private int markSegments(final Set<SegmentId> segmentIds, final boolean used, DateTime updateTime)
  {
    final String dataSource;

    if (segmentIds.isEmpty()) {
      return 0;
    } else {
      dataSource = segmentIds.iterator().next().getDataSource();
      if (segmentIds.stream().anyMatch(segment -> !dataSource.equals(segment.getDataSource()))) {
        throw new IAE("Segments to drop must all be part of the same datasource");
      }
    }

    final PreparedBatch batch =
        handle.prepareBatch(
            StringUtils.format(
                "UPDATE %s SET used = ?, used_status_last_updated = ? WHERE datasource = ? AND id = ?",
                dbTables.getSegmentsTable()
            )
        );

    for (SegmentId segmentId : segmentIds) {
      batch.add(used, updateTime.toString(), dataSource, segmentId.toString());
    }

    final int[] segmentChanges = batch.execute();
    return computeNumChangedSegments(
        segmentIds.stream().map(SegmentId::toString).collect(Collectors.toList()),
        segmentChanges
    );
  }

  /**
   * Marks all used segments that are <b>fully contained by</b> a particular interval
   * filtered by an optional list of versions as unused.
   *
   * @param interval   Only used segments fully contained within this interval
   *                   are eligible to be marked as unused
   * @param versions   List of eligible segment versions. If null or empty, all
   *                   versions are considered eligible to be marked as unused.
   * @param updateTime Updated segments will have their used_status_last_updated
   *                   column set to this value
   * @return Number of segments updated.
   */
  public int markSegmentsUnused(
      final String dataSource,
      final Interval interval,
      @Nullable final List<String> versions,
      final DateTime updateTime
  )
  {
    if (versions != null && versions.isEmpty()) {
      return 0;
    }

    if (Intervals.isEternity(interval)) {
      final StringBuilder sb = new StringBuilder();
      sb.append(
          StringUtils.format(
              "UPDATE %s SET used=:used, used_status_last_updated = :used_status_last_updated "
              + "WHERE dataSource = :dataSource AND used = true",
              dbTables.getSegmentsTable()
          )
      );

      if (versions != null) {
        sb.append(getParameterizedInConditionForColumn("version", versions));
      }

      final Update stmt = handle
          .createStatement(sb.toString())
          .bind("dataSource", dataSource)
          .bind("used", false)
          .bind("used_status_last_updated", updateTime.toString());

      if (versions != null) {
        bindColumnValuesToQueryWithInCondition("version", versions, stmt);
      }

      return stmt.execute();
    } else if (Intervals.canCompareEndpointsAsStrings(interval)
               && interval.getStart().getYear() == interval.getEnd().getYear()) {
      // Safe to write a WHERE clause with this interval. Note that it is unsafe if the years are different, because
      // that means extra characters can sneak in. (Consider a query interval like "2000-01-01/2001-01-01" and a
      // segment interval like "20001/20002".)
      final StringBuilder sb = new StringBuilder();
      sb.append(
          StringUtils.format(
              "UPDATE %s SET used=:used, used_status_last_updated = :used_status_last_updated "
              + "WHERE dataSource = :dataSource AND used = true AND %s",
              dbTables.getSegmentsTable(),
              IntervalMode.CONTAINS.makeSqlCondition(connector.getQuoteString(), ":start", ":end")
          )
      );

      if (versions != null) {
        sb.append(getParameterizedInConditionForColumn("version", versions));
      }

      final Update stmt = handle
          .createStatement(sb.toString())
          .bind("dataSource", dataSource)
          .bind("used", false)
          .bind("start", interval.getStart().toString())
          .bind("end", interval.getEnd().toString())
          .bind("used_status_last_updated", updateTime.toString());

      if (versions != null) {
        bindColumnValuesToQueryWithInCondition("version", versions, stmt);
      }
      return stmt.execute();
    } else {
      // Retrieve, then drop, since we can't write a WHERE clause directly.
      final Set<SegmentId> segments = ImmutableSet.copyOf(
          Iterators.transform(
              retrieveSegments(
                  dataSource,
                  Collections.singletonList(interval),
                  versions,
                  IntervalMode.CONTAINS,
                  true,
                  null,
                  null,
                  null,
                  null
              ),
              DataSegment::getId
          )
      );
      return markSegments(segments, false, updateTime);
    }
  }

  public boolean markSegmentAsUsed(SegmentId segmentId, DateTime updateTime)
  {
    return markSegments(Set.of(segmentId), true, updateTime) > 0;
  }

  public int markAllNonOvershadowedSegmentsAsUsed(String dataSource, DateTime updateTime)
  {
    return markNonOvershadowedSegmentsAsUsedInternal(dataSource, null, null, updateTime);
  }

  public int markNonOvershadowedSegmentsAsUsed(
      String dataSource,
      Interval interval,
      @Nullable List<String> versions,
      DateTime updateTime
  )
  {
    Preconditions.checkNotNull(interval);
    return markNonOvershadowedSegmentsAsUsedInternal(dataSource, interval, versions, updateTime);
  }

  private int markNonOvershadowedSegmentsAsUsedInternal(
      final String dataSourceName,
      final @Nullable Interval interval,
      final @Nullable List<String> versions,
      DateTime updateTime
  )
  {
    final List<DataSegment> unusedSegments = new ArrayList<>();
    final SegmentTimeline timeline = new SegmentTimeline();

    final List<Interval> intervals =
        interval == null ? Intervals.ONLY_ETERNITY : Collections.singletonList(interval);

    try (final CloseableIterator<DataSegment> iterator =
             retrieveUsedSegments(dataSourceName, intervals, versions)) {
      timeline.addSegments(iterator);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    try (final CloseableIterator<DataSegment> iterator =
             retrieveUnusedSegments(dataSourceName, intervals, versions, null, null, null, null)) {
      while (iterator.hasNext()) {
        final DataSegment dataSegment = iterator.next();
        timeline.add(dataSegment);
        unusedSegments.add(dataSegment);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return markNonOvershadowedSegmentsAsUsed(unusedSegments, timeline, updateTime);
  }

  private int markNonOvershadowedSegmentsAsUsed(
      List<DataSegment> unusedSegments,
      SegmentTimeline timeline,
      DateTime updateTime
  )
  {
    Set<SegmentId> nonOvershadowedSegments =
        unusedSegments.stream()
                      .filter(segment -> !timeline.isOvershadowed(segment))
                      .map(DataSegment::getId)
                      .collect(Collectors.toSet());

    return markSegmentsAsUsed(nonOvershadowedSegments, updateTime);
  }

  public int markNonOvershadowedSegmentsAsUsed(
      final String dataSource,
      final Set<SegmentId> segmentIds,
      final DateTime updateTime
  )
  {
    final List<DataSegment> unusedSegments = retrieveUnusedSegments(dataSource, segmentIds);
    final List<Interval> unusedSegmentsIntervals = JodaUtils.condenseIntervals(
        unusedSegments.stream().map(DataSegment::getInterval).collect(Collectors.toList())
    );

    // Create a timeline with all used and unused segments in this interval
    final SegmentTimeline timeline = SegmentTimeline.forSegments(unusedSegments);

    try (CloseableIterator<DataSegment> usedSegmentsOverlappingUnusedSegmentsIntervals =
             retrieveUsedSegments(dataSource, unusedSegmentsIntervals)) {
      timeline.addSegments(usedSegmentsOverlappingUnusedSegmentsIntervals);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return markNonOvershadowedSegmentsAsUsed(unusedSegments, timeline, updateTime);
  }

  private List<DataSegment> retrieveUnusedSegments(
      final String dataSource,
      final Set<SegmentId> segmentIds
  )
  {
    final List<DataSegmentPlus> retrievedSegments = retrieveSegmentsById(dataSource, segmentIds);

    final Set<SegmentId> unknownSegmentIds = new HashSet<>(segmentIds);
    final List<DataSegment> unusedSegments = new ArrayList<>();
    for (DataSegmentPlus entry : retrievedSegments) {
      final DataSegment segment = entry.getDataSegment();
      unknownSegmentIds.remove(segment.getId());
      if (Boolean.FALSE.equals(entry.getUsed())) {
        unusedSegments.add(segment);
      }
    }

    if (!unknownSegmentIds.isEmpty()) {
      throw InvalidInput.exception(
          "Could not find segment IDs[%s] for datasource[%s]",
          unknownSegmentIds, dataSource
      );
    }

    return unusedSegments;
  }

  public List<Interval> retrieveUnusedSegmentIntervals(
      final String dataSource,
      @Nullable final DateTime minStartTime,
      final DateTime maxEndTime,
      final int limit,
      final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    final boolean filterByStartTime = minStartTime != null;

    // Handle cases where used_status_last_updated IS NULL for backward compatibility
    final String sql = StringUtils.format(
        "SELECT start, %2$send%2$s FROM %1$s"
        + " WHERE dataSource = :dataSource AND used = false"
        + " AND %2$send%2$s <= :end %3$s"
        + " AND used_status_last_updated IS NOT NULL"
        + " AND used_status_last_updated <= :used_status_last_updated"
        + " ORDER BY start, %2$send%2$s",
        dbTables.getSegmentsTable(),
        connector.getQuoteString(),
        filterByStartTime ? " AND start >= :start" : ""
    );

    Query<Map<String, Object>> query = handle
        .createQuery(sql)
        .setFetchSize(connector.getStreamingFetchSize())
        .setMaxRows(limit)
        .bind("dataSource", dataSource)
        .bind("end", maxEndTime.toString())
        .bind("used_status_last_updated", maxUsedStatusLastUpdatedTime.toString());

    if (filterByStartTime) {
      query.bind("start", minStartTime.toString());
    }

    List<Interval> unusedIntervals = query
        .map((index, r, ctx) -> mapToInterval(r, dataSource))
        .list();

    return unusedIntervals.stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  /**
   * Gets unused segment intervals for the specified datasource. There is no
   * guarantee on the order of intervals in the list or on whether the limited
   * list contains the earliest or latest intervals present in the datasource.
   *
   * @return List of unused segment intervals containing upto {@code limit} entries.
   */
  public List<Interval> retrieveUnusedSegmentIntervals(String dataSource, int limit)
  {
    final String sql = StringUtils.format(
        "SELECT start, %2$send%2$s FROM %1$s"
        + " WHERE dataSource = :dataSource AND used = false"
        + " GROUP BY start, %2$send%2$s"
        + "  %3$s",
        dbTables.getSegmentsTable(), connector.getQuoteString(), connector.limitClause(limit)
    );

    final List<Interval> intervals = connector.inReadOnlyTransaction(
        (handle, status) ->
            handle.createQuery(sql)
                  .setFetchSize(connector.getStreamingFetchSize())
                  .bind("dataSource", dataSource)
                  .map((index, r, ctx) -> mapToInterval(r, dataSource))
                  .list()
    );

    return intervals.stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  /**
   * Retrieves unused segments that exactly match the given interval.
   *
   * @param interval       Returned segments must exactly match this interval.
   * @param maxUpdatedTime Returned segments must have a {@code used_status_last_updated}
   *                       which is either null or earlier than this value.
   * @param limit          Maximum number of segments to return
   */
  public List<DataSegment> retrieveUnusedSegmentsWithExactInterval(
      String dataSource,
      Interval interval,
      DateTime maxUpdatedTime,
      int limit
  )
  {
    final String sql = StringUtils.format(
        "SELECT id, payload FROM %1$s"
        + " WHERE dataSource = :dataSource AND used = false"
        + " AND %2$send%2$s = :end AND start = :start"
        + " AND (used_status_last_updated IS NULL OR used_status_last_updated <= :maxUpdatedTime)"
        + "  %3$s",
        dbTables.getSegmentsTable(), connector.getQuoteString(), connector.limitClause(limit)
    );

    final List<DataSegment> segments = connector.inReadOnlyTransaction(
        (handle, status) ->
            handle.createQuery(sql)
                  .setFetchSize(connector.getStreamingFetchSize())
                  .bind("dataSource", dataSource)
                  .bind("start", interval.getStart().toString())
                  .bind("end", interval.getEnd().toString())
                  .bind("maxUpdatedTime", maxUpdatedTime.toString())
                  .map((index, r, ctx) -> mapToSegment(r))
                  .list()
    );

    return segments.stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  /**
   * Retrieve the used segment for a given id if it exists in the metadata store and null otherwise
   */
  @Nullable
  public DataSegment retrieveUsedSegmentForId(SegmentId segmentId)
  {
    final String query = "SELECT payload FROM %s WHERE used = true AND id = :id";

    final List<DataSegment> segments = handle
        .createQuery(StringUtils.format(query, dbTables.getSegmentsTable()))
        .bind("id", segmentId.toString())
        .map((index, r, ctx) -> JacksonUtils.readValue(jsonMapper, r.getBytes(1), DataSegment.class))
        .list();

    return segments.isEmpty() ? null : segments.get(0);
  }

  /**
   * Retrieve the segment for a given id if it exists in the metadata store and null otherwise
   */
  @Nullable
  public DataSegment retrieveSegmentForId(SegmentId segmentId)
  {
    final String query = "SELECT payload FROM %s WHERE id = :id";

    final List<DataSegment> segments = handle
        .createQuery(StringUtils.format(query, dbTables.getSegmentsTable()))
        .bind("id", segmentId.toString())
        .map((index, r, ctx) -> JacksonUtils.readValue(jsonMapper, r.getBytes(1), DataSegment.class))
        .list();

    return segments.isEmpty() ? null : segments.get(0);
  }

  public List<SegmentIdWithShardSpec> retrievePendingSegmentIds(
      final String dataSource,
      final String sequenceName,
      final String sequencePreviousId
  )
  {
    final String sql = StringUtils.format(
        "SELECT payload FROM %s WHERE "
        + "dataSource = :dataSource AND "
        + "sequence_name = :sequence_name AND "
        + "sequence_prev_id = :sequence_prev_id",
        dbTables.getPendingSegmentsTable()
    );
    return handle
        .createQuery(sql)
        .bind("dataSource", dataSource)
        .bind("sequence_name", sequenceName)
        .bind("sequence_prev_id", sequencePreviousId)
        .map(
            (index, r, ctx) -> JacksonUtils.readValue(
                jsonMapper,
                r.getBytes("payload"),
                SegmentIdWithShardSpec.class
            )
        )
        .list();
  }

  public List<SegmentIdWithShardSpec> retrievePendingSegmentIdsWithExactInterval(
      final String dataSource,
      final String sequenceName,
      final Interval interval
  )
  {
    final String sql = StringUtils.format(
        "SELECT payload FROM %s WHERE "
        + "dataSource = :dataSource AND "
        + "sequence_name = :sequence_name AND "
        + "start = :start AND "
        + "%2$send%2$s = :end",
        dbTables.getPendingSegmentsTable(),
        connector.getQuoteString()
    );
    return handle
        .createQuery(sql)
        .bind("dataSource", dataSource)
        .bind("sequence_name", sequenceName)
        .bind("start", interval.getStart().toString())
        .bind("end", interval.getEnd().toString())
        .map(
            (index, r, ctx) -> JacksonUtils.readValue(
                jsonMapper,
                r.getBytes("payload"),
                SegmentIdWithShardSpec.class
            )
        )
        .list();
  }

  public List<PendingSegmentRecord> retrievePendingSegmentsWithExactInterval(
      final String dataSource,
      final Interval interval
  )
  {
    final String sql = StringUtils.format(
        "SELECT payload, sequence_name, sequence_prev_id,"
        + " task_allocator_id, upgraded_from_segment_id, created_date"
        + " FROM %1$s WHERE dataSource = :dataSource"
        + " AND start = :start AND %2$send%2$s = :end",
        dbTables.getPendingSegmentsTable(), connector.getQuoteString()
    );
    return handle
        .createQuery(sql)
        .bind("dataSource", dataSource)
        .bind("start", interval.getStart().toString())
        .bind("end", interval.getEnd().toString())
        .map((index, r, ctx) -> PendingSegmentRecord.fromResultSet(r, jsonMapper))
        .list();
  }

  /**
   * Fetches all the pending segments, whose interval overlaps with the given
   * search interval, from the metadata store.
   */
  public List<PendingSegmentRecord> retrievePendingSegmentsOverlappingInterval(
      final String dataSource,
      final Interval interval
  )
  {
    final boolean compareIntervalEndpointsAsStrings = Intervals.canCompareEndpointsAsStrings(interval);

    String sql = StringUtils.format(
        "SELECT payload, sequence_name, sequence_prev_id,"
        + " task_allocator_id, upgraded_from_segment_id, created_date"
        + " FROM %1$s WHERE dataSource = :dataSource",
        dbTables.getPendingSegmentsTable()
    );
    if (compareIntervalEndpointsAsStrings) {
      sql += " AND start < :end"
             + StringUtils.format(" AND %1$send%1$s > :start", connector.getQuoteString());
    }

    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .bind("dataSource", dataSource);
    if (compareIntervalEndpointsAsStrings) {
      query = query.bind("start", interval.getStart().toString())
                   .bind("end", interval.getEnd().toString());
    }

    final ResultIterator<PendingSegmentRecord> pendingSegmentIterator =
        query.map((index, r, ctx) -> PendingSegmentRecord.fromResultSet(r, jsonMapper))
             .iterator();
    final ImmutableList.Builder<PendingSegmentRecord> pendingSegments = ImmutableList.builder();
    while (pendingSegmentIterator.hasNext()) {
      final PendingSegmentRecord pendingSegment = pendingSegmentIterator.next();
      if (compareIntervalEndpointsAsStrings || pendingSegment.getId().getInterval().overlaps(interval)) {
        pendingSegments.add(pendingSegment);
      }
    }
    pendingSegmentIterator.close();
    return pendingSegments.build();
  }

  public List<PendingSegmentRecord> retrievePendingSegmentsForTaskAllocatorId(
      final String dataSource,
      final String taskAllocatorId
  )
  {
    final String sql = StringUtils.format(
        "SELECT payload, sequence_name, sequence_prev_id,"
        + " task_allocator_id, upgraded_from_segment_id, created_date"
        + " FROM %1$s WHERE dataSource = :dataSource"
        + " AND task_allocator_id = :task_allocator_id",
        dbTables.getPendingSegmentsTable()
    );

    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .bind("dataSource", dataSource)
                                             .bind("task_allocator_id", taskAllocatorId);

    final ResultIterator<PendingSegmentRecord> pendingSegmentRecords =
        query.map((index, r, ctx) -> PendingSegmentRecord.fromResultSet(r, jsonMapper))
             .iterator();

    final List<PendingSegmentRecord> pendingSegments = new ArrayList<>();
    while (pendingSegmentRecords.hasNext()) {
      pendingSegments.add(pendingSegmentRecords.next());
    }

    pendingSegmentRecords.close();

    return pendingSegments;
  }

  /**
   * Get the condition for the interval and match mode.
   * @param intervals - intervals to fetch the segments for
   * @param matchMode - Interval match mode - overlaps or contains
   * @param quoteString - the connector-specific quote string
   */
  public static String getConditionForIntervalsAndMatchMode(
      final Collection<Interval> intervals,
      final IntervalMode matchMode,
      final String quoteString
  )
  {
    if (intervals.isEmpty()) {
      return "";
    }

    final StringBuilder sb = new StringBuilder();

    sb.append(" AND (");
    for (int i = 0; i < intervals.size(); i++) {
      sb.append(
          matchMode.makeSqlCondition(
              quoteString,
              StringUtils.format(":start%d", i),
              StringUtils.format(":end%d", i)
          )
      );

      // Add a special check for a segment which have one end at eternity and the other at some finite value. Since
      // we are using string comparison, a segment with this start or end will not be returned otherwise.
      if (matchMode.equals(IntervalMode.OVERLAPS)) {
        sb.append(StringUtils.format(
            " OR (start = '%s' AND \"end\" != '%s' AND \"end\" > :start%d)",
            Intervals.ETERNITY.getStart(), Intervals.ETERNITY.getEnd(), i
        ));
        sb.append(StringUtils.format(
            " OR (start != '%s' AND \"end\" = '%s' AND start < :end%d)",
            Intervals.ETERNITY.getStart(), Intervals.ETERNITY.getEnd(), i
        ));
      }

      if (i != intervals.size() - 1) {
        sb.append(" OR ");
      }
    }

    // Add a special check for a single segment with eternity. Since we are using string comparison, a segment with
    // this start and end will not be returned otherwise.
    // Known Issue: https://github.com/apache/druid/issues/12860
    if (matchMode.equals(IntervalMode.OVERLAPS)) {
      sb.append(StringUtils.format(
          " OR (start = '%s' AND \"end\" = '%s')", Intervals.ETERNITY.getStart(), Intervals.ETERNITY.getEnd()
      ));
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Bind the supplied {@code intervals} to {@code query}.
   * @see #getConditionForIntervalsAndMatchMode(Collection, IntervalMode, String)
   */
  public static void bindIntervalsToQuery(final Query<Map<String, Object>> query, final Collection<Interval> intervals)
  {
    if (intervals.isEmpty()) {
      return;
    }

    final Iterator<Interval> iterator = intervals.iterator();
    for (int i = 0; iterator.hasNext(); i++) {
      Interval interval = iterator.next();
      query.bind(StringUtils.format("start%d", i), interval.getStart().toString())
           .bind(StringUtils.format("end%d", i), interval.getEnd().toString());
    }
  }

  private CloseableIterator<DataSegment> retrieveSegments(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      final IntervalMode matchMode,
      final boolean used,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    if (versions != null && versions.isEmpty()) {
      return CloseableIterators.withEmptyBaggage(Collections.emptyIterator());
    }

    if (intervals.isEmpty() || intervals.size() <= MAX_INTERVALS_PER_BATCH) {
      return CloseableIterators.withEmptyBaggage(
          retrieveSegmentsInIntervalsBatch(dataSource, intervals, versions, matchMode, used, limit, lastSegmentId, sortOrder, maxUsedStatusLastUpdatedTime)
      );
    } else {
      final List<List<Interval>> intervalsLists = Lists.partition(new ArrayList<>(intervals), MAX_INTERVALS_PER_BATCH);
      final List<Iterator<DataSegment>> resultingIterators = new ArrayList<>();
      Integer limitPerBatch = limit;

      for (final List<Interval> intervalList : intervalsLists) {
        final UnmodifiableIterator<DataSegment> iterator = retrieveSegmentsInIntervalsBatch(
            dataSource,
            intervalList,
            versions,
            matchMode,
            used,
            limitPerBatch,
            lastSegmentId,
            sortOrder,
            maxUsedStatusLastUpdatedTime
        );
        if (limitPerBatch != null) {
          // If limit is provided, we need to shrink the limit for subsequent batches or circuit break if
          // we have reached what was requested for.
          final List<DataSegment> dataSegments = ImmutableList.copyOf(iterator);
          resultingIterators.add(dataSegments.iterator());
          if (dataSegments.size() >= limitPerBatch) {
            break;
          }
          limitPerBatch -= dataSegments.size();
        } else {
          resultingIterators.add(iterator);
        }
      }
      return CloseableIterators.withEmptyBaggage(Iterators.concat(resultingIterators.iterator()));
    }
  }

  private CloseableIterator<DataSegmentPlus> retrieveSegmentsPlus(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      final IntervalMode matchMode,
      final boolean used,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    if (intervals.isEmpty() || intervals.size() <= MAX_INTERVALS_PER_BATCH) {
      return CloseableIterators.withEmptyBaggage(
          retrieveSegmentsPlusInIntervalsBatch(dataSource, intervals, versions, matchMode, used, limit, lastSegmentId, sortOrder, maxUsedStatusLastUpdatedTime)
      );
    } else {
      final List<List<Interval>> intervalsLists = Lists.partition(new ArrayList<>(intervals), MAX_INTERVALS_PER_BATCH);
      final List<Iterator<DataSegmentPlus>> resultingIterators = new ArrayList<>();
      Integer limitPerBatch = limit;

      for (final List<Interval> intervalList : intervalsLists) {
        final UnmodifiableIterator<DataSegmentPlus> iterator = retrieveSegmentsPlusInIntervalsBatch(
            dataSource,
            intervalList,
            versions,
            matchMode,
            used,
            limitPerBatch,
            lastSegmentId,
            sortOrder,
            maxUsedStatusLastUpdatedTime
        );
        if (limitPerBatch != null) {
          // If limit is provided, we need to shrink the limit for subsequent batches or circuit break if
          // we have reached what was requested for.
          final List<DataSegmentPlus> dataSegments = ImmutableList.copyOf(iterator);
          resultingIterators.add(dataSegments.iterator());
          if (dataSegments.size() >= limitPerBatch) {
            break;
          }
          limitPerBatch -= dataSegments.size();
        } else {
          resultingIterators.add(iterator);
        }
      }
      return CloseableIterators.withEmptyBaggage(Iterators.concat(resultingIterators.iterator()));
    }
  }

  private UnmodifiableIterator<DataSegment> retrieveSegmentsInIntervalsBatch(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      final IntervalMode matchMode,
      final boolean used,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    final Query<Map<String, Object>> sql = buildSegmentsTableQuery(
        dataSource,
        intervals,
        versions,
        matchMode,
        used,
        limit,
        lastSegmentId,
        sortOrder,
        maxUsedStatusLastUpdatedTime,
        false
    );

    final ResultIterator<DataSegment> resultIterator = getDataSegmentResultIterator(sql);

    return filterDataSegmentIteratorByInterval(resultIterator, intervals, matchMode);
  }

  private UnmodifiableIterator<DataSegmentPlus> retrieveSegmentsPlusInIntervalsBatch(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      final IntervalMode matchMode,
      final boolean used,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime
  )
  {

    final Query<Map<String, Object>> sql = buildSegmentsTableQuery(
        dataSource,
        intervals,
        versions,
        matchMode,
        used,
        limit,
        lastSegmentId,
        sortOrder,
        maxUsedStatusLastUpdatedTime,
        true
    );

    final ResultIterator<DataSegmentPlus> resultIterator = getDataSegmentPlusResultIterator(sql, used);

    return filterDataSegmentPlusIteratorByInterval(resultIterator, intervals, matchMode);
  }

  private Query<Map<String, Object>> buildSegmentsTableQuery(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      final IntervalMode matchMode,
      final boolean used,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime,
      final boolean includeExtraInfo
  )
  {
    // Check if the intervals all support comparing as strings. If so, bake them into the SQL.
    final boolean compareAsString = intervals.stream().allMatch(Intervals::canCompareEndpointsAsStrings);
    final StringBuilder sb = new StringBuilder();
    if (includeExtraInfo) {
      sb.append("SELECT id, payload, created_date, used_status_last_updated FROM %s WHERE used = :used AND dataSource = :dataSource");
    } else {
      sb.append("SELECT id, payload FROM %s WHERE used = :used AND dataSource = :dataSource");
    }

    if (compareAsString) {
      sb.append(getConditionForIntervalsAndMatchMode(intervals, matchMode, connector.getQuoteString()));
    }

    if (versions != null) {
      sb.append(getParameterizedInConditionForColumn("version", versions));
    }

    // Add the used_status_last_updated time filter only for unused segments when maxUsedStatusLastUpdatedTime is non-null.
    final boolean addMaxUsedLastUpdatedTimeFilter = !used && maxUsedStatusLastUpdatedTime != null;
    if (addMaxUsedLastUpdatedTimeFilter) {
      sb.append(" AND (used_status_last_updated IS NOT NULL AND used_status_last_updated <= :used_status_last_updated)");
    }

    if (lastSegmentId != null) {
      sb.append(
          StringUtils.format(
              " AND id %s :id",
              (sortOrder == null || sortOrder == SortOrder.ASC)
                  ? ">"
                  : "<"
          )
      );
    }

    if (sortOrder != null) {
      sb.append(StringUtils.format(" ORDER BY id %2$s, start %2$s, %1$send%1$s %2$s",
          connector.getQuoteString(),
          sortOrder.toString()));
    }
    final Query<Map<String, Object>> sql = handle
        .createQuery(StringUtils.format(
            sb.toString(),
            dbTables.getSegmentsTable()
        ))
        .setFetchSize(connector.getStreamingFetchSize())
        .bind("used", used)
        .bind("dataSource", dataSource);

    if (addMaxUsedLastUpdatedTimeFilter) {
      sql.bind("used_status_last_updated", maxUsedStatusLastUpdatedTime.toString());
    }

    if (lastSegmentId != null) {
      sql.bind("id", lastSegmentId);
    }

    if (limit != null) {
      sql.setMaxRows(limit);
    }

    if (compareAsString) {
      bindIntervalsToQuery(sql, intervals);
    }

    if (versions != null) {
      bindColumnValuesToQueryWithInCondition("version", versions, sql);
    }

    return sql;
  }

  @Nullable
  private Interval mapToInterval(ResultSet resultSet, String dataSource)
  {
    try {
      return new Interval(
          DateTimes.of(resultSet.getString("start")),
          DateTimes.of(resultSet.getString("end"))
      );
    }
    catch (Throwable t) {
      log.error(t, "Could not read an interval of datasource[%s]", dataSource);
      return null;
    }
  }

  /**
   * Tries to parse the fields of the result set into a {@link SegmentSchemaRecord}.
   *
   * @return null if an error occurred while parsing the result
   */
  @Nullable
  private SegmentSchemaRecord mapToSchemaRecord(ResultSet resultSet)
  {
    String fingerprint = null;
    try {
      fingerprint = resultSet.getString("fingerprint");
      return new SegmentSchemaRecord(
          fingerprint,
          jsonMapper.readValue(resultSet.getBytes("payload"), SchemaPayload.class)
      );
    }
    catch (Throwable t) {
      log.error(t, "Could not read segment schema with fingerprint[%s]", fingerprint);
      return null;
    }
  }

  private ResultIterator<DataSegment> getDataSegmentResultIterator(Query<Map<String, Object>> sql)
  {
    return sql.map((index, r, ctx) -> JacksonUtils.readValue(jsonMapper, r.getBytes(2), DataSegment.class))
              .iterator();
  }

  private ResultIterator<DataSegmentPlus> getDataSegmentPlusResultIterator(
      Query<Map<String, Object>> sql,
      boolean used
  )
  {
    return sql.map((index, r, ctx) -> {
      final String segmentId = r.getString(1);
      try {
        return new DataSegmentPlus(
            JacksonUtils.readValue(jsonMapper, r.getBytes(2), DataSegment.class),
            DateTimes.of(r.getString(3)),
            nullAndEmptySafeDate(r.getString(4)),
            used,
            null,
            null,
            null
        );
      }
      catch (Throwable t) {
        log.error(t, "Could not read segment with ID[%s]", segmentId);
        return null;
      }
    }).iterator();
  }

  @Nullable
  private DataSegment mapToSegment(ResultSet resultSet)
  {
    String segmentId = "";
    try {
      segmentId = resultSet.getString("id");
      return JacksonUtils.readValue(jsonMapper, resultSet.getBytes("payload"), DataSegment.class);
    }
    catch (Throwable t) {
      log.error(t, "Could not read segment with ID[%s]", segmentId);
      return null;
    }
  }

  private UnmodifiableIterator<DataSegment> filterDataSegmentIteratorByInterval(
      ResultIterator<DataSegment> resultIterator,
      final Collection<Interval> intervals,
      final IntervalMode matchMode
  )
  {
    return Iterators.filter(
        resultIterator,
        dataSegment -> {
          if (intervals.isEmpty()) {
            return true;
          } else {
            // Must re-check that the interval matches, even if comparing as string, because the *segment interval*
            // might not be string-comparable. (Consider a query interval like "2000-01-01/3000-01-01" and a
            // segment interval like "20010/20011".)
            for (Interval interval : intervals) {
              if (matchMode.apply(interval, dataSegment.getInterval())) {
                return true;
              }
            }

            return false;
          }
        }
    );
  }

  private UnmodifiableIterator<DataSegmentPlus> filterDataSegmentPlusIteratorByInterval(
      ResultIterator<DataSegmentPlus> resultIterator,
      final Collection<Interval> intervals,
      final IntervalMode matchMode
  )
  {
    return Iterators.filter(
        resultIterator,
        dataSegment -> {
          if (dataSegment == null) {
            return false;
          } else if (intervals.isEmpty()) {
            return true;
          } else {
            // Must re-check that the interval matches, even if comparing as string, because the *segment interval*
            // might not be string-comparable. (Consider a query interval like "2000-01-01/3000-01-01" and a
            // segment interval like "20010/20011".)
            for (Interval interval : intervals) {
              if (matchMode.apply(interval, dataSegment.getDataSegment().getInterval())) {
                return true;
              }
            }

            return false;
          }
        }
    );
  }

  private static int computeNumChangedSegments(List<String> segmentIds, int[] segmentChanges)
  {
    int numChangedSegments = 0;
    for (int i = 0; i < segmentChanges.length; i++) {
      int numUpdatedRows = segmentChanges[i];
      if (numUpdatedRows < 0) {
        log.error(
            "ASSERTION_ERROR: Negative number of rows updated for segment id [%s]: %d",
            segmentIds.get(i),
            numUpdatedRows
        );
      } else if (numUpdatedRows > 1) {
        log.error(
            "More than one row updated for segment id [%s]: %d, "
            + "there may be more than one row for the segment id in the database",
            segmentIds.get(i),
            numUpdatedRows
        );
      }
      if (numUpdatedRows > 0) {
        numChangedSegments += 1;
      }
    }
    return numChangedSegments;
  }

  /**
   * @return a parameterized {@code IN} clause for the specified {@code columnName}. The column values need to be bound
   * to a query by calling {@link #bindColumnValuesToQueryWithInCondition(String, List, SQLStatement)}.
   *
   * @implNote JDBI 3.x has better support for binding {@code IN} clauses directly.
   */
  public static String getParameterizedInConditionForColumn(final String columnName, final List<String> values)
  {
    if (values == null) {
      return "";
    }

    final StringBuilder sb = new StringBuilder();

    sb.append(StringUtils.format(" AND %s IN (", columnName));
    for (int i = 0; i < values.size(); i++) {
      sb.append(StringUtils.format(":%s%d", columnName, i));
      if (i != values.size() - 1) {
        sb.append(",");
      }
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Binds the provided list of {@code values} to the specified {@code columnName} in the given SQL {@code query} that
   * contains an {@code IN} clause.
   *
   * @see #getParameterizedInConditionForColumn(String, List)
   */
  public static void bindColumnValuesToQueryWithInCondition(
      final String columnName,
      final List<String> values,
      final SQLStatement<?> query
  )
  {
    if (values == null) {
      return;
    }

    for (int i = 0; i < values.size(); i++) {
      query.bind(StringUtils.format("%s%d", columnName, i), values.get(i));
    }
  }

  enum IntervalMode
  {
    CONTAINS {
      @Override
      public String makeSqlCondition(String quoteString, String startPlaceholder, String endPlaceholder)
      {
        // 2 range conditions are used on different columns, but not all SQL databases properly optimize it.
        // Some databases can only use an index on one of the columns. An additional condition provides
        // explicit knowledge that 'start' cannot be greater than 'end'.
        return StringUtils.format(
            "(start >= %2$s and start <= %3$s and %1$send%1$s <= %3$s)",
            quoteString,
            startPlaceholder,
            endPlaceholder
        );
      }

      @Override
      public boolean apply(Interval a, Interval b)
      {
        return a.contains(b);
      }
    },
    OVERLAPS {
      @Override
      public String makeSqlCondition(String quoteString, String startPlaceholder, String endPlaceholder)
      {
        return StringUtils.format(
            "(start < %3$s AND %1$send%1$s > %2$s)",
            quoteString,
            startPlaceholder,
            endPlaceholder
        );
      }

      @Override
      public boolean apply(Interval a, Interval b)
      {
        return a.overlaps(b);
      }
    };

    public abstract String makeSqlCondition(String quoteString, String startPlaceholder, String endPlaceholder);

    public abstract boolean apply(Interval a, Interval b);
  }
}
