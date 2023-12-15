package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@ManageLifecycle
public class SegmentSchemaBackfillQueue
{
  private static final EmittingLogger log = new EmittingLogger(SegmentSchemaBackfillQueue.class);
  private static final int MAX_BATCH_SIZE = 500;
  private static final int DB_ACTION_PARTITION_SIZE = 100;
  private final long maxWaitTimeMillis;
  private final ScheduledExecutorService executor;
  private final MetadataStorageTablesConfig dbTables;
  private final SQLMetadataConnector connector;
  private final ObjectMapper jsonMapper;
  private final BlockingDeque<SegmentSchema> segmentSchemaQueue = new LinkedBlockingDeque<>();

  public SegmentSchemaBackfillQueue(
      MetadataStorageTablesConfig dbTables,
      SQLMetadataConnector connector,
      ObjectMapper jsonMapper,
      ScheduledExecutorFactory scheduledExecutorFactory
  )
  {
    this.dbTables = dbTables;
    this.connector = connector;
    this.jsonMapper = jsonMapper;
    this.executor = scheduledExecutorFactory.create(1, "SegmentSchemaBackfillQueue-%s");
  }

  @LifecycleStart
  public void start()
  {
    if (isEnabled()) {
      scheduleQueuePoll(maxWaitTimeMillis);
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (isEnabled()) {
      executor.shutdownNow();
    }
  }

  public void add(SegmentSchema segmentSchema)
  {
    segmentSchemaQueue.add(segmentSchema);
  }

  public boolean isEnabled()
  {
    return executor != null && !executor.isShutdown();
  }

  private void scheduleQueuePoll(long delay)
  {
    executor.schedule(this::processBatchesDue, delay, TimeUnit.MILLISECONDS);
  }

  private void processBatchesDue()
  {
    List<SegmentSchema> polled = null; // process 500 segments

    // first filter out created schema

    connector.retryTransaction((handle, status) -> persistSchema(handle, polled), 1, 1);
    // same transaction, otherwise the cleanup job will remove persisted segments
    connector.retryTransaction((handle, status) -> updateSegments(handle, polled), 1, 1);
  }

  private int persistSchema(Handle handle, List<SegmentSchema> segmentSchemas)
  {
    try {
      // find out all the unique schema insert them and get their id
      // go and update the segment table with the schema id

      Map<String, SchemaPayload> schemaPayloadMap = new HashMap<>();

      for (SegmentSchema segmentSchema : segmentSchemas) {
        schemaPayloadMap.put(segmentSchema.getFingerprint(), segmentSchema.getSchemaPayload());
      }

      // Filter already existing schema
      Set<String> existingSchemas = schemaExistBatch(handle, schemaPayloadMap.keySet());
      log.info("Found already existing schema in the DB: %s", existingSchemas);
      schemaPayloadMap.keySet().retainAll(existingSchemas);

      final List<List<String>> partitionedFingerprints = Lists.partition(
          new ArrayList<>(schemaPayloadMap.keySet()),
          DB_ACTION_PARTITION_SIZE
      );

      String insertSql = StringUtils.format(
          "INSERT INTO %1$s (fingerprint, created_date, payload) "
          + "VALUES (:fingerprint, :created_date, :payload)",
          dbTables.getSegmentSchemaTable()
      );

      // insert schemas
      PreparedBatch schemaInsertBatch = handle.prepareBatch(insertSql);
      for (List<String> partition : partitionedFingerprints) {
        for (String fingerprint : partition) {
          final String now = DateTimes.nowUtc().toString();
          schemaInsertBatch.add()
                       .bind("fingerprint", fingerprint)
                       .bind("created_date", now)
                       .bind("payload", jsonMapper.writeValueAsBytes(schemaPayloadMap.get(fingerprint)));
        }
        final int[] affectedRows = schemaInsertBatch.execute();
        final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);
        if (succeeded) {
          log.info("Published schemas to DB: %s", partition);
        } else {
          final List<String> failedToPublish = IntStream.range(0, partition.size())
                                                             .filter(i -> affectedRows[i] != 1)
                                                             .mapToObj(partition::get)
                                                             .collect(Collectors.toList());
          throw new ISE("Failed to publish schemas to DB: %s", failedToPublish);
        }
      }
    }
    catch (Exception e) {
      log.error("Exception inserting schemas to DB: %s", segmentSchemas);
      throw e;
    }
  }

  private int updateSegments(Handle handle, List<SegmentSchema> segmentSchemas)
  {
    Set<String> updatedSegments = segmentUpdatedBatch(handle, segmentSchemas.stream().map(SegmentSchema::getSegmentId).collect(
        Collectors.toSet()));
    List<SegmentSchema> segmentsToUpdate = segmentSchemas.stream().filter(v -> updatedSegments.contains(v.getSegmentId())).collect(Collectors.toList());

    // fetch schemaId
    Map<String, Integer> fingerprintSchemaIdMap =
        schemaIdFetchBatch(handle, segmentsToUpdate.stream().map(SegmentSchema::getFingerprint).collect(Collectors.toSet()));

    // update schemaId and numRows in segments table
    String updateSql = "";
    PreparedBatch segmentUpdateBatch = handle.prepareBatch(updateSql);

    List<List<SegmentSchema>> partitionedSegmentIds = Lists.partition(
        segmentsToUpdate,
        DB_ACTION_PARTITION_SIZE
    );

    for (List<SegmentSchema> partition : partitionedSegmentIds) {
      for (SegmentSchema segmentSchema : segmentsToUpdate) {
        String fingerprint = segmentSchema.getFingerprint();
        if (!fingerprintSchemaIdMap.containsKey(fingerprint)) {
          // this should not happen
          continue;
        }

        segmentUpdateBatch.add()
                          .bind("id", segmentSchema.getSegmentId())
                          .bind("schema_id", fingerprintSchemaIdMap.get(fingerprint))
                          .bind("num_rows", segmentSchema.getNumRows());
      }

      final int[] affectedRows = segmentUpdateBatch.execute();
      final boolean succeeded = Arrays.stream(affectedRows).allMatch(eachAffectedRows -> eachAffectedRows == 1);

      if (succeeded) {
        log.info("Updated segments with num DB: %s", partition);
      } else {
        final List<String> failedToPublish = IntStream.range(0, partition.size())
                                                      .filter(i -> affectedRows[i] != 1)
                                                      .mapToObj(partition::get)
                                                      .collect(Collectors.toList());
        throw new ISE("Failed to publish schemas to DB: %s", failedToPublish);
      }
    }
  }

  private Set<String> schemaExistBatch(Handle handle, Set<String> fingerprintsToInsert)
  {
    List<List<String>> partitionedFingerprints = Lists.partition(
        new ArrayList<>(fingerprintsToInsert),
        DB_ACTION_PARTITION_SIZE
    );

    Set<String> existingSchemas = new HashSet<>();
    for (List<String> fingerprintList : partitionedFingerprints) {
      List<String> existIds =
          handle.createQuery(
                    StringUtils.format(
                        "SELECT fingerprint FROM %s WHERE fingerprint in (%s)",
                        dbTables.getSegmentSchemaTable(), fingerprintList
                    )
                )
                .mapTo(String.class)
                .list();
      existingSchemas.addAll(existIds);
    }
    return existingSchemas;
  }

  private Map<String, Integer> schemaIdFetchBatch(Handle handle, Set<String> fingerprintsToQuery)
  {
    List<List<String>> partitionedFingerprints = Lists.partition(
        new ArrayList<>(fingerprintsToQuery),
        DB_ACTION_PARTITION_SIZE
    );

    Map<String, Integer> fingerprintIdMap = new HashMap<>();
    for (List<String> fingerprintList : partitionedFingerprints) {
      Map<String, Integer> partitionFingerprintIdMap =
          handle.createQuery(
                    StringUtils.format(
                        "SELECT fingerprint, id FROM %s WHERE fingerprint in (%s)",
                        dbTables.getSegmentSchemaTable(), fingerprintList
                    )
                )
                .map((index, r, ctx) -> Pair.of(r.getString("fingerprint"), r.getInt("id")))
                .fold(
                    new HashMap<>(), (accumulator, rs, control, ctx) -> {
                      accumulator.put(rs.lhs, rs.rhs);
                      return accumulator;
                    }
                );
      fingerprintIdMap.putAll(partitionFingerprintIdMap);
    }
    return fingerprintIdMap;
  }

  private Set<String> segmentUpdatedBatch(Handle handle, Set<String> segmentIds)
  {
    List<List<String>> partitionedSegmentIds = Lists.partition(
        new ArrayList<>(segmentIds),
        DB_ACTION_PARTITION_SIZE
    );

    Set<String> updated = new HashSet<>();
    for (List<String> partition : partitionedSegmentIds) {
      List<String> updatedSegmentIds =
          handle.createQuery(
                    StringUtils.format(
                        "SELECT id from %s where id in (%s) and schema_id != null",
                        dbTables.getSegmentsTable(),
                        partition
                    ))
                .mapTo(String.class)
                .list();

      updated.addAll(updatedSegmentIds);
    }
    return updated;
  }
}
