package org.apache.druid.segment.metadata;

import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.SchemaPayload;
import org.apache.druid.segment.column.SegmentSchemaMetadata;
import org.apache.druid.segment.metadata.SchemaPersistHelper.SegmentSchemaMetadataPlus;
import org.apache.druid.timeline.SegmentId;
import org.skife.jdbi.v2.TransactionCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class SegmentSchemaBackfillQueue
{
  private static final EmittingLogger log = new EmittingLogger(SegmentSchemaBackfillQueue.class);
  private static final int MAX_BATCH_SIZE = 500;
  private final BlockingDeque<SegmentSchemaMetadataPlus> queue = new LinkedBlockingDeque<>();
  private final long executionPeriod;
  private final ScheduledExecutorService executor;

  private final SQLMetadataConnector connector;
  private final SegmentSchemaCache cache;
  private final SchemaPersistHelper schemaPersistHelper;

  public SegmentSchemaBackfillQueue(
      SchemaPersistHelper schemaPersistHelper,
      SQLMetadataConnector connector,
      ScheduledExecutorFactory scheduledExecutorFactory,
      SegmentSchemaCache cache
  )
  {
    this.schemaPersistHelper = schemaPersistHelper;
    this.connector = connector;
    this.executor = scheduledExecutorFactory.create(1, "SegmentSchemaBackfillQueue-%s");
    this.cache = cache;
    this.executionPeriod = TimeUnit.MINUTES.toMillis(1);
  }

  @LifecycleStart
  public void start()
  {
    if (isEnabled()) {
      scheduleQueuePoll(executionPeriod);
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (isEnabled()) {
      executor.shutdownNow();
    }
  }

  public void add(SegmentId segmentId, RowSignature rowSignature, long numRows, Map<String, AggregatorFactory> aggregators)
  {
    SchemaPayload schemaPayload = new SchemaPayload(rowSignature, aggregators);
    SegmentSchemaMetadata schemaMetadata = new SegmentSchemaMetadata(schemaPayload, numRows);
    queue.add(new SegmentSchemaMetadataPlus(segmentId.toString(), schemaMetadata, SchemaFingerprintGenerator.generateId(schemaMetadata.getSchemaPayload())));
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
    int itemsToProcess = Math.min(MAX_BATCH_SIZE, queue.size());

    List<SegmentSchemaMetadataPlus> polled = new ArrayList<>();

    for (int i = 0; i < itemsToProcess; i++) {
      SegmentSchemaMetadataPlus item = queue.poll();
      if (item != null) {
        polled.add(item);
      }
    }

    // finally add the published schema to different map in the schema cache
    try {
      connector.retryTransaction((TransactionCallback<Void>) (handle, status) -> {
        schemaPersistHelper.persistSchema(handle, polled);
        schemaPersistHelper.updateSegments(handle, polled);
        return null;
      }, 1, 3);
    } catch () {

    } finally {

    }
  }
}
