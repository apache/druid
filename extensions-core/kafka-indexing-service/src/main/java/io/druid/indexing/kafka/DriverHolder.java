package io.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.FiniteAppenderatorDriver;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.timeline.DataSegment;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;

public class DriverHolder implements Closeable
{
  public enum DriverStatus
  {
    NOT_OPEN,
    OPEN,
    COMPLETE,
    PERSISTING,
    PERSISTED,
    PUBLISHING,
    PUBLISHED,
    CLOSED
  }

  volatile DriverStatus driverStatus = DriverStatus.NOT_OPEN;

  private static final EmittingLogger log = new EmittingLogger(DriverHolder.class);

  private final FiniteAppenderatorDriver driver;
  private final Set<Integer> assignment;
  private Supplier<Committer> committerSupplier;

  private final Map<Integer, Long> nextPartitionOffsets = Maps.newHashMap();

  /*********** Driver Metadata **********/
  private final String topic;
  private final Map<Integer, Long> startOffsets;
  private final Map<Integer, Long> endOffsets;
  private final String sequenceName;
  // this uniquely identifies the persist dir for this driver
  private final int driverIndex;
  // whether this is the last driver for the task
  private volatile boolean last;
  // whether the end offsets for this driver is set
  private volatile boolean checkPointed;
  private boolean maxRowsPerSegmentLimitReached;

  /***************************************/

  private DriverHolder(
      String topic,
      FiniteAppenderatorDriver driver,
      Map<Integer, Long> startOffsets,
      Map<Integer, Long> endOffsets,
      int driverIndex,
      String sequenceName,
      boolean last,
      boolean checkPointed,
      boolean maxRowsPerSegmentLimitReached
  )
  {
    this.topic = topic;
    this.driver = driver;
    this.startOffsets = startOffsets;
    // endOffsets will change when a check point is set so make a local copy of it
    this.endOffsets = Maps.newHashMap(endOffsets);
    this.driverIndex = driverIndex;
    this.sequenceName = sequenceName;
    this.assignment = Sets.newHashSet(endOffsets.keySet());
    this.last = last;
    this.checkPointed = checkPointed;
    this.maxRowsPerSegmentLimitReached = maxRowsPerSegmentLimitReached;
  }

  Map<Integer, Long> startJob(final ObjectMapper mapper)
  {
    final Object restored = driver.startJob();
    Preconditions.checkState(
        driverStatus == DriverStatus.NOT_OPEN,
        "WTH?! Cannot change driver status to [%s] from [%s]",
        DriverStatus.OPEN,
        driverStatus
    );
    driverStatus = DriverStatus.OPEN;
    if (restored == null) {
      nextPartitionOffsets.putAll(startOffsets);
    } else {
      Map<String, Object> restoredMetadataMap = (Map) restored;
      final KafkaPartitions restoredNextPartitions = mapper.convertValue(
          restoredMetadataMap.get(KafkaIndexTask.METADATA_NEXT_PARTITIONS),
          KafkaPartitions.class
      );
      nextPartitionOffsets.putAll(restoredNextPartitions.getPartitionOffsetMap());

      // Sanity checks.
      if (!restoredNextPartitions.getTopic().equals(topic)) {
        throw new ISE(
            "WTF?! Restored topic[%s] but expected topic[%s]",
            restoredNextPartitions.getTopic(),
            topic
        );
      }

      if (!nextPartitionOffsets.keySet().equals(startOffsets.keySet())) {
        throw new ISE(
            "WTF?! Restored partitions[%s] but expected partitions[%s]",
            nextPartitionOffsets.keySet(),
            startOffsets.keySet()
        );
      }
    }
    committerSupplier = new Supplier<Committer>()
    {
      @Override
      public Committer get()
      {
        final Map<Integer, Long> snapshot = ImmutableMap.copyOf(nextPartitionOffsets);

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return ImmutableMap.of(
                KafkaIndexTask.METADATA_NEXT_PARTITIONS, new KafkaPartitions(
                    topic,
                    snapshot
                )
            );
          }

          @Override
          public void run()
          {
            // Do nothing
          }
        };
      }
    };

    // remove partition for which offsets have been consumed fully
    for (Map.Entry<Integer, Long> partitionOffset : nextPartitionOffsets.entrySet()) {
      if (endOffsets.get(partitionOffset.getKey()).equals(partitionOffset.getValue())) {
        logInfo("[Start-Job] read partition [%d]", partitionOffset.getKey());
        assignment.remove(partitionOffset.getKey());
      }
    }
    if (assignment.isEmpty()) {
      // finish the driver
      logInfo("[Start-Job] read all partitions");
      Preconditions.checkState(
          driverStatus == DriverStatus.OPEN,
          "WTH?! Cannot change driver status to [%s] from [%s]",
          DriverStatus.COMPLETE,
          driverStatus
      );
      driverStatus = DriverStatus.COMPLETE;
    }

    return nextPartitionOffsets;
  }

  boolean canHandle(ConsumerRecord<byte[], byte[]> record)
  {
    return driverStatus == DriverStatus.OPEN
           && endOffsets.get(record.partition()) != null
           && record.offset() >= startOffsets.get(record.partition())
           && record.offset() < endOffsets.get(record.partition());
  }

  public SegmentIdentifier add(InputRow row) throws IOException
  {
    Preconditions.checkState(driverStatus == DriverStatus.OPEN, "Cannot add to driver which is not open!");

    SegmentIdentifier identifier = driver.add(row, sequenceName, committerSupplier, false);

    if (identifier == null) {
      // Failure to allocate segment puts determinism at risk, bail out to be safe.
      // May want configurable behavior here at some point.
      // If we allow continuing, then consider blacklisting the interval for a while to avoid constant checks.
      throw new ISE("Could not allocate segment for row with timestamp[%s]", row.getTimestamp());
    }

    // remember to call incrementNextOffsets before using any other method
    // which relies on updated value of nextPartitionOffsets
    if (driver.numRowsInSegment(identifier) >= driver.getMaxRowPerSegment()) {
      maxRowsPerSegmentLimitReached = true;
    }
    return identifier;
  }

  void incrementNextOffsets(ConsumerRecord<byte[], byte[]> record)
  {
    // we do not automatically increment next offset with add call
    // as in case record cannot be parsed add method will not be called
    // however we still need to increment next offsets irrespective

    // update local nextOffset to be used by committer at some point
    nextPartitionOffsets.put(record.partition(), nextPartitionOffsets.get(record.partition()) + 1);

    if (nextPartitionOffsets.get(record.partition()).equals(endOffsets.get(record.partition()))) {
      logInfo("[Increment-Offsets] read partition [%d]", record.partition());
      // done with this partition, remove it from end offsets
      assignment.remove(record.partition());
      if (assignment.isEmpty()) {
        logInfo("[Increment-Offsets] read all partitions");
        Preconditions.checkState(
            driverStatus == DriverStatus.OPEN,
            "WTH?! Cannot change driver status to [%s] from [%s]",
            DriverStatus.COMPLETE,
            driverStatus
        );
        driverStatus = DriverStatus.COMPLETE;
      }
    }
  }

  boolean isCheckPointingRequired()
  {
    return maxRowsPerSegmentLimitReached && !checkPointed;
  }

  boolean isComplete()
  {
    return driverStatus == DriverStatus.COMPLETE;
  }

  void setEndOffsets(Map<Integer, Long> offsets)
  {
    // sanity check again with driver's local offsets
    if (!endOffsets.keySet().containsAll(offsets.keySet())) {
      throw new IAE(
          "Got request to set some offsets not handled by driver [%d], handling [%s] partitions",
          endOffsets.keySet()
      );
    }
    for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
      if (entry.getValue().compareTo(nextPartitionOffsets.get(entry.getKey())) < 0) {
        throw new IAE(
            "End offset must be >= current offset for driver [%d] for partition [%s] (current: %s)",
            driverIndex,
            entry.getKey(),
            nextPartitionOffsets.get(entry.getKey())
        );
      }
    }
    endOffsets.putAll(offsets);
    // check if any or all partitions have been consumed
    for (Map.Entry<Integer, Long> entry : endOffsets.entrySet()) {
      if (entry.getValue().compareTo(nextPartitionOffsets.get(entry.getKey())) == 0) {
        assignment.remove(entry.getKey());
        logInfo("[Set-End-Offsets] read partition [%d]", entry.getKey());
      }
    }
    logInfo("endOffsets changed to [%s]", endOffsets);
    if (assignment.isEmpty()) {
      logInfo("[Set-End-Offsets] read all partitions");
      Preconditions.checkState(
          driverStatus == DriverStatus.OPEN,
          "WTH?! Cannot change driver status to [%s] from [%s]",
          DriverStatus.COMPLETE,
          driverStatus
      );
      driverStatus = DriverStatus.COMPLETE;
    }
    checkPointed = true;
    logInfo("check-pointed");
  }

  SegmentsAndMetadata finish(final TaskToolbox toolbox, final boolean isUseTransaction)
      throws InterruptedException
  {
    try {
      final TransactionalSegmentPublisher publisher = new TransactionalSegmentPublisher()
      {
        @Override
        public boolean publishSegments(Set<DataSegment> segments, Object commitMetadata) throws IOException
        {
          final KafkaPartitions finalPartitions = toolbox.getObjectMapper().convertValue(
              ((Map) commitMetadata).get(KafkaIndexTask.METADATA_NEXT_PARTITIONS),
              KafkaPartitions.class
          );

          // Sanity check, we should only be publishing things that match our desired end state.
          if (!endOffsets.equals(finalPartitions.getPartitionOffsetMap())) {
            throw new ISE("WTF?! Driver attempted to publish invalid metadata[%s].", commitMetadata);
          }

          final SegmentTransactionalInsertAction action;

          if (isUseTransaction) {
            action = new SegmentTransactionalInsertAction(
                segments,
                new KafkaDataSourceMetadata(new KafkaPartitions(topic, startOffsets)),
                new KafkaDataSourceMetadata(finalPartitions)
            );
          } else {
            action = new SegmentTransactionalInsertAction(segments, null, null);
          }

          logInfo("Publishing with isTransaction[%s].", isUseTransaction);

          return toolbox.getTaskActionClient().submit(action).isSuccess();
        }
      };

      return driver.finish(publisher, committerSupplier.get());
    }
    catch (InterruptedException | RejectedExecutionException e) {
      logError("Interrupted in finish");
      throw e;
    }
  }

  void waitForHandOff() throws InterruptedException
  {
    driver.waitForHandOff();
  }

  Object persist() throws InterruptedException
  {
    return driver.persist(committerSupplier.get());
  }

  public DriverMetadata getMetadata()
  {
    return new DriverMetadata(
        topic,
        startOffsets,
        endOffsets,
        driverIndex,
        sequenceName,
        last,
        checkPointed,
        maxRowsPerSegmentLimitReached
    );
  }

  @Override
  public void close() throws IOException
  {
    if (driverStatus == DriverStatus.CLOSED) {
      logWarn("already closed");
    } else {
      synchronized (this) {
        if (driverStatus == DriverStatus.CLOSED) {
          logWarn("already closed");
          return;
        }
        driverStatus = DriverStatus.CLOSED;
      }
      driver.getAppenderator().close();
      driver.close();
    }
  }

  void setLast() {
    last = true;
  }

  FiniteAppenderatorDriver getDriver()
  {
    return driver;
  }

  Map<Integer, Long> getStartOffsets()
  {
    return startOffsets;
  }

  Map<Integer, Long> getEndOffsets()
  {
    return endOffsets;
  }

  int getDriverIndex()
  {
    return driverIndex;
  }

  boolean isLast()
  {
    return last;
  }

  boolean isCheckPointed()
  {
    return checkPointed;
  }

  private void logInfo(String msg, Object... formatArgs)
  {
    log.info("Driver [%d], status [%s]: [%s]", driverIndex, driverStatus, String.format(msg, formatArgs));
  }

  private void logWarn(String msg, Object... formatArgs)
  {
    log.warn("Driver [%d], status [%s]: [%s]", driverIndex, driverStatus, String.format(msg, formatArgs));
  }

  private void logError(String msg, Object... formatArgs)
  {
    log.error("Driver [%d], status [%s]: [%s]", driverIndex, driverStatus, String.format(msg, formatArgs));
  }

  @Override
  public String toString()
  {
    return "DriverHolder{" +
           "driverStatus=" + driverStatus +
           ", driver=" + driver +
           ", assignment=" + assignment +
           ", committerSupplier=" + committerSupplier +
           ", nextPartitionOffsets=" + nextPartitionOffsets +
           ", topic='" + topic + '\'' +
           ", startOffsets=" + startOffsets +
           ", endOffsets=" + endOffsets +
           ", sequenceName='" + sequenceName + '\'' +
           ", driverIndex=" + driverIndex +
           ", last=" + last +
           ", checkPointed=" + checkPointed +
           ", maxRowsPerSegmentLimitReached=" + maxRowsPerSegmentLimitReached +
           '}';
  }

  static DriverHolder getNextDriverHolder(
      KafkaIndexTask taskContext,
      Map<Integer, Long> startOffsets,
      Map<Integer, Long> endOffsets,
      String baseSequenceName,
      FireDepartmentMetrics fireDepartmentMetrics,
      TaskToolbox taskToolbox
  )
  {
    return createDriverHolder(
        taskContext,
        startOffsets,
        endOffsets,
        taskContext.nextDriverIndex++,
        baseSequenceName,
        fireDepartmentMetrics,
        taskToolbox,
        false,
        false,
        false
    );
  }

  static DriverHolder getNextDriverHolder(
      KafkaIndexTask taskContext,
      Map<Integer, Long> startOffsets,
      Map<Integer, Long> endOffsets,
      String baseSequenceName,
      FireDepartmentMetrics fireDepartmentMetrics,
      TaskToolbox taskToolbox,
      boolean checkPointed
  )
  {
    return createDriverHolder(
        taskContext,
        startOffsets,
        endOffsets,
        taskContext.nextDriverIndex++,
        baseSequenceName,
        fireDepartmentMetrics,
        taskToolbox,
        false,
        checkPointed,
        false
    );
  }

  static DriverHolder createDriverHolder(
      KafkaIndexTask taskContext,
      Map<Integer, Long> startOffsets,
      Map<Integer, Long> endOffsets,
      int basePersistDirectoryIndex,
      String baseSequenceName,
      FireDepartmentMetrics fireDepartmentMetrics,
      TaskToolbox taskToolbox,
      boolean last,
      boolean checkPointed,
      boolean maxRowsPerSegmentLimitReached
  )
  {
    final File basePersistDir = new File(
        taskToolbox.getTaskWorkDir(),
        String.format("%s%s", "persist", basePersistDirectoryIndex > 0 ? basePersistDirectoryIndex : "")
    );
    DriverHolder holder = new DriverHolder(
        taskContext.getIOConfig().getStartPartitions().getTopic(),
        taskContext.newDriver(taskContext.newAppenderator(
            fireDepartmentMetrics,
            taskToolbox,
            basePersistDir
        ), taskToolbox, fireDepartmentMetrics),
        startOffsets,
        endOffsets,
        basePersistDirectoryIndex,
        String.format("%s_%d", baseSequenceName, basePersistDirectoryIndex),
        last,
        checkPointed,
        maxRowsPerSegmentLimitReached
    );
    log.info("Created new Driver with metadata [%s]", holder.getMetadata());
    return holder;
  }

  public static class DriverMetadata implements Comparable<DriverMetadata>
  {
    private final String topic;
    private final Map<Integer, Long> startOffsets;
    private final Map<Integer, Long> endOffsets;
    private final int driverIndex;
    private final String sequenceName;
    private final boolean last;
    private final boolean checkPointed;
    private final boolean maxRowsPerSegmentLimitReached;

    @JsonCreator
    public DriverMetadata(
        @JsonProperty("topic") String topic,
        @JsonProperty("startOffsets") Map<Integer, Long> startOffsets,
        @JsonProperty("endOffsets") Map<Integer, Long> endOffsets,
        @JsonProperty("driverIndex") int driverIndex,
        @JsonProperty("sequenceName") String sequenceName,
        @JsonProperty("last") boolean last,
        @JsonProperty("checkPointed") boolean checkPointed,
        @JsonProperty("maxRowsPerSegmentLimitReached") boolean maxRowsPerSegmentLimitReached
    )
    {
      this.topic = topic;
      this.startOffsets = startOffsets;
      this.endOffsets = endOffsets;
      this.driverIndex = driverIndex;
      this.sequenceName = sequenceName;
      this.last = last;
      this.checkPointed = checkPointed;
      this.maxRowsPerSegmentLimitReached = maxRowsPerSegmentLimitReached;
    }

    @JsonProperty
    public int getDriverIndex()
    {
      return driverIndex;
    }

    @JsonProperty
    public String getSequenceName()
    {
      return sequenceName;
    }

    @JsonProperty
    public Map<Integer, Long> getEndOffsets()
    {
      return endOffsets;
    }

    @JsonProperty
    public Map<Integer, Long> getStartOffsets()
    {
      return startOffsets;
    }

    @JsonProperty
    public String getTopic()
    {
      return topic;
    }

    @JsonProperty
    public boolean isLast()
    {
      return last;
    }

    @JsonProperty
    public boolean isCheckPointed()
    {
      return checkPointed;
    }

    @JsonProperty
    public boolean isMaxRowsPerSegmentLimitReached()
    {
      return maxRowsPerSegmentLimitReached;
    }

    @Override
    public String toString()
    {
      return "DriverMetadata{" +
             "topic='" + topic + '\'' +
             ", startOffsets=" + startOffsets +
             ", endOffsets=" + endOffsets +
             ", driverIndex=" + driverIndex +
             ", sequenceName='" + sequenceName + '\'' +
             ", last=" + last +
             ", checkPointed=" + checkPointed +
             ", maxRowsPerSegmentLimitReached=" + maxRowsPerSegmentLimitReached +
             '}';
    }

    @Override
    public int compareTo(DriverMetadata o)
    {
      return driverIndex - o.driverIndex;
    }
  }

  static class SentinelDriverHolder extends DriverHolder
  {
    final CountDownLatch persistLatch;
    final CountDownLatch handOffLatch;

    SentinelDriverHolder(
        CountDownLatch persistLatch,
        CountDownLatch handOffLatch
    )
    {
      super(null, null, ImmutableMap.<Integer, Long>of(), ImmutableMap.<Integer, Long>of(), -1, null, true, true, true);
      super.driverStatus = DriverStatus.COMPLETE;
      this.persistLatch = persistLatch;
      this.handOffLatch = handOffLatch;
    }

    @Override
    Object persist() throws InterruptedException
    {
      persistLatch.countDown();
      return null;
    }

    @Override
    SegmentsAndMetadata finish(TaskToolbox toolbox, boolean isUseTransaction) throws InterruptedException
    {
      // Do nothing
      return null;
    }

    @Override
    public void close() throws IOException
    {
      handOffLatch.countDown();
    }
  }
}
