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

package org.apache.druid.indexing.kinesis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.druid.common.aws.AWSClientUtil;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.common.aws.AWSCredentialsUtils;
import org.apache.druid.data.input.kinesis.KinesisRecordEntity;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisor;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.MemoryBoundLinkedBlockingQueue;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * This class implements a local buffer for storing fetched Kinesis records. Fetching is done
 * in background threads.
 */
public class KinesisRecordSupplier implements RecordSupplier<String, String, KinesisRecordEntity>
{
  private static final EmittingLogger log = new EmittingLogger(KinesisRecordSupplier.class);
  private static final long PROVISIONED_THROUGHPUT_EXCEEDED_BACKOFF_MS = 3000;
  private static final long EXCEPTION_RETRY_DELAY_MS = 10000;

  /**
   * We call getRecords with limit 1000 to make sure that we can find the first (earliest) record in the shard.
   * In the case where the shard is constantly removing records that are past their retention period, it is possible
   * that we never find the first record in the shard if we use a limit of 1.
   */
  private static final int GET_SEQUENCE_NUMBER_RECORD_COUNT = 1000;
  private static final int GET_SEQUENCE_NUMBER_RETRY_COUNT = 10;

  /**
   * Catch any exception and wrap it in a {@link StreamException}
   */
  private static <T> T wrapExceptions(Callable<T> callable)
  {
    try {
      return callable.call();
    }
    catch (Exception e) {
      throw new StreamException(e);
    }
  }

  private class PartitionResource
  {
    private final StreamPartition<String> streamPartition;

    // shardIterator points to the record that will be polled next by recordRunnable
    // can be null when shard is closed due to the user shard splitting or changing the number
    // of shards in the stream, in which case a 'EOS' marker is used by the KinesisRecordSupplier
    // to indicate that this shard has no more records to read
    @Nullable
    private volatile String shardIterator;
    private volatile long currentLagMillis;

    private final AtomicBoolean fetchStarted = new AtomicBoolean();
    private ScheduledFuture<?> currentFetch;

    private PartitionResource(StreamPartition<String> streamPartition)
    {
      this.streamPartition = streamPartition;
    }

    private void startBackgroundFetch()
    {
      if (!backgroundFetchEnabled) {
        return;
      }
      // if seek has been called
      if (shardIterator == null) {
        log.warn(
            "Skipping background fetch for stream[%s] partition[%s] since seek has not been called for this partition",
            streamPartition.getStream(),
            streamPartition.getPartitionId()
        );
        return;
      }
      if (fetchStarted.compareAndSet(false, true)) {
        log.debug(
            "Starting scheduled fetch for stream[%s] partition[%s]",
            streamPartition.getStream(),
            streamPartition.getPartitionId()
        );

        scheduleBackgroundFetch(fetchDelayMillis);
      }
    }

    private void stopBackgroundFetch()
    {
      if (fetchStarted.compareAndSet(true, false)) {
        log.debug(
            "Stopping scheduled fetch for stream[%s] partition[%s]",
            streamPartition.getStream(),
            streamPartition.getPartitionId()
        );
        if (currentFetch != null && !currentFetch.isDone()) {
          currentFetch.cancel(true);
        }
      }
    }

    private void scheduleBackgroundFetch(long delayMillis)
    {
      if (fetchStarted.get()) {
        try {
          currentFetch = scheduledExec.schedule(fetchRecords(), delayMillis, TimeUnit.MILLISECONDS);
        }
        catch (RejectedExecutionException e) {
          log.warn(
              e,
              "Caught RejectedExecutionException, KinesisRecordSupplier for partition[%s] has likely temporarily shutdown the ExecutorService. "
              + "This is expected behavior after calling seek(), seekToEarliest() and seekToLatest()",
              streamPartition.getPartitionId()
          );

        }
      } else {
        log.debug("Worker for partition[%s] is already stopped", streamPartition.getPartitionId());
      }
    }

    private Runnable fetchRecords()
    {
      return () -> {
        if (!fetchStarted.get()) {
          log.debug("Worker for partition[%s] has been stopped", streamPartition.getPartitionId());
          return;
        }

        // used for retrying on InterruptedException
        GetRecordsResponse recordsResult = null;
        OrderedPartitionableRecord<String, String, KinesisRecordEntity> currRecord;
        long recordBufferOfferWaitMillis;
        try {

          if (shardIterator == null) {
            log.info("shardIterator[%s] has been closed and has no more records", streamPartition.getPartitionId());

            // add an end-of-shard marker so caller knows this shard is closed
            currRecord = new OrderedPartitionableRecord<>(
                streamPartition.getStream(),
                streamPartition.getPartitionId(),
                KinesisSequenceNumber.END_OF_SHARD_MARKER,
                null
            );

            recordBufferOfferWaitMillis = recordBufferOfferTimeout;
            while (!records.offer(
                new MemoryBoundLinkedBlockingQueue.ObjectContainer<>(currRecord, 0),
                recordBufferOfferWaitMillis,
                TimeUnit.MILLISECONDS
            )) {
              log.warn("Kinesis records are being processed slower than they are fetched. "
                       + "OrderedPartitionableRecord buffer full, retrying in [%,dms].",
                       recordBufferFullWait);
              recordBufferOfferWaitMillis = recordBufferFullWait;
            }

            return;
          }

          recordsResult = kinesis.getRecords(
              GetRecordsRequest.builder().shardIterator(shardIterator).build()
          );

          currentLagMillis = recordsResult.millisBehindLatest();

          // list will come back empty if there are no records
          for (software.amazon.awssdk.services.kinesis.model.Record kinesisRecord : recordsResult.records()) {
            final List<KinesisRecordEntity> data = new ArrayList<>();

            // Convert SDK v2 Record to KinesisClientRecord for KCL v2 deaggregation
            KinesisClientRecord clientRecord = KinesisClientRecord.builder()
                .sequenceNumber(kinesisRecord.sequenceNumber())
                .partitionKey(kinesisRecord.partitionKey())
                .data(kinesisRecord.data().asByteBuffer())
                .approximateArrivalTimestamp(kinesisRecord.approximateArrivalTimestamp())
                .build();

            // Deaggregate using KCL v2's AggregatorUtil
            final List<KinesisClientRecord> userRecords = aggregatorUtil.deaggregate(
                Collections.singletonList(clientRecord)
            );

            int recordSize = 0;
            for (KinesisClientRecord userRecord : userRecords) {
              KinesisRecordEntity kinesisRecordEntity = new KinesisRecordEntity(userRecord);
              recordSize += kinesisRecordEntity.getBuffer().remaining();
              data.add(kinesisRecordEntity);
            }


            currRecord = new OrderedPartitionableRecord<>(
                streamPartition.getStream(),
                streamPartition.getPartitionId(),
                kinesisRecord.sequenceNumber(),
                data
            );


            if (log.isTraceEnabled()) {
              log.trace(
                  "Stream[%s] / partition[%s] / sequenceNum[%s] / bufferByteSize[%d] / bufferRemainingByteCapacity[%d]: %s",
                  currRecord.getStream(),
                  currRecord.getPartitionId(),
                  currRecord.getSequenceNumber(),
                  records.byteSize(),
                  records.remainingCapacity(),
                  currRecord.getData()
                            .stream()
                            .map(b -> StringUtils.fromUtf8(
                                // duplicate buffer to avoid changing its position when logging
                                b.getBuffer().duplicate())
                            )
                            .collect(Collectors.toList())
              );
            }

            recordBufferOfferWaitMillis = recordBufferOfferTimeout;
            while (!records.offer(
                new MemoryBoundLinkedBlockingQueue.ObjectContainer<>(currRecord, recordSize),
                recordBufferOfferWaitMillis,
                TimeUnit.MILLISECONDS
            )) {
              log.warn(
                  "Kinesis records are being processed slower than they are fetched. "
                  + "OrderedPartitionableRecord buffer full, storing iterator and retrying in [%,dms].",
                  recordBufferFullWait
              );
              recordBufferOfferWaitMillis = recordBufferFullWait;
            }
          }

          shardIterator = recordsResult.nextShardIterator(); // will be null if the shard has been closed

          scheduleBackgroundFetch(fetchDelayMillis);
        }
        catch (ProvisionedThroughputExceededException e) {
          log.warn(
              e,
              "encounted ProvisionedThroughputExceededException while fetching records, this means "
              + "that the request rate for the stream is too high, or the requested data is too large for "
              + "the available throughput. Reduce the frequency or size of your requests."
          );
          long retryMs = Math.max(PROVISIONED_THROUGHPUT_EXCEEDED_BACKOFF_MS, fetchDelayMillis);
          scheduleBackgroundFetch(retryMs);
        }
        catch (InterruptedException e) {
          // may happen if interrupted while BlockingQueue.offer() is waiting
          log.warn(
              e,
              "Interrupted while waiting to add record to buffer, retrying in [%,dms]",
              EXCEPTION_RETRY_DELAY_MS
          );
          scheduleBackgroundFetch(EXCEPTION_RETRY_DELAY_MS);
        }
        catch (ExpiredIteratorException e) {
          log.warn(
              e,
              "ShardIterator expired while trying to fetch records, retrying in [%,dms]",
              fetchDelayMillis
          );
          if (recordsResult != null) {
            shardIterator = recordsResult.nextShardIterator(); // will be null if the shard has been closed
            scheduleBackgroundFetch(fetchDelayMillis);
          } else {
            throw new ISE("can't reschedule fetch records runnable, recordsResult is null??");
          }
        }
        catch (ResourceNotFoundException | InvalidArgumentException e) {
          // aws errors
          log.error(e, "encounted AWS error while attempting to fetch records, will not retry");
          throw e;
        }
        catch (SdkException e) {
          if (AWSClientUtil.isClientExceptionRecoverable(e)) {
            log.warn(e, "encounted unknown recoverable AWS exception, retrying in [%,dms]", EXCEPTION_RETRY_DELAY_MS);
            scheduleBackgroundFetch(EXCEPTION_RETRY_DELAY_MS);
          } else {
            log.warn(e, "encounted unknown unrecoverable AWS exception, will not retry");
            throw new RuntimeException(e);
          }
        }
        catch (Throwable e) {
          // non transient errors
          log.error(e, "unknown fetchRecords exception, will not retry");
          throw new RuntimeException(e);
        }

      };
    }

    private void seek(ShardIteratorType iteratorEnum, String sequenceNumber)
    {
      log.debug(
          "Seeking partition [%s] to [%s]",
          streamPartition.getPartitionId(),
          sequenceNumber != null ? sequenceNumber : iteratorEnum.toString()
      );

      GetShardIteratorRequest.Builder requestBuilder = GetShardIteratorRequest.builder()
          .streamName(streamPartition.getStream())
          .shardId(streamPartition.getPartitionId())
          .shardIteratorType(iteratorEnum);

      if (sequenceNumber != null) {
        requestBuilder.startingSequenceNumber(sequenceNumber);
      }

      shardIterator = wrapExceptions(() -> kinesis.getShardIterator(requestBuilder.build()).shardIterator());
    }

    private long getPartitionTimeLag()
    {
      return currentLagMillis;
    }
  }

  private final KinesisClient kinesis;
  private final AggregatorUtil aggregatorUtil = new AggregatorUtil();
  private final int fetchDelayMillis;
  private final int recordBufferOfferTimeout;
  private final int recordBufferFullWait;
  private final int maxBytesPerPoll;
  private final int fetchThreads;
  private final int recordBufferSizeBytes;
  private final boolean useEarliestSequenceNumber;
  private final boolean useListShards;

  private ScheduledExecutorService scheduledExec;

  private final ConcurrentMap<StreamPartition<String>, PartitionResource> partitionResources =
      new ConcurrentHashMap<>();
  private MemoryBoundLinkedBlockingQueue<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> records;

  private final boolean backgroundFetchEnabled;
  private volatile boolean closed = false;
  private final AtomicBoolean partitionsFetchStarted = new AtomicBoolean();

  public KinesisRecordSupplier(
      KinesisClient kinesisClient,
      int fetchDelayMillis,
      int fetchThreads,
      int recordBufferSizeBytes,
      int recordBufferOfferTimeout,
      int recordBufferFullWait,
      int maxBytesPerPoll,
      boolean useEarliestSequenceNumber,
      boolean useListShards
  )
  {
    Preconditions.checkNotNull(kinesisClient);
    this.kinesis = kinesisClient;
    this.fetchDelayMillis = fetchDelayMillis;
    this.recordBufferOfferTimeout = recordBufferOfferTimeout;
    this.recordBufferFullWait = recordBufferFullWait;
    this.maxBytesPerPoll = maxBytesPerPoll;
    this.fetchThreads = fetchThreads;
    this.recordBufferSizeBytes = recordBufferSizeBytes;
    this.useEarliestSequenceNumber = useEarliestSequenceNumber;
    this.useListShards = useListShards;
    this.backgroundFetchEnabled = fetchThreads > 0;

    if (backgroundFetchEnabled) {
      log.info(
          "Creating fetch thread pool of size [%d] (Runtime.availableProcessors=%d)",
          fetchThreads,
          Runtime.getRuntime().availableProcessors()
      );

      scheduledExec = Executors.newScheduledThreadPool(
          fetchThreads,
          Execs.makeThreadFactory("KinesisRecordSupplier-Worker-%d")
      );
    }

    records = new MemoryBoundLinkedBlockingQueue<>(recordBufferSizeBytes);
  }

  public static KinesisClient getAmazonKinesisClient(
      String endpoint,
      AWSCredentialsConfig awsCredentialsConfig,
      String awsAssumedRoleArn,
      String awsExternalId
  )
  {
    AwsCredentialsProvider credentialsProvider = AWSCredentialsUtils.defaultAWSCredentialsProviderChain(
        awsCredentialsConfig
    );

    if (awsAssumedRoleArn != null) {
      log.info("Assuming role [%s] with externalId [%s]", awsAssumedRoleArn, awsExternalId);

      AssumeRoleRequest.Builder assumeRoleBuilder = AssumeRoleRequest.builder()
          .roleArn(awsAssumedRoleArn)
          .roleSessionName(StringUtils.format("druid-kinesis-%s", UUID.randomUUID().toString()));

      if (awsExternalId != null) {
        assumeRoleBuilder.externalId(awsExternalId);
      }

      StsClient stsClient = StsClient.builder()
          .credentialsProvider(credentialsProvider)
          .build();

      credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
          .stsClient(stsClient)
          .refreshRequest(assumeRoleBuilder.build())
          .build();
    }

    KinesisClientBuilder builder = KinesisClient.builder()
        .credentialsProvider(credentialsProvider);

    if (endpoint != null && !endpoint.isEmpty()) {
      // Only set endpoint override if it's a valid URI with a scheme
      URI endpointUri = URI.create(endpoint);
      if (endpointUri.getScheme() != null) {
        builder.endpointOverride(endpointUri);
        // Extract region from endpoint if possible
        Region region = parseRegionFromEndpoint(endpoint);
        if (region != null) {
          builder.region(region);
        }
      } else {
        log.warn("Invalid endpoint URI provided (no scheme): [%s]. Using default endpoint.", endpoint);
      }
    }

    return builder.build();
  }

  /**
   * Parse region from a Kinesis endpoint URL.
   * Expected format: https://kinesis.{region}.amazonaws.com
   */
  private static Region parseRegionFromEndpoint(String endpoint)
  {
    if (endpoint == null) {
      return null;
    }
    // Try to extract region from endpoint like "https://kinesis.us-east-1.amazonaws.com"
    String lowerEndpoint = endpoint.toLowerCase(Locale.ENGLISH);
    if (lowerEndpoint.contains(".amazonaws.com")) {
      int kinesisIndex = lowerEndpoint.indexOf("kinesis.");
      if (kinesisIndex >= 0) {
        int regionStart = kinesisIndex + "kinesis.".length();
        int regionEnd = lowerEndpoint.indexOf(".amazonaws.com");
        if (regionEnd > regionStart) {
          String regionStr = endpoint.substring(regionStart, regionEnd);
          return Region.of(regionStr);
        }
      }
    }
    return null;
  }

  @VisibleForTesting
  public void start()
  {
    checkIfClosed();
    if (backgroundFetchEnabled && partitionsFetchStarted.compareAndSet(false, true)) {
      partitionResources.values().forEach(PartitionResource::startBackgroundFetch);
    }
  }

  @Override
  public void close()
  {
    if (this.closed) {
      return;
    }

    assign(ImmutableSet.of());

    if (scheduledExec != null) {
      scheduledExec.shutdown();

      try {
        if (!scheduledExec.awaitTermination(EXCEPTION_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)) {
          scheduledExec.shutdownNow();
        }
      }
      catch (InterruptedException e) {
        log.warn(e, "InterruptedException while shutting down");
        throw new RuntimeException(e);
      }
    }

    this.closed = true;
  }

  @Override
  public void assign(Set<StreamPartition<String>> collection)
  {
    checkIfClosed();

    collection.forEach(
        streamPartition -> partitionResources.putIfAbsent(
            streamPartition,
            new PartitionResource(streamPartition)
        )
    );

    Iterator<Map.Entry<StreamPartition<String>, PartitionResource>> i = partitionResources.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry<StreamPartition<String>, PartitionResource> entry = i.next();
      if (!collection.contains(entry.getKey())) {
        i.remove();
        entry.getValue().stopBackgroundFetch();
      }
    }
  }

  @Override
  public Collection<StreamPartition<String>> getAssignment()
  {
    return partitionResources.keySet();
  }

  @Override
  public void seek(StreamPartition<String> partition, String sequenceNumber) throws InterruptedException
  {
    filterBufferAndResetBackgroundFetch(ImmutableSet.of(partition));
    if (KinesisSequenceNumber.UNREAD_TRIM_HORIZON.equals(sequenceNumber)) {
      partitionSeek(partition, null, ShardIteratorType.TRIM_HORIZON);
    } else if (KinesisSequenceNumber.UNREAD_LATEST.equals(sequenceNumber)) {
      partitionSeek(partition, null, ShardIteratorType.LATEST);
    } else {
      partitionSeek(partition, sequenceNumber, ShardIteratorType.AT_SEQUENCE_NUMBER);
    }
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<String>> partitions) throws InterruptedException
  {
    filterBufferAndResetBackgroundFetch(partitions);
    partitions.forEach(partition -> partitionSeek(partition, null, ShardIteratorType.TRIM_HORIZON));
  }

  @Override
  public void seekToLatest(Set<StreamPartition<String>> partitions) throws InterruptedException
  {
    filterBufferAndResetBackgroundFetch(partitions);
    partitions.forEach(partition -> partitionSeek(partition, null, ShardIteratorType.LATEST));
  }

  @Nullable
  @Override
  public String getPosition(StreamPartition<String> partition)
  {
    throw new UnsupportedOperationException("getPosition() is not supported in Kinesis");
  }

  @Nonnull
  @Override
  public List<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> poll(long timeout)
  {
    start();

    try {
      List<MemoryBoundLinkedBlockingQueue.ObjectContainer<OrderedPartitionableRecord<String, String, KinesisRecordEntity>>> polledRecords = new ArrayList<>();

      records.drain(
          polledRecords,
          maxBytesPerPoll,
          timeout,
          TimeUnit.MILLISECONDS
      );

      return polledRecords.stream()
          .filter(x -> partitionResources.containsKey(x.getData().getStreamPartition()))
          .map(MemoryBoundLinkedBlockingQueue.ObjectContainer::getData)
          .collect(Collectors.toList());
    }
    catch (InterruptedException e) {
      log.warn(e, "Interrupted while polling");
      return Collections.emptyList();
    }

  }

  @Nullable
  @Override
  public String getLatestSequenceNumber(StreamPartition<String> partition)
  {
    return getSequenceNumber(partition, ShardIteratorType.LATEST);
  }

  @Nullable
  @Override
  public String getEarliestSequenceNumber(StreamPartition<String> partition)
  {
    return getSequenceNumber(partition, ShardIteratorType.TRIM_HORIZON);
  }

  @Override
  public boolean isOffsetAvailable(StreamPartition<String> partition, OrderedSequenceNumber<String> offset)
  {
    return wrapExceptions(() -> {
      KinesisSequenceNumber kinesisSequence = (KinesisSequenceNumber) offset;
      // No records have been read from the stream and any record is valid
      if (kinesisSequence.isUnread()) {
        return true;
      }
      // Any other custom sequence number
      if (!KinesisSequenceNumber.isValidAWSKinesisSequence(kinesisSequence.get())) {
        return false;
      }
      // The first record using AT_SEQUENCE_NUMBER should match the offset
      // Should not return empty records provided the record is present
      // Reference: https://docs.aws.amazon.com/streams/latest/dev/troubleshooting-consumers.html
      // Section: GetRecords Returns Empty Records Array Even When There is Data in the Stream
      String shardIterator = RetryUtils.retry(
          () -> kinesis.getShardIterator(
              GetShardIteratorRequest.builder()
                  .streamName(partition.getStream())
                  .shardId(partition.getPartitionId())
                  .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                  .startingSequenceNumber(kinesisSequence.get())
                  .build()
          ).shardIterator(),
          (throwable) -> {
            if (throwable instanceof ProvisionedThroughputExceededException) {
              log.warn(
                  throwable,
                  "encountered ProvisionedThroughputExceededException while fetching records, this means "
                  + "that the request rate for the stream is too high, or the requested data is too large for "
                  + "the available throughput. Reduce the frequency or size of your requests. Consider increasing "
                  + "the number of shards to increase throughput."
              );
              return true;
            }
            if (throwable instanceof SdkException) {
              SdkException sdkException = (SdkException) throwable;
              return AWSClientUtil.isClientExceptionRecoverable(sdkException);
            }
            return false;
          },
          GET_SEQUENCE_NUMBER_RETRY_COUNT
      );
      GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder()
          .shardIterator(shardIterator)
          .build();
      List<software.amazon.awssdk.services.kinesis.model.Record> records = RetryUtils.retry(
          () -> kinesis.getRecords(getRecordsRequest).records(),
          (throwable) -> {
            if (throwable instanceof ProvisionedThroughputExceededException) {
              log.warn(
                  throwable,
                  "encountered ProvisionedThroughputExceededException while fetching records, this means "
                  + "that the request rate for the stream is too high, or the requested data is too large for "
                  + "the available throughput. Reduce the frequency or size of your requests. Consider increasing "
                  + "the number of shards to increase throughput."
              );
              return true;
            }
            if (throwable instanceof SdkException) {
              SdkException sdkException = (SdkException) throwable;
              return AWSClientUtil.isClientExceptionRecoverable(sdkException);
            }
            return false;
          },
          GET_SEQUENCE_NUMBER_RETRY_COUNT
      );
      return !records.isEmpty() && records.get(0).sequenceNumber().equals(kinesisSequence.get());
    });
  }

  private Set<Shard> getShards(String stream)
  {
    if (useListShards) {
      return getShardsUsingListShards(stream);
    }
    return getShardsUsingDescribeStream(stream);
  }

  /**
   * Default method to avoid incompatibility when user doesn't have sufficient IAM permissions on AWS
   * Not advised. getShardsUsingListShards is recommended instead if sufficient permissions are present.
   *
   * @param stream name of stream
   * @return Immutable set of shards
   */
  private Set<Shard> getShardsUsingDescribeStream(String stream)
  {
    ImmutableSet.Builder<Shard> shards = ImmutableSet.builder();
    String exclusiveStartShardId = null;
    boolean hasMoreShards = true;

    while (hasMoreShards) {
      DescribeStreamRequest.Builder requestBuilder = DescribeStreamRequest.builder()
          .streamName(stream);
      if (exclusiveStartShardId != null) {
        requestBuilder.exclusiveStartShardId(exclusiveStartShardId);
      }

      DescribeStreamResponse response = kinesis.describeStream(requestBuilder.build());
      StreamDescription description = response.streamDescription();
      List<Shard> shardResult = description.shards();
      shards.addAll(shardResult);

      hasMoreShards = description.hasMoreShards();
      if (hasMoreShards && !shardResult.isEmpty()) {
        exclusiveStartShardId = Iterables.getLast(shardResult).shardId();
      }
    }
    return shards.build();
  }

  /**
   * If the user has the IAM policy for listShards, and useListShards is true:
   * Use the API listShards which is the recommended way instead of describeStream
   * listShards can return 1000 shards per call and has a limit of 100TPS
   * This makes the method resilient to LimitExceeded exceptions (compared to 100 shards, 10 TPS of describeStream)
   *
   * @param stream name of stream
   *
   * @return Set of Shard ids
   */
  private Set<Shard> getShardsUsingListShards(String stream)
  {
    ImmutableSet.Builder<Shard> shards = ImmutableSet.builder();
    String nextToken = null;
    boolean firstRequest = true;

    while (firstRequest || nextToken != null) {
      ListShardsRequest.Builder requestBuilder = ListShardsRequest.builder();
      if (firstRequest) {
        requestBuilder.streamName(stream);
        firstRequest = false;
      } else {
        requestBuilder.nextToken(nextToken);
      }

      ListShardsResponse result = kinesis.listShards(requestBuilder.build());
      shards.addAll(result.shards());
      nextToken = result.nextToken();
    }
    return shards.build();
  }

  @Override
  public Set<String> getPartitionIds(String stream)
  {
    return wrapExceptions(() -> {
      Set<String> partitionIds = new TreeSet<>();
      for (Shard shard : getShards(stream)) {
        partitionIds.add(shard.shardId());
      }
      return partitionIds;
    });
  }

  /**
   * Fetch the partition lag, given a stream and set of current partition offsets. This operates independently from
   * the {@link PartitionResource} which have been assigned to this record supplier.
   */
  public Map<String, Long> getPartitionsTimeLag(String stream, Map<String, String> currentOffsets)
  {
    Map<String, Long> partitionLag = Maps.newHashMapWithExpectedSize(currentOffsets.size());
    for (Map.Entry<String, String> partitionOffset : currentOffsets.entrySet()) {
      StreamPartition<String> partition = new StreamPartition<>(stream, partitionOffset.getKey());
      long currentLag = 0L;
      if (KinesisSequenceNumber.isValidAWSKinesisSequence(partitionOffset.getValue())) {
        currentLag = getPartitionTimeLag(partition, partitionOffset.getValue());
      }
      partitionLag.put(partitionOffset.getKey(), currentLag);
    }
    return partitionLag;
  }

  /**
   * This method is only used for tests to verify that {@link PartitionResource} in fact tracks it's current lag
   * as it is polled for records. This isn't currently used in production at all, but could be one day if we were
   * to prefer to get the lag from the running tasks in the same API call which fetches the current task offsets,
   * instead of directly calling the AWS Kinesis API with the offsets returned from those tasks
   * (see {@link #getPartitionsTimeLag}, which accepts a map of current partition offsets).
   */
  @VisibleForTesting
  Map<String, Long> getPartitionResourcesTimeLag()
  {
    return partitionResources.entrySet()
                             .stream()
                             .collect(
                                 Collectors.toMap(
                                     k -> k.getKey().getPartitionId(),
                                     k -> k.getValue().getPartitionTimeLag()
                                 )
                             );
  }

  @VisibleForTesting
  public int bufferSize()
  {
    return records.size();
  }

  @VisibleForTesting
  public boolean isBackgroundFetchRunning()
  {
    return partitionsFetchStarted.get();
  }

  @VisibleForTesting
  public boolean isAnyFetchActive()
  {
    return partitionResources.values()
                             .stream()
                             .map(pr -> pr.currentFetch)
                             .anyMatch(fetch -> (fetch != null && !fetch.isDone()));
  }

  /**
   * Check that a {@link PartitionResource} has been assigned to this record supplier, and if so call
   * {@link PartitionResource#seek} to move it to the latest offsets. Note that this method does not restart background
   * fetch, which should have been stopped prior to calling this method by a call to
   * {@link #filterBufferAndResetBackgroundFetch}.
   */
  private void partitionSeek(StreamPartition<String> partition, String sequenceNumber, ShardIteratorType iteratorEnum)
  {
    PartitionResource resource = partitionResources.get(partition);
    if (resource == null) {
      throw new ISE("Partition [%s] has not been assigned", partition);
    }
    resource.seek(iteratorEnum, sequenceNumber);
  }

  /**
   * Given a partition and a {@link ShardIteratorType}, create a shard iterator and fetch
   * {@link #GET_SEQUENCE_NUMBER_RECORD_COUNT} records and return the first sequence number from the result set.
   * This method is thread safe as it does not depend on the internal state of the supplier (it doesn't use the
   * {@link PartitionResource} which have been assigned to the supplier), and the Kinesis client is thread safe.
   * <p>
   * When there are no records at the offset corresponding to the ShardIteratorType,
   *    If shard is closed, return custom EOS sequence marker
   *    While getting the earliest sequence number, return a custom marker corresponding to TRIM_HORIZON
   *    While getting the most recent sequence number, return a custom marker corresponding to LATEST
   */
  @Nullable
  private String getSequenceNumber(StreamPartition<String> partition, ShardIteratorType iteratorEnum)
  {
    return wrapExceptions(() -> {
      String shardIterator = kinesis.getShardIterator(
          GetShardIteratorRequest.builder()
              .streamName(partition.getStream())
              .shardId(partition.getPartitionId())
              .shardIteratorType(iteratorEnum)
              .build()
      ).shardIterator();

      if (closed) {
        log.info("KinesisRecordSupplier closed while fetching sequenceNumber");
        return null;
      }
      final GetRecordsRequest request = GetRecordsRequest.builder()
          .shardIterator(shardIterator)
          .limit(GET_SEQUENCE_NUMBER_RECORD_COUNT)
          .build();
      GetRecordsResponse recordsResult = RetryUtils.retry(
          () -> kinesis.getRecords(request),
          (throwable) -> {
            if (throwable instanceof ProvisionedThroughputExceededException) {
              log.warn(
                  throwable,
                  "encountered ProvisionedThroughputExceededException while fetching records, this means "
                  + "that the request rate for the stream is too high, or the requested data is too large for "
                  + "the available throughput. Reduce the frequency or size of your requests. Consider increasing "
                  + "the number of shards to increase throughput."
              );
              return true;
            }
            if (throwable instanceof SdkException) {
              SdkException sdkException = (SdkException) throwable;
              return AWSClientUtil.isClientExceptionRecoverable(sdkException);
            }
            return false;
          },
          GET_SEQUENCE_NUMBER_RETRY_COUNT
      );

      List<software.amazon.awssdk.services.kinesis.model.Record> records = recordsResult.records();

      if (!records.isEmpty()) {
        return records.get(0).sequenceNumber();
      }

      if (recordsResult.nextShardIterator() == null) {
        log.info("Partition[%s] is closed and empty", partition.getPartitionId());
        return KinesisSequenceNumber.END_OF_SHARD_MARKER;
      }

      if (iteratorEnum.equals(ShardIteratorType.LATEST)) {
        log.info("Partition[%s] has no records at LATEST offset", partition.getPartitionId());
        return KinesisSequenceNumber.UNREAD_LATEST;
      }

      // Even if there are records in the shard, they may not be returned on the first call to getRecords with TRIM_HORIZON
      // Reference: https://docs.aws.amazon.com/streams/latest/dev/troubleshooting-consumers.html
      // Section: GetRecords Returns Empty Records Array Even When There is Data in the Stream
      if (iteratorEnum.equals(ShardIteratorType.TRIM_HORIZON)) {
        log.info("Partition[%s] has no records at TRIM_HORIZON offset", partition.getPartitionId());
        return KinesisSequenceNumber.UNREAD_TRIM_HORIZON;
      }

      log.warn("Could not fetch sequence number for Partition[%s]", partition.getPartitionId());
      return null;
    });
  }

  /**
   * Given a {@link StreamPartition} and an offset, create a 'shard iterator' for the offset and fetch a single record
   * in order to get the lag: {@link GetRecordsResponse#millisBehindLatest()}. This method is thread safe as it does
   * not depend on the internal state of the supplier (it doesn't use the {@link PartitionResource} which have been
   * assigned to the supplier), and the Kinesis client is thread safe.
   */
  private Long getPartitionTimeLag(StreamPartition<String> partition, String offset)
  {
    return wrapExceptions(() -> {
      final ShardIteratorType iteratorType;
      final String offsetToUse;
      if (offset == null || KinesisSupervisor.OFFSET_NOT_SET.equals(offset)) {
        if (useEarliestSequenceNumber) {
          iteratorType = ShardIteratorType.TRIM_HORIZON;
          offsetToUse = null;
        } else {
          // if offset is not set and not using earliest, it means we will start reading from latest,
          // so lag will be 0 and we have nothing to do here
          return 0L;
        }
      } else {
        iteratorType = ShardIteratorType.AT_SEQUENCE_NUMBER;
        offsetToUse = offset;
      }

      GetRecordsResponse recordsResult = getRecordsForLag(ShardIteratorType.AFTER_SEQUENCE_NUMBER, offsetToUse, partition);

      // If no more new data after offsetToUse, it means there is no lag for now.
      // So report lag points as 0L.
      if (recordsResult.records().isEmpty()) {
        return 0L;
      } else {
        recordsResult = getRecordsForLag(iteratorType, offsetToUse, partition);
      }

      return recordsResult.millisBehindLatest();
    });
  }

  private GetRecordsResponse getRecordsForLag(ShardIteratorType iteratorType, String offsetToUse, StreamPartition<String> partition)
  {
    GetShardIteratorRequest.Builder iteratorRequestBuilder = GetShardIteratorRequest.builder()
        .streamName(partition.getStream())
        .shardId(partition.getPartitionId())
        .shardIteratorType(iteratorType);

    if (offsetToUse != null) {
      iteratorRequestBuilder.startingSequenceNumber(offsetToUse);
    }

    String shardIterator = kinesis.getShardIterator(iteratorRequestBuilder.build()).shardIterator();

    return kinesis.getRecords(
        GetRecordsRequest.builder()
            .shardIterator(shardIterator)
            .limit(1)
            .build()
    );
  }

  /**
   * Explode if {@link #close()} has been called on the supplier.
   */
  private void checkIfClosed()
  {
    if (closed) {
      throw new ISE("Invalid operation - KinesisRecordSupplier has already been closed");
    }
  }

  /**
   * This method must be called before a seek operation ({@link #seek}, {@link #seekToLatest}, or
   * {@link #seekToEarliest}).
   * <p>
   * When called, it will nuke the {@link #scheduledExec} that is shared by all {@link PartitionResource}, filters
   * records from the buffer for partitions which will have a seek operation performed, and stops background fetch for
   * each {@link PartitionResource} to prepare for the seek. If background fetch is not currently running, the
   * {@link #scheduledExec} will not be re-created.
   */
  private void filterBufferAndResetBackgroundFetch(Set<StreamPartition<String>> partitions) throws InterruptedException
  {
    checkIfClosed();
    if (backgroundFetchEnabled && partitionsFetchStarted.compareAndSet(true, false)) {
      scheduledExec.shutdown();

      try {
        if (!scheduledExec.awaitTermination(EXCEPTION_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)) {
          scheduledExec.shutdownNow();
        }
      }
      catch (InterruptedException e) {
        log.warn(e, "InterruptedException while shutting down");
        throw e;
      }

      scheduledExec = Executors.newScheduledThreadPool(
          fetchThreads,
          Execs.makeThreadFactory("KinesisRecordSupplier-Worker-%d")
      );
    }

    // filter records in buffer and only retain ones whose partition was not seeked
    MemoryBoundLinkedBlockingQueue<OrderedPartitionableRecord<String, String, KinesisRecordEntity>> newQ =
        new MemoryBoundLinkedBlockingQueue<>(recordBufferSizeBytes);

    records.stream()
        .filter(x -> !partitions.contains(x.getData().getStreamPartition()))
        .forEachOrdered(x -> {
          if (!newQ.offer(x)) {
            // this should never really happen in practice but adding check here for safety.
            throw DruidException.defensive("Failed to insert item to new queue when resetting background fetch. "
                + "[stream: '%s', partitionId: '%s', sequenceNumber: '%s']",
                x.getData().getStream(),
                x.getData().getPartitionId(),
                x.getData().getSequenceNumber()
            );
          }
        });

    records = newQ;

    // restart fetching threads
    partitionResources.values().forEach(PartitionResource::stopBackgroundFetch);
  }
}
