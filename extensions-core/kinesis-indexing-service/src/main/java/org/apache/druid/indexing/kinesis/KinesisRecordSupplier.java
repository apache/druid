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

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.util.AwsHostNameUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.druid.common.aws.AWSClientUtil;
import org.apache.druid.common.aws.AWSCredentialsConfig;
import org.apache.druid.common.aws.AWSCredentialsUtils;
import org.apache.druid.data.input.impl.ByteEntity;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
public class KinesisRecordSupplier implements RecordSupplier<String, String, ByteEntity>
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
        GetRecordsResult recordsResult = null;
        OrderedPartitionableRecord<String, String, ByteEntity> currRecord;
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

            recordsResult = null;

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

          recordsResult = kinesis.getRecords(new GetRecordsRequest().withShardIterator(shardIterator));

          currentLagMillis = recordsResult.getMillisBehindLatest();

          // list will come back empty if there are no records
          for (Record kinesisRecord : recordsResult.getRecords()) {
            final List<ByteEntity> data;

            if (deaggregateHandle == null || getDataHandle == null) {
              throw new ISE("deaggregateHandle or getDataHandle is null!");
            }

            data = new ArrayList<>();

            final List userRecords = (List) deaggregateHandle.invokeExact(
                Collections.singletonList(kinesisRecord)
            );

            int recordSize = 0;
            for (Object userRecord : userRecords) {
              ByteEntity byteEntity = new ByteEntity((ByteBuffer) getDataHandle.invoke(userRecord));
              recordSize += byteEntity.getBuffer().array().length;
              data.add(byteEntity);
            }


            currRecord = new OrderedPartitionableRecord<>(
                streamPartition.getStream(),
                streamPartition.getPartitionId(),
                kinesisRecord.getSequenceNumber(),
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

          shardIterator = recordsResult.getNextShardIterator(); // will be null if the shard has been closed

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
            shardIterator = recordsResult.getNextShardIterator(); // will be null if the shard has been closed
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
        catch (AmazonClientException e) {
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

      shardIterator = wrapExceptions(() -> kinesis.getShardIterator(
          streamPartition.getStream(),
          streamPartition.getPartitionId(),
          iteratorEnum.toString(),
          sequenceNumber
      ).getShardIterator());
    }

    private long getPartitionTimeLag()
    {
      return currentLagMillis;
    }
  }

  // used for deaggregate
  private final MethodHandle deaggregateHandle;
  private final MethodHandle getDataHandle;

  private final AmazonKinesis kinesis;
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
  private MemoryBoundLinkedBlockingQueue<OrderedPartitionableRecord<String, String, ByteEntity>> records;

  private final boolean backgroundFetchEnabled;
  private volatile boolean closed = false;
  private AtomicBoolean partitionsFetchStarted = new AtomicBoolean();

  public KinesisRecordSupplier(
      AmazonKinesis amazonKinesis,
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
    Preconditions.checkNotNull(amazonKinesis);
    this.kinesis = amazonKinesis;
    this.fetchDelayMillis = fetchDelayMillis;
    this.recordBufferOfferTimeout = recordBufferOfferTimeout;
    this.recordBufferFullWait = recordBufferFullWait;
    this.maxBytesPerPoll = maxBytesPerPoll;
    this.fetchThreads = fetchThreads;
    this.recordBufferSizeBytes = recordBufferSizeBytes;
    this.useEarliestSequenceNumber = useEarliestSequenceNumber;
    this.useListShards = useListShards;
    this.backgroundFetchEnabled = fetchThreads > 0;

    // The deaggregate function is implemented by the amazon-kinesis-client, whose license was formerly not compatible
    // with Apache. The code here avoids the license issue by using reflection, but is no longer necessary since
    // amazon-kinesis-client is now Apache-licensed and is now a dependency of Druid. This code could safely be
    // modified to use regular calls rather than reflection.
    try {
      Class<?> kclUserRecordclass = Class.forName("com.amazonaws.services.kinesis.clientlibrary.types.UserRecord");
      MethodHandles.Lookup lookup = MethodHandles.publicLookup();

      Method deaggregateMethod = kclUserRecordclass.getMethod("deaggregate", List.class);
      Method getDataMethod = kclUserRecordclass.getMethod("getData");

      deaggregateHandle = lookup.unreflect(deaggregateMethod);
      getDataHandle = lookup.unreflect(getDataMethod);
    }
    catch (ClassNotFoundException e) {
      throw new ISE(e, "cannot find class[com.amazonaws.services.kinesis.clientlibrary.types.UserRecord], "
                       + "note that when using deaggregate=true, you must provide the Kinesis Client Library jar in the classpath");
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

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

  public static AmazonKinesis getAmazonKinesisClient(
      String endpoint,
      AWSCredentialsConfig awsCredentialsConfig,
      String awsAssumedRoleArn,
      String awsExternalId
  )
  {
    AWSCredentialsProvider awsCredentialsProvider = AWSCredentialsUtils.defaultAWSCredentialsProviderChain(
        awsCredentialsConfig
    );

    if (awsAssumedRoleArn != null) {
      log.info("Assuming role [%s] with externalId [%s]", awsAssumedRoleArn, awsExternalId);

      STSAssumeRoleSessionCredentialsProvider.Builder builder = new STSAssumeRoleSessionCredentialsProvider
          .Builder(awsAssumedRoleArn, StringUtils.format("druid-kinesis-%s", UUID.randomUUID().toString()))
          .withStsClient(AWSSecurityTokenServiceClientBuilder.standard()
                                                             .withCredentials(awsCredentialsProvider)
                                                             .build());

      if (awsExternalId != null) {
        builder.withExternalId(awsExternalId);
      }

      awsCredentialsProvider = builder.build();
    }

    return AmazonKinesisClientBuilder.standard()
                                     .withCredentials(awsCredentialsProvider)
                                     .withClientConfiguration(new ClientConfiguration())
                                     .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                                         endpoint,
                                         AwsHostNameUtils.parseRegion(
                                             endpoint,
                                             null
                                         )
                                     )).build();
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
  public List<OrderedPartitionableRecord<String, String, ByteEntity>> poll(long timeout)
  {
    start();

    try {
      List<MemoryBoundLinkedBlockingQueue.ObjectContainer<OrderedPartitionableRecord<String, String, ByteEntity>>> polledRecords = new ArrayList<>();

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
          () -> kinesis.getShardIterator(partition.getStream(),
                                         partition.getPartitionId(),
                                         ShardIteratorType.AT_SEQUENCE_NUMBER.name(),
                                         kinesisSequence.get())
                       .getShardIterator(),
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
            if (throwable instanceof AmazonClientException) {
              AmazonClientException ase = (AmazonClientException) throwable;
              return AWSClientUtil.isClientExceptionRecoverable(ase);
            }
            return false;
          },
          GET_SEQUENCE_NUMBER_RETRY_COUNT
      );
      GetRecordsRequest getRecordsRequest = new GetRecordsRequest().withShardIterator(shardIterator);
      List<Record> records = RetryUtils.retry(
          () -> kinesis.getRecords(getRecordsRequest)
                       .getRecords(),
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
            if (throwable instanceof AmazonClientException) {
              AmazonClientException ase = (AmazonClientException) throwable;
              return AWSClientUtil.isClientExceptionRecoverable(ase);
            }
            return false;
          },
          GET_SEQUENCE_NUMBER_RETRY_COUNT
      );
      return !records.isEmpty() && records.get(0).getSequenceNumber().equals(kinesisSequence.get());
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
    DescribeStreamRequest describeRequest = new DescribeStreamRequest();
    describeRequest.setStreamName(stream);
    while (describeRequest != null) {
      StreamDescription description = kinesis.describeStream(describeRequest).getStreamDescription();
      List<Shard> shardResult = description.getShards();
      shards.addAll(shardResult);
      if (description.isHasMoreShards()) {
        describeRequest.setExclusiveStartShardId(Iterables.getLast(shardResult).getShardId());
      } else {
        describeRequest = null;
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
    ListShardsRequest request = new ListShardsRequest().withStreamName(stream);
    while (true) {
      ListShardsResult result = kinesis.listShards(request);
      shards.addAll(result.getShards());
      String nextToken = result.getNextToken();
      if (nextToken == null) {
        return shards.build();
      }
      request = new ListShardsRequest().withNextToken(nextToken);
    }
  }

  @Override
  public Set<String> getPartitionIds(String stream)
  {
    return wrapExceptions(() -> {
      Set<String> partitionIds = new TreeSet<>();
      for (Shard shard : getShards(stream)) {
        partitionIds.add(shard.getShardId());
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
   *
   * When there are no records at the offset corresponding to the ShardIteratorType,
   *    If shard is closed, return custom EOS sequence marker
   *    While getting the earliest sequence number, return a custom marker corresponding to TRIM_HORIZON
   *    While getting the most recent sequence number, return a custom marker corresponding to LATEST
   */
  @Nullable
  private String getSequenceNumber(StreamPartition<String> partition, ShardIteratorType iteratorEnum)
  {
    return wrapExceptions(() -> {
      String shardIterator =
          kinesis.getShardIterator(partition.getStream(), partition.getPartitionId(), iteratorEnum.toString())
                 .getShardIterator();

      if (closed) {
        log.info("KinesisRecordSupplier closed while fetching sequenceNumber");
        return null;
      }
      final GetRecordsRequest request = new GetRecordsRequest().withShardIterator(shardIterator)
                                                               .withLimit(GET_SEQUENCE_NUMBER_RECORD_COUNT);
      GetRecordsResult recordsResult = RetryUtils.retry(
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
            if (throwable instanceof AmazonClientException) {
              AmazonClientException ase = (AmazonClientException) throwable;
              return AWSClientUtil.isClientExceptionRecoverable(ase);
            }
            return false;
          },
          GET_SEQUENCE_NUMBER_RETRY_COUNT
      );

      List<Record> records = recordsResult.getRecords();

      if (!records.isEmpty()) {
        return records.get(0).getSequenceNumber();
      }

      if (recordsResult.getNextShardIterator() == null) {
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
   * in order to get the lag: {@link GetRecordsResult#getMillisBehindLatest()}. This method is thread safe as it does
   * not depend on the internal state of the supplier (it doesn't use the {@link PartitionResource} which have been
   * assigned to the supplier), and the Kinesis client is thread safe.
   */
  private Long getPartitionTimeLag(StreamPartition<String> partition, String offset)
  {
    return wrapExceptions(() -> {
      final String iteratorType;
      final String offsetToUse;
      if (offset == null || KinesisSupervisor.OFFSET_NOT_SET.equals(offset)) {
        if (useEarliestSequenceNumber) {
          iteratorType = ShardIteratorType.TRIM_HORIZON.toString();
          offsetToUse = null;
        } else {
          // if offset is not set and not using earliest, it means we will start reading from latest,
          // so lag will be 0 and we have nothing to do here
          return 0L;
        }
      } else {
        iteratorType = ShardIteratorType.AT_SEQUENCE_NUMBER.toString();
        offsetToUse = offset;
      }

      GetRecordsResult recordsResult = getRecordsForLag(ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), offsetToUse, partition);

      // If no more new data after offsetToUse, it means there is no lag for now.
      // So report lag points as 0L.
      if (recordsResult.getRecords().size() == 0) {
        return 0L;
      } else {
        recordsResult = getRecordsForLag(iteratorType, offsetToUse, partition);
      }

      return recordsResult.getMillisBehindLatest();
    });
  }

  private GetRecordsResult getRecordsForLag(String iteratorType, String offsetToUse, StreamPartition<String> partition)
  {
    String shardIterator = kinesis.getShardIterator(
            partition.getStream(),
            partition.getPartitionId(),
            iteratorType,
            offsetToUse
    ).getShardIterator();

    GetRecordsResult recordsResult = kinesis.getRecords(
            new GetRecordsRequest().withShardIterator(shardIterator).withLimit(1)
    );
    return recordsResult;
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
   *
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
    MemoryBoundLinkedBlockingQueue<OrderedPartitionableRecord<String, String, ByteEntity>> newQ =
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
    partitionResources.values().forEach(x -> x.stopBackgroundFetch());
  }
}
