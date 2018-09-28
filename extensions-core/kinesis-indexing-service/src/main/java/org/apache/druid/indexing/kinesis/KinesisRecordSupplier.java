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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxyFactory;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxyFactory;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.aws.AWSCredentialsUtils;
import org.apache.druid.indexing.kinesis.aws.ConstructibleAWSCredentialsConfig;
import org.apache.druid.indexing.seekablestream.common.Record;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class KinesisRecordSupplier implements RecordSupplier<String, String>
{
  private static final EmittingLogger log = new EmittingLogger(KinesisRecordSupplier.class);
  private static final long PROVISIONED_THROUGHPUT_EXCEEDED_BACKOFF_MS = 3000;
  private static final long EXCEPTION_RETRY_DELAY_MS = 10000;

  private class PartitionResource
  {
    private final StreamPartition<String> streamPartition;
    private final IKinesisProxy kinesisProxy;
    private final ScheduledExecutorService scheduledExec;
    private final Object startLock = new Object();

    private volatile String shardIterator;
    private volatile boolean started;
    private volatile boolean stopRequested;

    public PartitionResource(
        StreamPartition<String> streamPartition,
        IKinesisProxy kinesisProxy,
        ScheduledExecutorService scheduledExec
    )
    {
      this.streamPartition = streamPartition;
      this.kinesisProxy = kinesisProxy;
      this.scheduledExec = scheduledExec;
    }

    public void start()
    {
      synchronized (startLock) {
        if (started) {
          return;
        }

        log.info(
            "Starting scheduled fetch runnable for stream[%s] partition[%s]",
            streamPartition.getStreamName(),
            streamPartition.getPartitionId()
        );

        stopRequested = false;
        started = true;

        rescheduleRunnable(fetchDelayMillis);
      }
    }

    public void stop()
    {
      log.info(
          "Stopping scheduled fetch runnable for stream[%s] partition[%s]",
          streamPartition.getStreamName(),
          streamPartition.getPartitionId()
      );

      stopRequested = true;
    }

    private Runnable getRecordRunnable()
    {
      return () -> {
        if (stopRequested) {
          started = false;
          stopRequested = false;

          log.info("Worker for partition[%s] has been stopped", streamPartition.getPartitionId());
          return;
        }

        try {

          if (shardIterator == null) {
            log.info("shardIterator[%s] has been closed and has no more records", streamPartition.getPartitionId());

            // add an end-of-shard marker so caller knows this shard is closed
            Record<String, String> endOfShardRecord = new Record<String, String>(
                streamPartition.getStreamName(), streamPartition.getPartitionId(), Record.END_OF_SHARD_MARKER, null
            );

            if (!records.offer(endOfShardRecord, recordBufferOfferTimeout, TimeUnit.MILLISECONDS)) {
              log.warn("Record buffer full, retrying in [%,dms]", recordBufferFullWait);
              rescheduleRunnable(recordBufferFullWait);
            }

            return;
          }

          GetRecordsResult recordsResult = kinesisProxy.get(shardIterator, recordsPerFetch);

          // list will come back empty if there are no records
          for (com.amazonaws.services.kinesis.model.Record kinesisRecord : recordsResult.getRecords()) {
            final List<byte[]> data;

            if (deaggregate) {
              data = new ArrayList<>();

              final List<UserRecord> userRecords = UserRecord.deaggregate(Collections.singletonList(kinesisRecord));
              for (UserRecord userRecord : userRecords) {
                data.add(toByteArray(userRecord.getData()));
              }
            } else {
              data = Collections.singletonList(toByteArray(kinesisRecord.getData()));
            }

            final Record<String, String> record = new Record<>(
                streamPartition.getStreamName(),
                streamPartition.getPartitionId(),
                kinesisRecord.getSequenceNumber(),
                data
            );

            if (log.isTraceEnabled()) {
              log.trace(
                  "Stream[%s] / partition[%s] / sequenceNum[%s] / bufferRemainingCapacity[%d]: %s",
                  record.getStreamName(),
                  record.getPartitionId(),
                  record.getSequenceNumber(),
                  records.remainingCapacity(),
                  record.getData().stream().map(String::new).collect(Collectors.toList())
              );
            }

            // If the buffer was full and we weren't able to add the message, grab a new stream iterator starting
            // from this message and back off for a bit to let the buffer drain before retrying.
            if (!records.offer(record, recordBufferOfferTimeout, TimeUnit.MILLISECONDS)) {
              log.warn("Record buffer full, storing iterator and retrying in [%,dms]", recordBufferFullWait);

              shardIterator = kinesisProxy.getIterator(
                  record.getPartitionId(), ShardIteratorType.AT_SEQUENCE_NUMBER.toString(), record.getSequenceNumber()
              );

              rescheduleRunnable(recordBufferFullWait);
              return;
            }
          }

          shardIterator = recordsResult.getNextShardIterator(); // will be null if the shard has been closed

          rescheduleRunnable(fetchDelayMillis);
        }
        catch (ProvisionedThroughputExceededException e) {
          long retryMs = Math.max(PROVISIONED_THROUGHPUT_EXCEEDED_BACKOFF_MS, fetchDelayMillis);
          log.warn("Exceeded provisioned throughput, retrying in [%,dms]", retryMs);
          rescheduleRunnable(retryMs);
        }
        catch (Throwable e) {
          log.error(e, "getRecordRunnable exception, retrying in [%,dms]", EXCEPTION_RETRY_DELAY_MS);
          rescheduleRunnable(EXCEPTION_RETRY_DELAY_MS);
        }
      };
    }

    private void rescheduleRunnable(long delayMillis)
    {
      if (started && !stopRequested) {
        scheduledExec.schedule(getRecordRunnable(), delayMillis, TimeUnit.MILLISECONDS);
      } else {
        log.info("Worker for partition[%s] has been stopped", streamPartition.getPartitionId());
      }
    }
  }

  private final int recordsPerFetch;
  private final int fetchDelayMillis;
  private final boolean deaggregate;
  private final int recordBufferOfferTimeout;
  private final int recordBufferFullWait;
  private final int fetchSequenceNumberTimeout;

  private final IKinesisProxyFactory kinesisProxyFactory;
  private final ScheduledExecutorService scheduledExec;

  private final Map<String, IKinesisProxy> kinesisProxies = new ConcurrentHashMap<>();
  private final Map<StreamPartition<String>, PartitionResource> partitionResources = new ConcurrentHashMap<>();
  private final BlockingQueue<Record<String, String>> records;

  private volatile boolean checkPartitionsStarted = false;
  private volatile boolean closed = false;

  public KinesisRecordSupplier(
      String endpoint,
      String awsAccessKeyId,
      String awsSecretAccessKey,
      int recordsPerFetch,
      int fetchDelayMillis,
      int fetchThreads,
      String awsAssumedRoleArn,
      String awsExternalId,
      boolean deaggregate,
      int recordBufferSize,
      int recordBufferOfferTimeout,
      int recordBufferFullWait,
      int fetchSequenceNumberTimeout
  )
  {
    this.recordsPerFetch = recordsPerFetch;
    this.fetchDelayMillis = fetchDelayMillis;
    this.deaggregate = deaggregate;
    this.recordBufferOfferTimeout = recordBufferOfferTimeout;
    this.recordBufferFullWait = recordBufferFullWait;
    this.fetchSequenceNumberTimeout = fetchSequenceNumberTimeout;

    AWSCredentialsProvider awsCredentialsProvider = AWSCredentialsUtils.defaultAWSCredentialsProviderChain(
        new ConstructibleAWSCredentialsConfig(awsAccessKeyId, awsSecretAccessKey)
    );

    if (awsAssumedRoleArn != null) {
      log.info("Assuming role [%s] with externalId [%s]", awsAssumedRoleArn, awsExternalId);

      STSAssumeRoleSessionCredentialsProvider.Builder builder = new STSAssumeRoleSessionCredentialsProvider
          .Builder(awsAssumedRoleArn, String.format("druid-kinesis-%s", UUID.randomUUID().toString()))
          .withStsClient(new AWSSecurityTokenServiceClient(awsCredentialsProvider));

      if (awsExternalId != null) {
        builder.withExternalId(awsExternalId);
      }

      awsCredentialsProvider = builder.build();
    }

    AmazonKinesisClient kinesisClient = new AmazonKinesisClient(awsCredentialsProvider, new ClientConfiguration());
    kinesisClient.setEndpoint(endpoint);

    kinesisProxyFactory = new KinesisProxyFactory(awsCredentialsProvider, kinesisClient);
    records = new LinkedBlockingQueue<>(recordBufferSize);

    log.info(
        "Creating fetch thread pool of size [%d] (Runtime.availableProcessors=%d)",
        fetchThreads,
        Runtime.getRuntime().availableProcessors()
    );

    scheduledExec = Executors.newScheduledThreadPool(
        fetchThreads, Execs.makeThreadFactory("KinesisRecordSupplier-Worker-%d")
    );
  }

  @Override
  public void assign(Set<StreamPartition<String>> collection)
  {
    checkIfClosed();

    collection.stream().forEach(
        streamPartition -> partitionResources.putIfAbsent(
            streamPartition,
            new PartitionResource(streamPartition, getKinesisProxy(streamPartition.getStreamName()), scheduledExec)
        )
    );

    for (Iterator<Map.Entry<StreamPartition<String>, PartitionResource>> i = partitionResources.entrySet()
                                                                                               .iterator(); i.hasNext(); ) {
      Map.Entry<StreamPartition<String>, PartitionResource> entry = i.next();
      if (!collection.contains(entry.getKey())) {
        i.remove();
        entry.getValue().stop();
      }
    }
  }

  @Override
  public void seek(StreamPartition<String> partition, String sequenceNumber)
  {
    checkIfClosed();
    seekInternal(partition, sequenceNumber, ShardIteratorType.AT_SEQUENCE_NUMBER);
  }

  @Override
  public void seekAfter(StreamPartition<String> partition, String sequenceNumber)
  {
    checkIfClosed();
    seekInternal(partition, sequenceNumber, ShardIteratorType.AFTER_SEQUENCE_NUMBER);
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<String>> partitions)
  {
    checkIfClosed();
    partitions.forEach(partition -> seekInternal(partition, null, ShardIteratorType.TRIM_HORIZON));

  }

  @Override
  public void seekToLatest(Set<StreamPartition<String>> partitions)
  {
    checkIfClosed();
    partitions.forEach(partition -> seekInternal(partition, null, ShardIteratorType.LATEST));
  }

  @Override
  public Collection<StreamPartition<String>> getAssignment()
  {
    return null;
  }

  @Override
  public Record<String, String> poll(long timeout)
  {
    checkIfClosed();
    if (checkPartitionsStarted) {
      partitionResources.values().forEach(PartitionResource::start);
      checkPartitionsStarted = false;
    }

    try {
      while (true) {
        Record<String, String> record = records.poll(timeout, TimeUnit.MILLISECONDS);
        if (record == null || partitionResources.containsKey(record.getStreamPartition())) {
          return record;
        } else if (log.isTraceEnabled()) {
          log.trace(
              "Skipping stream[%s] / partition[%s] / sequenceNum[%s] because it is not in current assignment",
              record.getStreamName(),
              record.getPartitionId(),
              record.getSequenceNumber()
          );
        }
      }
    }
    catch (InterruptedException e) {
      log.warn(e, "InterruptedException");
      return null;
    }
  }

  @Override
  public String getLatestSequenceNumber(StreamPartition<String> partition) throws TimeoutException
  {
    checkIfClosed();
    return getSequenceNumberInternal(partition, ShardIteratorType.LATEST);
  }

  @Override
  public String getEarliestSequenceNumber(StreamPartition<String> partition) throws TimeoutException
  {
    checkIfClosed();
    return getSequenceNumberInternal(partition, ShardIteratorType.TRIM_HORIZON);
  }

  @Override
  public String position(StreamPartition<String> partition)
  {
    return null;
  }

  @Override
  public Set<String> getPartitionIds(String streamName)
  {
    checkIfClosed();
    Set<String> shardList = getKinesisProxy(streamName).getAllShardIds();
    return shardList != null ? shardList : ImmutableSet.of();
  }

  @Override
  public void close()
  {
    if (this.closed) {
      return;
    }

    assign(ImmutableSet.of());

    scheduledExec.shutdown();

    try {
      if (!scheduledExec.awaitTermination(EXCEPTION_RETRY_DELAY_MS, TimeUnit.MILLISECONDS)) {
        scheduledExec.shutdownNow();
      }
    }
    catch (InterruptedException e) {
      log.info(e, "InterruptedException while shutting down");
    }

    this.closed = true;
  }

  private IKinesisProxy getKinesisProxy(String streamName)
  {
    if (!kinesisProxies.containsKey(streamName)) {
      kinesisProxies.put(streamName, kinesisProxyFactory.getProxy(streamName));
    }

    return kinesisProxies.get(streamName);
  }

  private void seekInternal(StreamPartition<String> partition, String sequenceNumber, ShardIteratorType iteratorEnum)
  {
    PartitionResource resource = partitionResources.get(partition);
    if (resource == null) {
      throw new ISE("Partition [%s] has not been assigned", partition);
    }

    log.debug(
        "Seeking partition [%s] to [%s]",
        partition.getPartitionId(),
        sequenceNumber != null ? sequenceNumber : iteratorEnum.toString()
    );

    resource.shardIterator = getKinesisProxy(partition.getStreamName()).getIterator(
        partition.getPartitionId(), iteratorEnum.toString(), sequenceNumber
    );

    checkPartitionsStarted = true;
  }

  private String getSequenceNumberInternal(StreamPartition<String> partition, ShardIteratorType iteratorEnum)
      throws TimeoutException
  {
    long timeoutMillis = System.currentTimeMillis() + fetchSequenceNumberTimeout;
    IKinesisProxy kinesisProxy = getKinesisProxy(partition.getStreamName());
    String shardIterator = null;

    try {
      shardIterator = kinesisProxy.getIterator(partition.getPartitionId(), iteratorEnum.toString(), null);
    }
    catch (ResourceNotFoundException e) {
      log.warn("Caught ResourceNotFoundException: %s", e.getMessage());
    }

    while (shardIterator != null && System.currentTimeMillis() < timeoutMillis) {

      if (closed) {
        log.info("KinesisRecordSupplier closed while fetching sequenceNumber");
        return null;
      }

      GetRecordsResult recordsResult;
      try {
        recordsResult = kinesisProxy.get(shardIterator, 1);
      }
      catch (ProvisionedThroughputExceededException e) {
        log.warn("Exceeded provisioned throughput, retrying in [%,dms]", PROVISIONED_THROUGHPUT_EXCEEDED_BACKOFF_MS);
        try {
          Thread.sleep(PROVISIONED_THROUGHPUT_EXCEEDED_BACKOFF_MS);
          continue;
        }
        catch (InterruptedException e1) {
          log.warn(e1, "Thread interrupted!");
          Thread.currentThread().interrupt();
          break;
        }
      }

      List<com.amazonaws.services.kinesis.model.Record> records = recordsResult.getRecords();

      if (!records.isEmpty()) {
        return records.get(0).getSequenceNumber();
      }

      shardIterator = recordsResult.getNextShardIterator();
    }

    if (shardIterator == null) {
      log.info("Partition[%s] returned a null shard iterator", partition.getPartitionId());
      return null;
    }

    throw new TimeoutException(
        String.format(
            "Timeout while retrieving sequence number for partition[%s]",
            partition.getPartitionId()
        )
    );
  }

  private void checkIfClosed()
  {
    if (closed) {
      throw new ISE("Invalid operation - KinesisRecordSupplier has already been closed");
    }
  }

  /**
   * Returns an array with the content between the position and limit of "buffer". This may be the buffer's backing
   * array itself. Does not modify position or limit of the buffer.
   */
  private static byte[] toByteArray(final ByteBuffer buffer)
  {
    if (buffer.hasArray()
        && buffer.arrayOffset() == 0
        && buffer.position() == 0
        && buffer.array().length == buffer.limit()) {
      return buffer.array();
    } else {
      final byte[] retVal = new byte[buffer.remaining()];
      buffer.duplicate().get(retVal);
      return retVal;
    }
  }
}
