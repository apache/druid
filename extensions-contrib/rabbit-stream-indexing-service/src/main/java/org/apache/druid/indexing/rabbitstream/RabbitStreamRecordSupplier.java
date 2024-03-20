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

package org.apache.druid.indexing.rabbitstream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Queues;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.Client;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.rabbitstream.supervisor.RabbitStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.DynamicConfigProvider;

import javax.annotation.Nonnull;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RabbitStreamRecordSupplier implements RecordSupplier<String, Long, ByteEntity>, MessageHandler
{

  private static final EmittingLogger log = new EmittingLogger(RabbitStreamRecordSupplier.class);
  private Environment env;
  private Map<String, ConsumerBuilder> streamBuilders;
  private boolean closed;
  private BlockingQueue<OrderedPartitionableRecord<String, Long, ByteEntity>> queue;
  private String superStream;
  private String uri;
  private ObjectMapper mapper;

  private final int recordBufferOfferTimeout;
  private final int maxRecordsPerPoll;
  private final int recordBufferSize;

  private final Map<String, OffsetSpecification> offsetMap;

  private List<Consumer> consumers;
  private boolean isRunning;
  private Semaphore stateSemaphore;

  private String password;
  private String username;

  public RabbitStreamRecordSupplier(
      Map<String, Object> consumerProperties,
      ObjectMapper mapper,
      String uri,
      int recordBufferSize,
      int recordBufferOfferTimeout,
      int maxRecordsPerPoll
  )
  {
    this.uri = uri;
    this.mapper = mapper;

    this.recordBufferSize = recordBufferSize;
    this.maxRecordsPerPoll = maxRecordsPerPoll;

    this.recordBufferOfferTimeout = recordBufferOfferTimeout;

    // Messages will be added to this queue from multiple threads
    queue = new LinkedBlockingQueue<>(recordBufferSize);

    offsetMap = new ConcurrentHashMap<>();
    streamBuilders = new ConcurrentHashMap<>();

    // stateSemaphore protects isRunning and consumers
    stateSemaphore = new Semaphore(1, true);
    isRunning = false;
    consumers = new ArrayList<Consumer>();

    this.password = null;
    this.username = null;
    this.env = null;

    if (consumerProperties != null) {

      // Additional DynamicConfigProvider based extensible support for all consumer
      // properties
      Object dynamicConfigProviderJson = consumerProperties
          .get(RabbitStreamSupervisorIOConfig.DRUID_DYNAMIC_CONFIG_PROVIDER_KEY);
      if (dynamicConfigProviderJson != null) {
        DynamicConfigProvider dynamicConfigProvider = this.mapper.convertValue(dynamicConfigProviderJson,
            DynamicConfigProvider.class);
        Map<String, String> dynamicConfig = dynamicConfigProvider.getConfig();
        for (Map.Entry<String, String> e : dynamicConfig.entrySet()) {
          if (e.getKey().equals("password")) {
            this.password = e.getValue();
          }
          if (e.getKey().equals("username")) {
            this.username = e.getValue();
          }
        }
      }
    }
  }

  private void startBackgroundFetch()
  {
    try {
      // aquire uninteruptibly to prevent state corruption issues
      // on consumers and isRunning
      stateSemaphore.acquireUninterruptibly();
      if (this.isRunning != false) {
        return;
      }
      for (Map.Entry<String, ConsumerBuilder> entry : streamBuilders.entrySet()) {
        consumers.add(
            entry.getValue().offset(
                offsetMap.get(entry.getKey())).build());
      }
      this.isRunning = true;
    }
    finally {
      stateSemaphore.release();
    }
  }

  @VisibleForTesting
  public int bufferSize()
  {
    return queue.size();
  }

  @VisibleForTesting
  public boolean isRunning()
  {
    return this.isRunning;
  }

  @VisibleForTesting
  public OffsetSpecification getOffset(StreamPartition<String> partition)
  {
    return this.offsetMap.get(partition.getPartitionId());
  }

  public void stopBackgroundFetch()
  {
    try {
      stateSemaphore.acquire();
      try {
        if (this.isRunning != true) {
          return;
        }
        for (Consumer consumer : consumers) {
          consumer.close();
        }
        this.consumers.clear();
        this.isRunning = false;
      }
      finally {
        stateSemaphore.release();
      }
    }
    catch (InterruptedException exc) {
    }

  }

  public EnvironmentBuilder getEnvBuilder()
  {
    return Environment.builder();
  }

  public Environment getRabbitEnvironment()
  {
    if (this.env != null) {
      return this.env;
    }

    EnvironmentBuilder envBuilder = this.getEnvBuilder().uri(this.uri);

    if (this.password != null) {
      envBuilder = envBuilder.password(this.password);
    }
    if (this.username != null) {
      envBuilder = envBuilder.username(this.username);
    }

    this.env = envBuilder.build();
    return this.env;
  }

  public static String getStreamFromSubstream(String partionID)
  {
    String[] res = partionID.split("-");
    res = Arrays.copyOf(res, res.length - 1);
    return String.join("-", res);
  }

  private void removeOldAssignments(Set<StreamPartition<String>> streamPartitionstoKeep)
  {
    Iterator<Map.Entry<String, ConsumerBuilder>> streamBuilderIterator = streamBuilders.entrySet().iterator();
    while (streamBuilderIterator.hasNext()) {
      Map.Entry<String, ConsumerBuilder> entry = streamBuilderIterator.next();
      StreamPartition<String> comparitor = new StreamPartition<String>(getStreamFromSubstream(entry.getKey()), entry.getKey());
      if (!streamPartitionstoKeep.contains(comparitor)) {
        streamBuilderIterator.remove();
      }
    }

    Iterator<Map.Entry<String, OffsetSpecification>> offsetIterator = offsetMap.entrySet().iterator();
    while (offsetIterator.hasNext()) {
      Map.Entry<String, OffsetSpecification> entry = offsetIterator.next();
      StreamPartition<String> comparitor = new StreamPartition<String>(getStreamFromSubstream(entry.getKey()), entry.getKey());
      if (!streamPartitionstoKeep.contains(comparitor)) {
        offsetIterator.remove();
      }
    }
  }

  @Override
  public void assign(Set<StreamPartition<String>> streamPartitions)
  {
    this.stopBackgroundFetch();

    for (StreamPartition<String> part : streamPartitions) {
      ConsumerBuilder builder = this.getRabbitEnvironment().consumerBuilder().noTrackingStrategy();
      builder = builder.stream(part.getPartitionId());
      builder = builder.messageHandler(this);
      streamBuilders.put(part.getPartitionId(), builder);
      this.superStream = part.getStream();
    }

    removeOldAssignments(streamPartitions);


  }

  private void filterBufferAndResetBackgroundFetch(Set<StreamPartition<String>> partitions)
  {
    this.stopBackgroundFetch();
    // filter records in buffer and only retain ones whose partition was not seeked
    BlockingQueue<OrderedPartitionableRecord<String, Long, ByteEntity>> newQ = new LinkedBlockingQueue<>(
        recordBufferSize);

    queue.stream()
        .filter(x -> !streamBuilders.containsKey(x.getPartitionId()))
        .forEachOrdered(newQ::offer);

    queue = newQ;
  }

  @Override
  public void seek(StreamPartition<String> partition, Long sequenceNumber)
  {
    filterBufferAndResetBackgroundFetch(ImmutableSet.of(partition));
    offsetMap.put(partition.getPartitionId(), OffsetSpecification.offset(sequenceNumber));
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<String>> partitions)
  {
    filterBufferAndResetBackgroundFetch(partitions);
    for (StreamPartition<String> part : partitions) {
      offsetMap.put(part.getPartitionId(), OffsetSpecification.first());
    }
  }

  @Override
  public void seekToLatest(Set<StreamPartition<String>> partitions)
  {
    filterBufferAndResetBackgroundFetch(partitions);
    for (StreamPartition<String> part : partitions) {
      offsetMap.put(part.getPartitionId(), OffsetSpecification.last());
    }
  }

  @Override
  public Set<StreamPartition<String>> getAssignment()
  {
    return streamBuilders.keySet().stream().map(e -> StreamPartition.of(this.superStream, e))
        .collect(Collectors.toSet());
  }

  /**
   * handle can be called from any consumer, so it must be
   * as short as possible and thread safe
   */
  @Override
  public void handle(MessageHandler.Context context, Message message)
  {

    OrderedPartitionableRecord<String, Long, ByteEntity> currRecord;
    currRecord = new OrderedPartitionableRecord<>(
        this.superStream,
        context.stream(),
        context.offset(),
        ImmutableList.of(new ByteEntity(message.getBodyAsBinary())));

    try {
      if (!queue.offer(
          currRecord,
          this.recordBufferOfferTimeout,
          TimeUnit.MILLISECONDS)) {
        log.warn("Message not accepted, message buffer full");
        stopBackgroundFetch();
      } else {
        this.offsetMap.put(context.stream(), OffsetSpecification.offset(context.offset() + 1));
      }
    }
    catch (InterruptedException e) {
      // may happen if interrupted while BlockingQueue.offer() is waiting
      log.warn(
          e,
          "Interrupted while waiting to add record to buffer");
      stopBackgroundFetch();
    }

  }

  /**
   * optionalStartBackgroundFetch ensures that a background fetch is running
   * if this.queue is running low on records. We want to minimize thrashing
   * around starting/stopping the consumers.
   */
  public void optionalStartBackgroundFetch()
  {
    if (this.queue.size() < Math.min(this.maxRecordsPerPoll * 2, recordBufferSize / 2)) {
      this.startBackgroundFetch();
    }
  }

  @Nonnull
  @Override
  public List<OrderedPartitionableRecord<String, Long, ByteEntity>> poll(long timeout)
  {
    this.optionalStartBackgroundFetch();

    try {
      int expectedSize = Math.min(Math.max(queue.size(), 1), maxRecordsPerPoll);

      List<OrderedPartitionableRecord<String, Long, ByteEntity>> polledRecords = new ArrayList<>(expectedSize);

      Queues.drain(
          queue,
          polledRecords,
          expectedSize,
          timeout,
          TimeUnit.MILLISECONDS);

      polledRecords = polledRecords.stream()
          .filter(x -> streamBuilders.containsKey(x.getPartitionId()))
          .collect(Collectors.toList());

      return polledRecords;
    }
    catch (InterruptedException e) {
      log.warn(e, "Interrupted while polling");

      return Collections.emptyList();
    }
  }

  @Override
  public Long getEarliestSequenceNumber(StreamPartition<String> partition)
  {
    return this.getRabbitEnvironment().queryStreamStats(partition.getPartitionId()).firstOffset();
  }

  @Override
  public Long getLatestSequenceNumber(StreamPartition<String> partition)
  {
    return this.getRabbitEnvironment().queryStreamStats(partition.getPartitionId()).committedChunkId();
  }

  @Override
  public boolean isOffsetAvailable(StreamPartition<String> partition, OrderedSequenceNumber<Long> offset)
  {
    final Long earliestOffset = getEarliestSequenceNumber(partition);
    return earliestOffset != null
        && offset.isAvailableWithEarliest(RabbitSequenceNumber.of(earliestOffset));
  }

  @Override
  public Long getPosition(StreamPartition<String> partition)
  {
    throw new UnsupportedOperationException("getPosition() is not supported in RabbitMQ streams");
  }


  public ClientParameters getParameters()
  {
    return new ClientParameters();
  }

  public Client getClient(ClientParameters parameters)
  {
    return new Client(parameters);
  }

  @Override
  public Set<String> getPartitionIds(String stream)
  {
    ClientParameters parameters = getParameters();

    try {
      URI parsedURI = new URI(uri);
      parameters.host(parsedURI.getHost());

      if (parsedURI.getPort() != -1) {
        parameters.port(parsedURI.getPort());
      }

      if (this.password != null) {
        parameters.password(password);
      }

      if (this.username != null) {
        parameters.username(username);
      }

      Client client = getClient(parameters);

      List<String> partitions = client.partitions(stream);
      client.close();
      return new HashSet<>(partitions);
    }
    catch (URISyntaxException e) {
      throw new IllegalArgumentException("error on uri" + uri);
    }
    catch (Exception e) {
      throw e;
    }
  }

  @Override
  public void close()
  {
    if (closed) {
      return;
    }
    this.stopBackgroundFetch();
    closed = true;
  }
}
