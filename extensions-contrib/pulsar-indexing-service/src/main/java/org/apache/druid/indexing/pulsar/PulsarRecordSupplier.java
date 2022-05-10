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

package org.apache.druid.indexing.pulsar;

import com.google.common.annotations.VisibleForTesting;
import io.vavr.Function3;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderListener;
import org.apache.pulsar.common.naming.TopicName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Encapsulates fetching records from pulsar
 */
public class PulsarRecordSupplier implements RecordSupplier<Integer, Long, ByteEntity>, ReaderListener<byte[]>
{

  private static final EmittingLogger log = new EmittingLogger(PulsarRecordSupplier.class);
  protected static Function3<PulsarClient, String, PulsarRecordSupplier, CompletableFuture<Reader<byte[]>>> buildConsumer =
      (PulsarClient client, String topic, PulsarRecordSupplier recordSupplier) -> {
        String readerName = topic + "-reader";


        return client.newReader()
            .readerListener(recordSupplier)
            .readerName(readerName)
            .topic(topic)
            .startMessageId(MessageId.latest)
            .startMessageIdInclusive()
            .createAsync();
      };
  protected final ConcurrentHashMap<StreamPartition<Integer>, CursorContainer> readerPartitions =
      new ConcurrentHashMap<>();
  protected final PulsarClient client;
  protected final String readerName;
  private final BlockingQueue<Message<byte[]>> received;
  private final Integer maxRecordsInSinglePoll;
  private final Function3<PulsarClient, String, PulsarRecordSupplier, CompletableFuture<Reader<byte[]>>> consumerBuilder;
  private PulsarClientException previousSeekFailure;

  public PulsarRecordSupplier(String serviceUrl, String readerName, Integer maxRecordsInSinglePoll)
  {
    try {
      this.readerName = readerName;
      this.maxRecordsInSinglePoll = maxRecordsInSinglePoll;
      this.received = new ArrayBlockingQueue<>(this.maxRecordsInSinglePoll);
      this.consumerBuilder = buildConsumer;

      client = PulsarClient.builder().serviceUrl(serviceUrl).build();
    } catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  public PulsarRecordSupplier(
      String serviceUrl, String readerName, Integer maxRecordsInSinglePoll, PulsarClient client,
      Function3<PulsarClient, String, PulsarRecordSupplier, CompletableFuture<Reader<byte[]>>> buildConsumer,
      BlockingQueue<Message<byte[]>> received
  )
  {
    this.readerName = readerName;
    this.maxRecordsInSinglePoll = maxRecordsInSinglePoll;
    this.received = received;
    this.client = client;
    this.consumerBuilder = buildConsumer;
  }

  private static <T> T wrapExceptions(Callable<T> callable)
  {
    try {
      return callable.call();
    } catch (Exception e) {
      throw new StreamException(e);
    }
  }

  private static void wrapExceptions(Runnable runnable)
  {
    wrapExceptions(() -> {
      runnable.run();
      return null;
    });
  }

  @Override
  public void assign(Set<StreamPartition<Integer>> streamPartitions)
  {
    List<CompletableFuture<Reader<byte[]>>> futures = new ArrayList<>();
    log.info("Assigning partitions: " + streamPartitions);

    try {
      for (StreamPartition<Integer> partition : streamPartitions) {
        if (readerPartitions.containsKey(partition)) {
          continue;
        }

        String topic = TopicName.get(partition.getStream())
            .getPartition(partition.getPartitionId())
            .toString();

        futures.add(consumerBuilder.apply(client, topic, this).thenApplyAsync(reader -> {
          if (readerPartitions.containsKey(partition)) {
            reader.closeAsync();
          } else {
            // start reader at LATEST position
            readerPartitions.put(partition, new CursorContainer(reader, MessageId.earliest));
          }
          return reader;
        }));
      }

      futures.forEach(CompletableFuture::join);
    } catch (Exception e) {
      futures.forEach(f -> {
        try {
          f.get().closeAsync();
        } catch (Exception ignored) {
          // ignore
        }
      });
      throw new StreamException(e);
    }
    log.info("Successfully assigned: " + streamPartitions);
  }

  @Override
  public void seek(StreamPartition<Integer> partition, Long sequenceNumber) throws InterruptedException
  {
    CursorContainer reader = readerPartitions.get(partition);
    if (reader == null) {
      String err =
          String.format(Locale.ENGLISH, "Cannot seek on a partition [%s] where we are not assigned", partition);
      throw new IllegalArgumentException();
    }

    try {
      // update our partitions
      reader.reader.seek(PulsarSequenceNumber.of(sequenceNumber).getMessageId());
      //
      final MessageId messageId = PulsarSequenceNumber.of(sequenceNumber).getMessageId();
      setPosition(partition, messageId);
      previousSeekFailure = null;
    } catch (PulsarClientException e) {
      previousSeekFailure = e;
    }
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<Integer>> streamPartitions) throws InterruptedException
  {
    streamPartitions.forEach(p -> {
      try {
        seek(p, PulsarSequenceNumber.EARLIEST_OFFSET);
      } catch (InterruptedException e) {
        throw new StreamException(e);
      }
    });
  }

  @Override
  public void seekToLatest(Set<StreamPartition<Integer>> streamPartitions) throws InterruptedException
  {
    streamPartitions.forEach(p -> {
      try {
        seek(p, PulsarSequenceNumber.LATEST_OFFSET);
      } catch (InterruptedException e) {
        throw new StreamException(e);
      }
    });
  }

  @Override
  public Collection<StreamPartition<Integer>> getAssignment()
  {
    return this.readerPartitions.keySet();
  }

  @Nonnull
  @Override
  public List<OrderedPartitionableRecord<Integer, Long, ByteEntity>> poll(long timeout)
  {
    try {
      List<OrderedPartitionableRecord<Integer, Long, ByteEntity>> records = new ArrayList<>();


      Message<byte[]> item = received.poll(timeout, TimeUnit.MILLISECONDS);
      if (item == null) {
        return records;
      }

      int numberOfRecords = 0;

      while (item != null) {
        StreamPartition<Integer> sp = getStreamPartitionFromMessage(item);
        PulsarSequenceNumber offset = PulsarSequenceNumber.of(item.getMessageId());

        records.add(new OrderedPartitionableRecord<Integer, Long, ByteEntity>(
            sp.getStream(),
            sp.getPartitionId(),
            offset.get(),
            Collections.singletonList(new ByteEntity(item.getValue()))
        ));

        if (++numberOfRecords >= maxRecordsInSinglePoll) {
          break;
        }

        // Check if we have an item already available
        item = received.poll(0, TimeUnit.MILLISECONDS);
      }

      return records;
    } catch (InterruptedException e) {
      throw new StreamException(e);
    }
  }

  @Nullable
  @Override
  public Long getLatestSequenceNumber(StreamPartition<Integer> partition)
  {
    return PulsarSequenceNumber.LATEST_OFFSET;
  }

  @Nullable
  @Override
  public Long getEarliestSequenceNumber(StreamPartition<Integer> partition)
  {
    return PulsarSequenceNumber.EARLIEST_OFFSET;
  }

  @Override
  public Long getPosition(StreamPartition<Integer> partition)
  {
    CursorContainer reader = readerPartitions.get(partition);
    if (reader == null) {
      throw new IllegalArgumentException(
          String.format(Locale.ENGLISH, "Cannot seek on a partition [%s] where we are not assigned", partition));
    }
    return PulsarSequenceNumber.of(reader.position).get();
  }

  @Override
  public Set<Integer> getPartitionIds(String stream)
  {
    try {
      return client.getPartitionsForTopic(stream).get().stream()
          .map(TopicName::get)
          .map(TopicName::getPartitionIndex)
          .collect(Collectors.toSet());
    } catch (Exception e) {
      throw new StreamException(e);
    }
  }

  @Override
  public void close()
  {
    readerPartitions.forEach((k, r) -> r.reader.closeAsync());
    client.closeAsync();
  }

  public PulsarClientException getPreviousSeekFailure()
  {
    return previousSeekFailure;
  }

  void setPosition(StreamPartition<Integer> partition, MessageId position)
  {
    CursorContainer reader = this.readerPartitions.get(partition);
    if (reader != null) {
      reader.position = position;
    }
  }

  private StreamPartition<Integer> getStreamPartitionFromMessage(Message<byte[]> msg)
  {
    TopicName topic = TopicName.get(msg.getTopicName());
    return new StreamPartition<>(topic.getPartitionedTopicName(), topic.getPartitionIndex());
  }

  @Override
  public void received(Reader<byte[]> reader, Message<byte[]> msg)
  {
    try {
      this.received.put(msg);
    } catch (InterruptedException e) {
      throw new StreamException(e);
    }
  }

  @Override
  public void reachedEndOfTopic(Reader<byte[]> reader)
  {
    ReaderListener.super.reachedEndOfTopic(reader);
  }

  /*
    Container class to hold contents of a pulsar message
   */
  public static class CursorContainer
  {
    public Reader<byte[]> reader;
    public MessageId position;

    public CursorContainer(Reader<byte[]> reader, MessageId position)
    {
      this.reader = reader;
      this.position = position;
    }
  }
}
