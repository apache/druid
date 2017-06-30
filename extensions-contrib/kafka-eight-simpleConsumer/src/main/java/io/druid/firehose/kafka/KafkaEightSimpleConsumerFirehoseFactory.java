/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.firehose.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.metamx.common.parsers.ParseException;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Committer;
import io.druid.data.input.FirehoseFactoryV2;
import io.druid.data.input.FirehoseV2;
import io.druid.data.input.InputRow;
import io.druid.firehose.kafka.KafkaSimpleConsumer.BytesMessageWithOffset;
import io.druid.java.util.common.StringUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaEightSimpleConsumerFirehoseFactory implements
    FirehoseFactoryV2<ByteBufferInputRowParser>
{
  private static final EmittingLogger log = new EmittingLogger(
      KafkaEightSimpleConsumerFirehoseFactory.class
  );

  @JsonProperty
  private final List<String> brokerList;

  @JsonProperty
  private final List<Integer> partitionIdList;

  @JsonProperty
  private final String clientId;

  @JsonProperty
  private final String feed;

  @JsonProperty
  private final int queueBufferLength;

  @JsonProperty
  private final boolean earliest;

  private final List<PartitionConsumerWorker> consumerWorkers = new CopyOnWriteArrayList<>();
  private static final int DEFAULT_QUEUE_BUFFER_LENGTH = 20000;
  private static final int CONSUMER_FETCH_TIMEOUT = 10000;

  @JsonCreator
  public KafkaEightSimpleConsumerFirehoseFactory(
      @JsonProperty("brokerList") List<String> brokerList,
      @JsonProperty("partitionIdList") List<Integer> partitionIdList,
      @JsonProperty("clientId") String clientId,
      @JsonProperty("feed") String feed,
      @JsonProperty("queueBufferLength") Integer queueBufferLength,
      @JsonProperty("resetOffsetToEarliest") Boolean resetOffsetToEarliest
  )
  {
    this.brokerList = brokerList;
    Preconditions.checkArgument(
        brokerList != null && brokerList.size() > 0,
        "brokerList is null/empty"
    );

    this.partitionIdList = partitionIdList;
    Preconditions.checkArgument(
        partitionIdList != null && partitionIdList.size() > 0,
        "partitionIdList is null/empty"
    );


    this.clientId = clientId;
    Preconditions.checkArgument(
        clientId != null && !clientId.isEmpty(),
        "clientId is null/empty"
    );

    this.feed = feed;
    Preconditions.checkArgument(
        feed != null && !feed.isEmpty(),
        "feed is null/empty"
    );

    this.queueBufferLength = queueBufferLength == null ? DEFAULT_QUEUE_BUFFER_LENGTH : queueBufferLength;
    Preconditions.checkArgument(this.queueBufferLength > 0, "queueBufferLength must be positive number");
    log.info("queueBufferLength loaded as[%s]", this.queueBufferLength);

    this.earliest = resetOffsetToEarliest == null ? true : resetOffsetToEarliest.booleanValue();
    log.info(
        "if old offsets are not known, data from partition will be read from [%s] available offset.",
        this.earliest ? "earliest" : "latest"
    );
  }

  private Map<Integer, Long> loadOffsetFromPreviousMetaData(Object lastCommit)
  {
    Map<Integer, Long> offsetMap = Maps.newHashMap();
    if (lastCommit == null) {
      return offsetMap;
    }
    if (lastCommit instanceof Map) {
      Map<Object, Object> lastCommitMap = (Map) lastCommit;
      for (Map.Entry<Object, Object> entry : lastCommitMap.entrySet()) {
        try {
          int partitionId = Integer.parseInt(entry.getKey().toString());
          long offset = Long.parseLong(entry.getValue().toString());
          log.debug("Recover last commit information partitionId [%s], offset [%s]", partitionId, offset);
          offsetMap.put(partitionId, offset);
        }
        catch (NumberFormatException e) {
          log.error(e, "Fail to load offset from previous meta data [%s]", entry);
        }
      }
      log.info("Loaded offset map[%s]", offsetMap);
    } else {
      log.makeAlert("Unable to cast lastCommit to Map for feed [%s]", feed);
    }
    return offsetMap;
  }

  @Override
  public FirehoseV2 connect(final ByteBufferInputRowParser firehoseParser, Object lastCommit) throws IOException
  {
    final Map<Integer, Long> lastOffsets = loadOffsetFromPreviousMetaData(lastCommit);

    for (Integer partition : partitionIdList) {
      final KafkaSimpleConsumer kafkaSimpleConsumer = new KafkaSimpleConsumer(
          feed, partition, clientId, brokerList, earliest
      );
      Long startOffset = lastOffsets.get(partition);
      PartitionConsumerWorker worker = new PartitionConsumerWorker(
          feed, kafkaSimpleConsumer, partition, startOffset == null ? -1 : startOffset
      );
      consumerWorkers.add(worker);
    }

    final LinkedBlockingQueue<BytesMessageWithOffset> messageQueue = new LinkedBlockingQueue<BytesMessageWithOffset>(
        queueBufferLength
    );
    log.info("Kicking off all consumers");
    for (PartitionConsumerWorker worker : consumerWorkers) {
      worker.go(messageQueue);
    }
    log.info("All consumer started");

    return new FirehoseV2()
    {
      private Map<Integer, Long> lastOffsetPartitions;
      private volatile boolean stopped;
      private volatile BytesMessageWithOffset msg = null;
      private volatile InputRow row = null;

      {
        lastOffsetPartitions = Maps.newHashMap();
        lastOffsetPartitions.putAll(lastOffsets);
      }

      @Override
      public void start() throws Exception
      {
      }

      @Override
      public boolean advance()
      {
        if (stopped) {
          return false;
        }

        nextMessage();
        return true;
      }

      private void nextMessage()
      {
        try {
          row = null;
          while (row == null) {
            if (msg != null) {
              lastOffsetPartitions.put(msg.getPartition(), msg.offset());
            }

            msg = messageQueue.take();

            final byte[] message = msg.message();
            row = message == null ? null : firehoseParser.parse(ByteBuffer.wrap(message));
          }
        }
        catch (InterruptedException e) {
          //Let the caller decide whether to stop or continue when thread is interrupted.
          log.warn(e, "Thread Interrupted while taking from queue, propagating the interrupt");
          Thread.currentThread().interrupt();
        }
      }

      @Override
      public InputRow currRow()
      {
        if (stopped) {
          return null;
        }
        // currRow will be called before the first advance
        if (row == null) {
          try {
            nextMessage();
          }
          catch (ParseException e) {
            return null;
          }
        }
        return row;
      }

      @Override
      public Committer makeCommitter()
      {
        final Map<Integer, Long> offsets = Maps.newHashMap(lastOffsetPartitions);

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return offsets;
          }

          @Override
          public void run()
          {

          }
        };
      }

      @Override
      public void close() throws IOException
      {
        log.info("Stopping kafka 0.8 simple firehose");
        stopped = true;
        for (PartitionConsumerWorker t : consumerWorkers) {
          Closeables.close(t, true);
        }
      }
    };
  }

  private static class PartitionConsumerWorker implements Closeable
  {
    private final String topic;
    private final KafkaSimpleConsumer consumer;
    private final int partitionId;
    private final long startOffset;

    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private volatile Thread thread = null;

    PartitionConsumerWorker(String topic, KafkaSimpleConsumer consumer, int partitionId, long startOffset)
    {
      this.topic = topic;
      this.consumer = consumer;
      this.partitionId = partitionId;
      this.startOffset = startOffset;
    }

    public void go(final LinkedBlockingQueue<BytesMessageWithOffset> messageQueue)
    {
      thread = new Thread()
      {
        @Override
        public void run()
        {
          long offset = startOffset;
          log.info("Start running parition[%s], offset[%s]", partitionId, offset);
          try {
            while (!stopped.get()) {
              try {
                Iterable<BytesMessageWithOffset> msgs = consumer.fetch(offset, CONSUMER_FETCH_TIMEOUT);
                int count = 0;
                for (BytesMessageWithOffset msgWithOffset : msgs) {
                  offset = msgWithOffset.offset();
                  messageQueue.put(msgWithOffset);
                  count++;
                }
                log.debug("fetch [%s] msgs for partition [%s] in one time ", count, partitionId);
              }
              catch (InterruptedException e) {
                log.info("Interrupted when fetching data, shutting down.");
                return;
              }
              catch (Exception e) {
                log.error(e, "Exception happened in fetching data, but will continue consuming");
              }
            }
          }
          finally {
            consumer.stop();
          }
        }
      };
      thread.setDaemon(true);
      thread.setName(StringUtils.format("kafka-%s-%s", topic, partitionId));
      thread.start();
    }

    @Override
    public synchronized void close() throws IOException
    {
      if (stopped.compareAndSet(false, true)) {
        thread.interrupt();
        thread = null;
      }
    }
  }
}
