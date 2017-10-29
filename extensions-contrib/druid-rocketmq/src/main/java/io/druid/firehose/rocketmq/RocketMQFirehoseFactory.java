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
package io.druid.firehose.rocketmq;

import com.alibaba.rocketmq.client.Validators;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MessageQueueListener;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import io.druid.data.input.ByteBufferInputRowParser;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;

public class RocketMQFirehoseFactory implements FirehoseFactory<ByteBufferInputRowParser>
{

  private static final Logger LOGGER = new Logger(RocketMQFirehoseFactory.class);

  /**
   * Passed in configuration for consumer client.
   * This provides an approach to override default values defined in {@link com.alibaba.rocketmq.common.MixAll}.
   */
  @JsonProperty
  private final Properties consumerProps;

  /**
   * Consumer group. It's required.
   */
  @JsonProperty(required = true)
  private final String consumerGroup;

  /**
   * Topics to consume. It's required.
   */
  @JsonProperty(required = true)
  private final List<String> feed;

  /**
   * Pull batch size. It's optional.
   */
  @JsonProperty
  private final String pullBatchSize;

  /**
   * Store messages that are fetched from brokers but not yet delivered to druid via fire hose.
   */
  private final ConcurrentHashMap<MessageQueue, ConcurrentSkipListSet<MessageExt>> messageQueueTreeSetMap =
      new ConcurrentHashMap<>();

  /**
   * Store message consuming status.
   */
  private final ConcurrentHashMap<MessageQueue, ConcurrentSkipListSet<Long>> windows = new ConcurrentHashMap<>();

  /**
   * Default pull batch size.
   */
  private static final int DEFAULT_PULL_BATCH_SIZE = 32;

  @JsonCreator
  public RocketMQFirehoseFactory(
      @JsonProperty("consumerProps") Properties consumerProps,
      @JsonProperty("consumerGroup") String consumerGroup,
      @JsonProperty("feed") List<String> feed,
      @JsonProperty("pullBatchSize") String pullBatchSize
  )
  {
    this.consumerProps = consumerProps;
    this.pullBatchSize = pullBatchSize;
    for (Map.Entry<Object, Object> configItem : this.consumerProps.entrySet()) {
      System.setProperty(configItem.getKey().toString(), configItem.getValue().toString());
    }
    this.consumerGroup = consumerGroup;
    this.feed = feed;
  }

  /**
   * Check if there are locally pending messages to consume.
   *
   * @return true if there are some; false otherwise.
   */
  private boolean hasMessagesPending()
  {

    for (Map.Entry<MessageQueue, ConcurrentSkipListSet<MessageExt>> entry : messageQueueTreeSetMap.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public Firehose connect(
      ByteBufferInputRowParser byteBufferInputRowParser,
      File temporaryDirectory
  ) throws IOException, ParseException
  {

    Set<String> newDimExclus = Sets.union(
        byteBufferInputRowParser.getParseSpec().getDimensionsSpec().getDimensionExclusions(),
        Sets.newHashSet("feed")
    );

    final ByteBufferInputRowParser theParser = byteBufferInputRowParser.withParseSpec(
        byteBufferInputRowParser.getParseSpec()
                                .withDimensionsSpec(
                                    byteBufferInputRowParser.getParseSpec()
                                                            .getDimensionsSpec()
                                                            .withDimensionExclusions(
                                                                newDimExclus
                                                            )
                                )
    );

    /**
     * Topic-Queue mapping.
     */
    final ConcurrentHashMap<String, Set<MessageQueue>> topicQueueMap;

    /**
     * Default Pull-style client for RocketMQ.
     */
    final DefaultMQPullConsumer defaultMQPullConsumer;
    final DruidPullMessageService pullMessageService;

    messageQueueTreeSetMap.clear();
    windows.clear();

    try {
      defaultMQPullConsumer = new DefaultMQPullConsumer(this.consumerGroup);
      defaultMQPullConsumer.setMessageModel(MessageModel.CLUSTERING);
      topicQueueMap = new ConcurrentHashMap<>();

      pullMessageService = new DruidPullMessageService(defaultMQPullConsumer);
      for (String topic : feed) {
        Validators.checkTopic(topic);
        topicQueueMap.put(topic, defaultMQPullConsumer.fetchSubscribeMessageQueues(topic));
      }
      DruidMessageQueueListener druidMessageQueueListener =
          new DruidMessageQueueListener(Sets.newHashSet(feed), topicQueueMap, defaultMQPullConsumer);
      defaultMQPullConsumer.setMessageQueueListener(druidMessageQueueListener);
      defaultMQPullConsumer.start();
      pullMessageService.start();
    }
    catch (MQClientException e) {
      LOGGER.error(e, "Failed to start DefaultMQPullConsumer");
      throw new IOException("Failed to start RocketMQ client", e);
    }

    return new Firehose()
    {

      @Override
      public boolean hasMore()
      {
        boolean hasMore = false;
        DruidPullRequest earliestPullRequest = null;

        for (Map.Entry<String, Set<MessageQueue>> entry : topicQueueMap.entrySet()) {
          for (MessageQueue messageQueue : entry.getValue()) {
            ConcurrentSkipListSet<MessageExt> messages = messageQueueTreeSetMap.get(messageQueue);
            if (messages != null && !messages.isEmpty()) {
              hasMore = true;
            } else {
              try {
                long offset = defaultMQPullConsumer.fetchConsumeOffset(messageQueue, false);
                int batchSize = (null == pullBatchSize || pullBatchSize.isEmpty()) ?
                                DEFAULT_PULL_BATCH_SIZE : Integer.parseInt(pullBatchSize);

                DruidPullRequest newPullRequest = new DruidPullRequest(messageQueue, null, offset,
                                                                       batchSize, !hasMessagesPending()
                );

                // notify pull message service to pull messages from brokers.
                pullMessageService.putRequest(newPullRequest);

                // set the earliest pull in case we need to block.
                if (null == earliestPullRequest) {
                  earliestPullRequest = newPullRequest;
                }
              }
              catch (MQClientException e) {
                LOGGER.error("Failed to fetch consume offset for queue: %s", entry.getKey());
              }
            }
          }
        }

        // Block only when there is no locally pending messages.
        if (!hasMore && null != earliestPullRequest) {
          try {
            earliestPullRequest.getCountDownLatch().await();
            hasMore = true;
          }
          catch (InterruptedException e) {
            LOGGER.error(e, "CountDownLatch await got interrupted");
          }
        }
        return hasMore;
      }

      @Override
      public InputRow nextRow()
      {
        for (Map.Entry<MessageQueue, ConcurrentSkipListSet<MessageExt>> entry : messageQueueTreeSetMap.entrySet()) {
          if (!entry.getValue().isEmpty()) {
            MessageExt message = entry.getValue().pollFirst();
            InputRow inputRow = theParser.parse(ByteBuffer.wrap(message.getBody()));

            windows
                .computeIfAbsent(entry.getKey(), k -> new ConcurrentSkipListSet<>())
                .add(message.getQueueOffset());
            return inputRow;
          }
        }

        // should never happen.
        throw new RuntimeException("Unexpected Fatal Error! There should have been one row available.");
      }

      @Override
      public Runnable commit()
      {
        return new Runnable()
        {
          @Override
          public void run()
          {
            OffsetStore offsetStore = defaultMQPullConsumer.getOffsetStore();
            Set<MessageQueue> updated = new HashSet<>();
            // calculate offsets according to consuming windows.
            for (Map.Entry<MessageQueue, ConcurrentSkipListSet<Long>> entry : windows.entrySet()) {
              while (!entry.getValue().isEmpty()) {

                long offset = offsetStore.readOffset(entry.getKey(), ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                if (offset + 1 > entry.getValue().first()) {
                  entry.getValue().pollFirst();
                } else if (offset + 1 == entry.getValue().first()) {
                  entry.getValue().pollFirst();
                  offsetStore.updateOffset(entry.getKey(), offset + 1, true);
                  updated.add(entry.getKey());
                } else {
                  break;
                }

              }
            }
            offsetStore.persistAll(updated);
          }
        };
      }

      @Override
      public void close() throws IOException
      {
        defaultMQPullConsumer.shutdown();
        pullMessageService.shutdown(false);
      }
    };
  }


  /**
   * Pull request.
   */
  static final class DruidPullRequest
  {
    private final MessageQueue messageQueue;
    private final String tag;
    private final long nextBeginOffset;
    private final int pullBatchSize;
    private final boolean longPull;
    private final CountDownLatch countDownLatch;

    public DruidPullRequest(
        final MessageQueue messageQueue,
        final String tag,
        final long nextBeginOffset,
        final int pullBatchSize,
        final boolean useLongPull
    )
    {
      this.messageQueue = messageQueue;
      this.tag = (null == tag ? "*" : tag);
      this.nextBeginOffset = nextBeginOffset;
      this.pullBatchSize = pullBatchSize;
      this.longPull = useLongPull;
      countDownLatch = new CountDownLatch(1);
    }

    public MessageQueue getMessageQueue()
    {
      return messageQueue;
    }

    public long getNextBeginOffset()
    {
      return nextBeginOffset;
    }

    public String getTag()
    {
      return tag;
    }

    public int getPullBatchSize()
    {
      return pullBatchSize;
    }

    public boolean isLongPull()
    {
      return longPull;
    }

    public CountDownLatch getCountDownLatch()
    {
      return countDownLatch;
    }
  }


  /**
   * Pull message service for druid.
   * <p/>
   * <strong>Note: this is a single thread service.</strong>
   */
  final class DruidPullMessageService extends ServiceThread
  {

    private volatile List<DruidPullRequest> requestsWrite = new ArrayList<>();
    private volatile List<DruidPullRequest> requestsRead = new ArrayList<>();

    private final DefaultMQPullConsumer defaultMQPullConsumer;

    public DruidPullMessageService(final DefaultMQPullConsumer defaultMQPullConsumer)
    {
      this.defaultMQPullConsumer = defaultMQPullConsumer;
    }

    public void putRequest(final DruidPullRequest request)
    {
      synchronized (this) {
        this.requestsWrite.add(request);
        if (!hasNotified) {
          hasNotified = true;
          notify();
        }
      }
    }

    private void swapRequests()
    {
      List<DruidPullRequest> tmp = requestsWrite;
      requestsWrite = requestsRead;
      requestsRead = tmp;
    }

    @Override
    public String getServiceName()
    {
      return getClass().getSimpleName();
    }

    /**
     * Core message pulling logic code goes here.
     */
    private void doPull()
    {
      for (DruidPullRequest pullRequest : requestsRead) {
        PullResult pullResult;
        try {
          if (!pullRequest.isLongPull()) {
            pullResult = defaultMQPullConsumer.pull(
                pullRequest.getMessageQueue(),
                pullRequest.getTag(),
                pullRequest.getNextBeginOffset(),
                pullRequest.getPullBatchSize()
            );
          } else {
            pullResult = defaultMQPullConsumer.pullBlockIfNotFound(
                pullRequest.getMessageQueue(),
                pullRequest.getTag(),
                pullRequest.getNextBeginOffset(),
                pullRequest.getPullBatchSize()
            );
          }

          switch (pullResult.getPullStatus()) {
            case FOUND:
              // Handle pull result.
              messageQueueTreeSetMap
                  .computeIfAbsent(pullRequest.getMessageQueue(), k -> new ConcurrentSkipListSet<>(MESSAGE_COMPARATOR))
                  .addAll(pullResult.getMsgFoundList());
              break;

            case NO_NEW_MSG:
            case NO_MATCHED_MSG:
              break;

            case OFFSET_ILLEGAL:
              LOGGER.error(
                  "Bad Pull Request: Offset is illegal. Offset used: %d",
                  pullRequest.getNextBeginOffset()
              );
              break;

            default:
              break;
          }
        }
        catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
          LOGGER.error(e, "Failed to pull message from broker.");
        }
        finally {
          pullRequest.getCountDownLatch().countDown();
        }

      }
      requestsRead.clear();
    }

    /**
     * Thread looping entry.
     */
    @Override
    public void run()
    {
      LOGGER.info(getServiceName() + " starts.");
      while (!isStoped()) {
        waitForRunning(0);
        doPull();
      }

      // in case this service is shutdown gracefully without interruption.
      try {
        Thread.sleep(10);
      }
      catch (InterruptedException e) {
        LOGGER.error(e, "");
      }

      synchronized (this) {
        swapRequests();
      }

      doPull();
      LOGGER.info(getServiceName() + " terminated.");
    }

    @Override
    protected void onWaitEnd()
    {
      swapRequests();
    }
  }


  /**
   * Compare messages pulled from same message queue according to queue offset.
   */
  private static final Comparator<MessageExt> MESSAGE_COMPARATOR = Comparator.comparingLong(MessageExt::getQueueOffset);


  /**
   * Handle message queues re-balance operations.
   */
  final class DruidMessageQueueListener implements MessageQueueListener
  {

    private final Set<String> topics;

    private final ConcurrentHashMap<String, Set<MessageQueue>> topicQueueMap;

    private final DefaultMQPullConsumer defaultMQPullConsumer;

    public DruidMessageQueueListener(
        final Set<String> topics,
        final ConcurrentHashMap<String, Set<MessageQueue>> topicQueueMap,
        final DefaultMQPullConsumer defaultMQPullConsumer
    )
    {
      this.topics = topics;
      this.topicQueueMap = topicQueueMap;
      this.defaultMQPullConsumer = defaultMQPullConsumer;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided)
    {
      if (topics.contains(topic)) {
        topicQueueMap.put(topic, mqDivided);

        // Remove message queues that are re-assigned to other clients.
        Iterator<Map.Entry<MessageQueue, ConcurrentSkipListSet<MessageExt>>> it =
            messageQueueTreeSetMap.entrySet().iterator();
        while (it.hasNext()) {
          if (!mqDivided.contains(it.next().getKey())) {
            it.remove();
          }
        }

        StringBuilder stringBuilder = new StringBuilder();
        for (MessageQueue messageQueue : mqDivided) {
          stringBuilder.append(messageQueue.getBrokerName())
                       .append("#")
                       .append(messageQueue.getQueueId())
                       .append(", ");
        }

        if (LOGGER.isDebugEnabled() && stringBuilder.length() > 2) {
          LOGGER.debug(StringUtils.format(
              "%s@%s is consuming the following message queues: %s",
              defaultMQPullConsumer.getClientIP(),
              defaultMQPullConsumer.getInstanceName(),
              stringBuilder.substring(0, stringBuilder.length() - 2) /*Remove the trailing comma*/
          ));
        }
      }

    }
  }
}
