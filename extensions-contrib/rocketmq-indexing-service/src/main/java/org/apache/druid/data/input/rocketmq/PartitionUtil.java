package org.apache.druid.data.input.rocketmq;

import org.apache.rocketmq.common.message.MessageQueue;

public class PartitionUtil
{
  private static final String PARTITION_SEPARATOR = "-queueid-";

  public static String genPartition(String brokerName, int queueId)
  {
    return brokerName + PARTITION_SEPARATOR + queueId;
  }

  public static String genPartition(MessageQueue mq)
  {
    return genPartition(mq.getBrokerName(), mq.getQueueId());
  }

  public static MessageQueue genMessageQueue(String topic, String brokerAndQueueID)
  {
    MessageQueue mq = new MessageQueue();
    String[] ss = brokerAndQueueID.split(PARTITION_SEPARATOR);
    mq.setTopic(topic);
    mq.setBrokerName(ss[0]);
    mq.setQueueId(Integer.parseInt(ss[1]));
    return mq;
  }
}
