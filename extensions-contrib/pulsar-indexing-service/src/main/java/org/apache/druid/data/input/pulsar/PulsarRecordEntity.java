package org.apache.druid.data.input.pulsar;

import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.pulsar.client.api.Message;

public class PulsarRecordEntity extends ByteEntity
{
  private final Message<byte[]> message;

  public PulsarRecordEntity(Message<byte[]> message)
  {
    super(message.getValue());
    this.message = message;
  }

  public Message<byte[]> getMessage()
  {
    return message;
  }
}
