package org.apache.druid.k8s.discovery;

import java.io.IOException;

public class ChannelResetException extends IOException
{
  public ChannelResetException(Throwable ex) {
    super(ex);
  }
}
