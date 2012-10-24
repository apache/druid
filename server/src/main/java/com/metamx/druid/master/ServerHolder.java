package com.metamx.druid.master;

import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DruidServer;

/**
 */
public class ServerHolder implements Comparable<ServerHolder>
{
  private static final Logger log = new Logger(ServerHolder.class);
  private final DruidServer server;
  private final LoadQueuePeon peon;

  public ServerHolder(
      DruidServer server,
      LoadQueuePeon peon
  )
  {
    this.server = server;
    this.peon = peon;
  }

  public DruidServer getServer()
  {
    return server;
  }

  public LoadQueuePeon getPeon()
  {
    return peon;
  }

  public Long getMaxSize()
  {
    return server.getMaxSize();
  }

  public Long getCurrServerSize()
  {
    return server.getCurrSize();
  }

  public Long getLoadQueueSize()
  {
    return peon.getLoadQueueSize();
  }

  public Long getSizeUsed()
  {
    return getCurrServerSize() + getLoadQueueSize();
  }

  public Double getPercentUsed()
  {
    return (100 * getSizeUsed().doubleValue()) / getMaxSize();
  }

  public Long getAvailableSize()
  {
    long maxSize = getMaxSize();
    long sizeUsed = getSizeUsed();
    long availableSize = maxSize - sizeUsed;

    log.debug(
        "Server[%s], MaxSize[%,d], CurrSize[%,d], QueueSize[%,d], SizeUsed[%,d], AvailableSize[%,d]",
        server.getName(),
        maxSize,
        getCurrServerSize(),
        getLoadQueueSize(),
        sizeUsed,
        availableSize
    );

    return availableSize;
  }

  @Override
  public int compareTo(ServerHolder serverHolder)
  {
    return getAvailableSize().compareTo(serverHolder.getAvailableSize());
  }
}
