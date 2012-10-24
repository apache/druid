package com.metamx.druid.client;

/**
 */
public interface MutableServerView extends ServerView
{
  public void clear();

  public void addServer(DruidServer server);

  public void removeServer(DruidServer server);

  public void serverAddedSegment(DruidServer server, DataSegment segment);

  public void serverRemovedSegment(DruidServer server, String segmentId);
}
