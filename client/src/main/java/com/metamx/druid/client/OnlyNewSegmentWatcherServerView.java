package com.metamx.druid.client;

import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.client.selector.ServerSelector;
import com.metamx.druid.query.QueryRunner;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 */
public class OnlyNewSegmentWatcherServerView implements MutableServerView
{
  private static final Logger log = new Logger(OnlyNewSegmentWatcherServerView.class);

  private final ConcurrentMap<SegmentCallback, Executor> segmentCallbacks;

  public OnlyNewSegmentWatcherServerView()
  {
    this.segmentCallbacks = Maps.newConcurrentMap();
  }

  @Override
  public void clear()
  {

  }

  @Override
  public void addServer(DruidServer server)
  {
  }

  @Override
  public void removeServer(DruidServer server)
  {
  }

  @Override
  public void serverAddedSegment(DruidServer server, DataSegment segment)
  {
    runSegmentCallbacks(server, segment);
  }

  @Override
  public void serverRemovedSegment(DruidServer server, String segmentId)
  {
  }

  @Override
  public VersionedIntervalTimeline<String, ServerSelector> getTimeline(String dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryRunner getQueryRunner(DruidServer server)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerServerCallback(Executor exec, ServerCallback callback)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    segmentCallbacks.put(callback, exec);
  }

  private void runSegmentCallbacks(final DruidServer server, final DataSegment segment)
  {
    Iterator<Map.Entry<SegmentCallback, Executor>> iter = segmentCallbacks.entrySet().iterator();

    while (iter.hasNext()) {
      final Map.Entry<SegmentCallback, Executor> entry = iter.next();
      entry.getValue().execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              if (CallbackAction.UNREGISTER == entry.getKey().segmentAdded(server, segment)) {
                segmentCallbacks.remove(entry.getKey());
              }
            }
          }
      );
    }
  }
}
