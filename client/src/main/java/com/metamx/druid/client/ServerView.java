package com.metamx.druid.client;

import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.client.selector.ServerSelector;
import com.metamx.druid.query.QueryRunner;

import java.util.concurrent.Executor;

/**
 */
public interface ServerView
{
  public VersionedIntervalTimeline<String, ServerSelector> getTimeline(String dataSource);
  public <T> QueryRunner<T> getQueryRunner(DruidServer server);

  public void registerServerCallback(Executor exec, ServerCallback callback);
  public void registerSegmentCallback(Executor exec, SegmentCallback callback);

  public enum CallbackAction
  {
    CONTINUE,
    UNREGISTER,
  }

  public static interface ServerCallback
  {
    /**
     * Called when a server is removed.
     *
     * The return value indicates if this callback has completed its work.  Note that even if this callback
     * indicates that it should be unregistered, it is not possible to guarantee that this callback will not
     * get called again.  There is a race condition between when this callback runs and other events that can cause
     * the callback to be queued for running.  Thus, callbacks shouldn't assume that they will not get called
     * again after they are done.  The contract is that the callback will eventually be unregistered, enforcing
     * a happens-before relationship is not part of the contract.
     *
     * @param server The server that was removed.
     * @return UNREGISTER if the callback has completed its work and should be unregistered.  CONTINUE if the callback
     * should remain registered.
     */
    public CallbackAction serverRemoved(DruidServer server);
  }

  public static interface SegmentCallback
  {
    /**
     * Called when a segment is added to a server.
     *
     * The return value indicates if this callback has completed its work.  Note that even if this callback
     * indicates that it should be unregistered, it is not possible to guarantee that this callback will not
     * get called again.  There is a race condition between when this callback runs and other events that can cause
     * the callback to be queued for running.  Thus, callbacks shouldn't assume that they will not get called
     * again after they are done.  The contract is that the callback will eventually be unregistered, enforcing
     * a happens-before relationship is not part of the contract.
     *
     * @param server The server that added a segment
     * @param segment The segment that was added
     * @return UNREGISTER if the callback has completed its work and should be unregistered.  CONTINUE if the callback
     * should remain registered.
     */
    public CallbackAction segmentAdded(DruidServer server, DataSegment segment);

    /**
     * Called when a segment is removed from a server.
     *
     * The return value indicates if this callback has completed its work.  Note that even if this callback
     * indicates that it should be unregistered, it is not possible to guarantee that this callback will not
     * get called again.  There is a race condition between when this callback runs and other events that can cause
     * the callback to be queued for running.  Thus, callbacks shouldn't assume that they will not get called
     * again after they are done.  The contract is that the callback will eventually be unregistered, enforcing
     * a happens-before relationship is not part of the contract.
     *
     * @param server The server that removed a segment
     * @param segment The segment that was removed
     * @return UNREGISTER if the callback has completed its work and should be unregistered.  CONTINUE if the callback
     * should remain registered.
     */
    public CallbackAction segmentRemoved(DruidServer server, DataSegment segment);
  }

  public static abstract class BaseSegmentCallback implements SegmentCallback
  {
    @Override
    public CallbackAction segmentAdded(DruidServer server, DataSegment segment)
    {
      return CallbackAction.CONTINUE;
    }

    @Override
    public CallbackAction segmentRemoved(DruidServer server, DataSegment segment)
    {
      return CallbackAction.CONTINUE;
    }
  }
}
