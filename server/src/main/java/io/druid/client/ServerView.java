/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.client;

import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;

import java.util.concurrent.Executor;

/**
 */
public interface ServerView
{
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
    public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment);

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
    public CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment);

    public CallbackAction segmentViewInitialized();
  }

  public static abstract class BaseSegmentCallback implements SegmentCallback
  {
    @Override
    public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
    {
      return CallbackAction.CONTINUE;
    }

    @Override
    public CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
    {
      return CallbackAction.CONTINUE;
    }

    @Override
    public CallbackAction segmentViewInitialized()
    {
      return CallbackAction.CONTINUE;
    }
  }
}
