/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.client;

import org.apache.druid.common.utils.Action;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

/**
 */
public interface ServerView
{
  void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback);
  void registerSegmentCallback(Executor exec, SegmentCallback callback);

  enum CallbackAction
  {
    CONTINUE,
    UNREGISTER,
  }

  interface ServerRemovedCallback
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
    CallbackAction serverRemoved(DruidServer server);
  }

  interface SegmentCallback
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
    CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment);

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
    CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment);

    CallbackAction segmentViewInitialized();
  }

  /**
   * Creates a {@code SegmentCallback} with the given actions. All the callback
   * methods of the created {@code SegmentCallback} always return
   * {@link CallbackAction#CONTINUE}. The actions that need not be handled can
   * be passed as null.
   */
  static SegmentCallback perpetualSegmentCallback(
      @Nullable BiConsumer<DruidServerMetadata, DataSegment> addAction,
      @Nullable BiConsumer<DruidServerMetadata, DataSegment> removeAction,
      @Nullable Action initializedAction
  )
  {
    return new SegmentCallback()
    {
      @Override
      public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
      {
        if (addAction != null) {
          addAction.accept(server, segment);
        }
        return CallbackAction.CONTINUE;
      }

      @Override
      public CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
      {
        if (removeAction != null) {
          removeAction.accept(server, segment);
        }
        return CallbackAction.CONTINUE;
      }

      @Override
      public CallbackAction segmentViewInitialized()
      {
        if (initializedAction != null) {
          initializedAction.perform();
        }
        return CallbackAction.CONTINUE;
      }
    };
  }

  /**
   * Creates a new {@code SegmentCallback} which wraps the given callback.
   * For every callback method, the original callback is invoked and then the
   * corresponding {@code afterXXX} action is executed.
   */
  static SegmentCallback performAfterSegmentCallback(
      SegmentCallback callback,
      @Nullable BiConsumer<DruidServerMetadata, DataSegment> afterAddAction,
      @Nullable BiConsumer<DruidServerMetadata, DataSegment> afterRemoveAction,
      @Nullable Action afterInitializeAction
  )
  {
    return new SegmentCallback()
    {
      @Override
      public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
      {
        CallbackAction result = callback.segmentAdded(server, segment);
        if (afterAddAction != null) {
          afterAddAction.accept(server, segment);
        }
        return result;
      }

      @Override
      public CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
      {
        CallbackAction result = callback.segmentAdded(server, segment);
        if (afterRemoveAction != null) {
          afterRemoveAction.accept(server, segment);
        }
        return result;
      }

      @Override
      public CallbackAction segmentViewInitialized()
      {
        CallbackAction result = callback.segmentViewInitialized();
        if (afterInitializeAction != null) {
          afterInitializeAction.perform();
        }
        return result;
      }
    };
  }

  abstract class BaseSegmentCallback implements SegmentCallback
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
