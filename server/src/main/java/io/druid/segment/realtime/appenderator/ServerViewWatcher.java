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

package io.druid.segment.realtime.appenderator;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.metamx.common.logger.Logger;
import io.druid.client.FilteredServerView;
import io.druid.client.ServerView;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;

/**
 * Watches a FilteredServerView for pending segments either showing up, or being overshadowed, in the historical
 * cluster.
 */
public class ServerViewWatcher implements Closeable
{
  private static final Logger log = new Logger(ServerViewWatcher.class);

  private final Appenderator appenderator;
  private final FilteredServerView serverView;
  private final ExecutorService serverViewExecutor;
  private final Callback callback;

  private volatile boolean stopped = false;

  public ServerViewWatcher(
      Appenderator appenderator,
      FilteredServerView serverView,
      ExecutorService serverViewExecutor,
      Callback callback
  )
  {
    this.appenderator = Preconditions.checkNotNull(appenderator, "appenderator");
    this.serverView = Preconditions.checkNotNull(serverView, "serverView");
    this.serverViewExecutor = Preconditions.checkNotNull(serverViewExecutor, "serverViewExecutor");
    this.callback = Preconditions.checkNotNull(callback, "callback");
  }

  public void start()
  {
    serverView.registerSegmentCallback(
        serverViewExecutor,
        new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment servedSegment)
          {
            if (stopped) {
              log.info("Unregistering ServerViewCallback");
              return ServerView.CallbackAction.UNREGISTER;
            }

            if (!server.isAssignable()) {
              return ServerView.CallbackAction.CONTINUE;
            }

            final SegmentIdentifier served = SegmentIdentifier.fromDataSegment(servedSegment);
            log.debug("Checking segment[%s] on server[%s]", served, server);

            if (appenderator.getDataSource().equals(servedSegment.getDataSource())) {
              for (SegmentIdentifier pending : appenderator.getSegments()) {
                // This isn't quite right, since we might unload some segments if they are only partially overshadowed
                // or if their overshadowing chunk is not completely present in the timeline. Because we don't maintain
                // a full timeline, let's err on the side of dropping things.

                if (pending.equals(served) ||
                    (served.getInterval().overlaps(pending.getInterval())
                     && served.getVersion().compareTo(pending.getVersion()) > 0)) {
                  log.info(
                      "Segment[%s] on server[%s] >= pending segment[%s].",
                      served,
                      server.getHost(),
                      pending
                  );

                  try {
                    callback.notify(pending, server, servedSegment);
                  }
                  catch (Exception e) {
                    log.warn(
                        e, "Callback failed for segment[%s] on server[%s] >= pending segment[%s].",
                        served,
                        server.getHost(),
                        pending
                    );
                  }
                }
              }
            }

            return ServerView.CallbackAction.CONTINUE;
          }
        },
        new Predicate<DataSegment>()
        {
          @Override
          public boolean apply(final DataSegment segment)
          {
            // Include any segments from our dataSource whose intervals overlap our pending segments.

            // Note that this filter's meaning changes over time (as the list of pending segments changes) and
            // therefore we may miss some interesting segments if they showed up before we realized we should care
            // about them. For now the workaround for this is to restart the ingester.

            return
                appenderator.getDataSource().equals(segment.getDataSource()) && Iterables.any(
                    appenderator.getSegments(),
                    new Predicate<SegmentIdentifier>()
                    {
                      @Override
                      public boolean apply(SegmentIdentifier identifier)
                      {
                        return segment.getInterval().overlaps(identifier.getInterval());
                      }
                    }
                );
          }
        }
    );
  }

  public void close()
  {
    stopped = true;
  }

  public interface Callback
  {
    /**
     * Called when the ServerViewWatcher notices that a particular pending identifier has either showed up, or
     * has been overshadowed, in the historical cluster.
     * <p/>
     * May be called multiple times for the same segment.
     *
     * @param pending       the pending identifier
     * @param server        the server we found a handed-off, or overshadowing, segment on
     * @param servedSegment the handed-off, or overshadowing, segment we found
     */
    void notify(SegmentIdentifier pending, DruidServerMetadata server, DataSegment servedSegment) throws Exception;
  }
}
