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

package io.druid.server.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.client.ServerView;
import io.druid.concurrent.Execs;
import io.druid.db.DatabaseSegmentManager;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.realtime.DbSegmentPublisher;
import io.druid.server.coordination.BaseZkCoordinator;
import io.druid.server.coordination.DataSegmentChangeCallback;
import io.druid.server.coordination.DataSegmentChangeHandler;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ExecutorService;

/**
 */
public class BridgeZkCoordinator extends BaseZkCoordinator
{
  private static final Logger log = new Logger(BaseZkCoordinator.class);

  private final DbSegmentPublisher dbSegmentPublisher;
  private final DatabaseSegmentManager databaseSegmentManager;
  private final ServerView serverView;

  private final ExecutorService exec = Execs.singleThreaded("BridgeZkCoordinatorServerView-%s");

  @Inject
  public BridgeZkCoordinator(
      ObjectMapper jsonMapper,
      ZkPathsConfig zkPaths,
      SegmentLoaderConfig config,
      DruidServerMetadata me,
      @Bridge CuratorFramework curator,
      DbSegmentPublisher dbSegmentPublisher,
      DatabaseSegmentManager databaseSegmentManager,
      ServerView serverView
  )
  {
    super(jsonMapper, zkPaths, config, me, curator);

    this.dbSegmentPublisher = dbSegmentPublisher;
    this.databaseSegmentManager = databaseSegmentManager;
    this.serverView = serverView;
  }

  @Override
  public void loadLocalCache()
  {
    // do nothing
  }

  @Override
  public DataSegmentChangeHandler getDataSegmentChangeHandler()
  {
    return BridgeZkCoordinator.this;
  }

  @Override
  public void addSegment(final DataSegment segment, final DataSegmentChangeCallback callback)
  {
    try {
      log.info("Publishing segment %s", segment.getIdentifier());
      dbSegmentPublisher.publishSegment(segment);
      serverView.registerSegmentCallback(
          exec,
          new ServerView.BaseSegmentCallback()
          {
            @Override
            public ServerView.CallbackAction segmentAdded(
                DruidServerMetadata server, DataSegment theSegment
            )
            {
              if (theSegment.equals(segment)) {
                callback.execute();
              }
              return ServerView.CallbackAction.CONTINUE;
            }
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void removeSegment(final DataSegment segment, final DataSegmentChangeCallback callback)
  {
    databaseSegmentManager.removeSegment(segment.getDataSource(), segment.getIdentifier());
    serverView.registerSegmentCallback(
        exec,
        new ServerView.BaseSegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentRemoved(
              DruidServerMetadata server, DataSegment theSegment
          )
          {
            if (theSegment.equals(segment)) {
              callback.execute();
            }
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }
}
