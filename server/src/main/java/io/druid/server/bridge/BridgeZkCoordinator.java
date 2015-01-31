/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.client.ServerView;
import io.druid.concurrent.Execs;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.realtime.SegmentPublisher;
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

  private final SegmentPublisher dbSegmentPublisher;
  private final MetadataSegmentManager databaseSegmentManager;
  private final ServerView serverView;

  private final ExecutorService exec = Execs.singleThreaded("BridgeZkCoordinatorServerView-%s");

  @Inject
  public BridgeZkCoordinator(
      ObjectMapper jsonMapper,
      ZkPathsConfig zkPaths,
      SegmentLoaderConfig config,
      DruidServerMetadata me,
      @Bridge CuratorFramework curator,
      SegmentPublisher dbSegmentPublisher,
      MetadataSegmentManager databaseSegmentManager,
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
