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

package io.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import io.druid.server.coordination.SegmentChangeRequestNoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ZkLoadQueuePeon extends LoadQueuePeon
{
  private static final EmittingLogger log = new EmittingLogger(ZkLoadQueuePeon.class);

  private final CuratorFramework curator;
  private final String basePath;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService processingExecutor;
  private final DruidCoordinatorConfig config;

  ZkLoadQueuePeon(
      CuratorFramework curator,
      String basePath,
      ObjectMapper jsonMapper,
      ScheduledExecutorService processingExecutor,
      ExecutorService callbackExecutor,
      DruidCoordinatorConfig config
  )
  {
    super(basePath, processingExecutor, callbackExecutor);
    this.curator = curator;
    this.basePath = basePath;
    this.jsonMapper = jsonMapper;
    this.processingExecutor = processingExecutor;
    this.config = config;
  }

  @Override
  void processHolder(final SegmentHolder holder)
  {
    try {
      final String path = ZKPaths.makePath(basePath, holder.getSegmentIdentifier());
      final byte[] payload = jsonMapper.writeValueAsBytes(holder.getChangeRequest());
      curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);

      processingExecutor.schedule(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                if (curator.checkExists().forPath(path) != null) {
                  failAssign(holder, new ISE("%s was never removed! Failing this operation!", path));
                }
              }
              catch (Exception e) {
                failAssign(holder, e);
              }
            }
          },
          config.getLoadTimeoutDelay().getMillis(),
          TimeUnit.MILLISECONDS
      );

      final Stat stat = curator.checkExists().usingWatcher(
          new CuratorWatcher()
          {
            @Override
            public void process(WatchedEvent watchedEvent) throws Exception
            {
              switch (watchedEvent.getType()) {
                case NodeDeleted:
                  if (!ZKPaths.getNodeFromPath(path).equals(holder.getSegmentIdentifier())) {
                    log.warn(
                        "Server[%s] entry [%s] was removed even though it's not what is currently loading[%s]",
                        basePath, path, holder
                    );
                    return;
                  }
                  actionCompleted(holder);
              }
            }
          }
      ).forPath(path);

      if (stat == null) {
        final byte[] noopPayload = jsonMapper.writeValueAsBytes(new SegmentChangeRequestNoop());

        // Create a node and then delete it to remove the registered watcher.  This is a work-around for
        // a zookeeper race condition.  Specifically, when you set a watcher, it fires on the next event
        // that happens for that node.  If no events happen, the watcher stays registered foreverz.
        // Couple that with the fact that you cannot set a watcher when you create a node, but what we
        // want is to create a node and then watch for it to get deleted.  The solution is that you *can*
        // set a watcher when you check to see if it exists so, we first create the node and then set a
        // watcher on its existence.  However, if already does not exist by the time the existence check
        // returns, then the watcher that was set will never fire (nobody will ever create the node
        // again) and thus lead to a slow, but real, memory leak.  So, we create another node to cause
        // that watcher to fire and delete it right away.
        //
        // We do not create the existence watcher first, because then it will fire when we create the
        // node and we'll have the same race when trying to refresh that watcher.
        curator.create().withMode(CreateMode.EPHEMERAL).forPath(path, noopPayload);
        actionCompleted(holder);
      }

    }
    catch (Exception e) {
      failAssign(holder, e);
    }
  }
}
