/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.resources;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.druid.concurrent.Execs;
import io.druid.indexing.overlord.TierLocalTaskRunner;
import io.druid.indexing.overlord.config.TierLocalTaskRunnerConfig;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class DeadhandMonitor
{
  private static final Logger log = new Logger(DeadhandMonitor.class);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final DeadhandResource resource;
  private final File deadhandFile;
  private final ListeningExecutorService watchdogService = MoreExecutors.listeningDecorator(Execs.multiThreaded(
      2,
      "deadhandWatchdog-%s"
  ));
  private final long timeout;

  @Inject
  public DeadhandMonitor(
      final DeadhandResource resource,
      final TierLocalTaskRunnerConfig tierLocalTaskRunnerConfig
  )
  {
    this(resource, tierLocalTaskRunnerConfig, new File(TierLocalTaskRunner.DEADHAND_FILE_NAME));
  }

  public DeadhandMonitor(
      final DeadhandResource resource,
      final TierLocalTaskRunnerConfig tierLocalTaskRunnerConfig,
      final File deadhandFile
  )
  {
    this.resource = resource;
    this.timeout = tierLocalTaskRunnerConfig.getHeartbeatTimeLimit();
    this.deadhandFile = deadhandFile;
  }

  void exit()
  {
    System.exit(0xDEAD);
  }

  @LifecycleStart
  public void start()
  {
    synchronized (started) {
      if (watchdogService.isShutdown()) {
        throw new ISE("Already stopped");
      }
      ListenableFuture<?> future = watchdogService.submit(
          new Runnable()
          {
            @Override
            public void run()
            {
              boolean shouldExit = true;
              final Path deadhandParentPath = deadhandFile.getAbsoluteFile().toPath().getParent();
              try (final WatchService watchService = deadhandParentPath.getFileSystem().newWatchService()) {
                deadhandParentPath.register(watchService, StandardWatchEventKinds.ENTRY_DELETE);
                log.info("Monitoring [%s] for shutdown", deadhandFile);
                while (deadhandFile.exists()) {
                  watchService.poll(10, TimeUnit.SECONDS);
                }
                log.warn("[%s] vanished! exiting", deadhandFile);
              }
              catch (IOException e) {
                log.error(e, "Could not register deadhand watchdog!");
              }
              catch (InterruptedException e) {
                shouldExit = false;
                log.info("Interrupted while watching deadhand file");
              }
              if (shouldExit) {
                exit();
              }
            }
          }
      );
      Futures.addCallback(future, new FutureCallback<Object>()
      {
        @Override
        public void onSuccess(@Nullable Object result)
        {
          log.debug("deadhand file watch finished");
        }

        @Override
        public void onFailure(Throwable t)
        {
          if (t.getCause() instanceof InterruptedException) {
            log.debug("Deadhand file watch interrupted");
          } else {
            log.error(t, "Failed deadhand file watch");
          }
        }
      });
      future = watchdogService.submit(
          new Runnable()
          {
            @Override
            public void run()
            {
              while (!Thread.currentThread().isInterrupted()) {
                try {
                  resource.waitForHeartbeat(timeout);
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  log.warn("Interrupted, stopping deadhand watchdog");
                  throw Throwables.propagate(e);
                }
                catch (TimeoutException e) {
                  boolean exiting = false;
                  synchronized (started) {
                    if (started.get()) {
                      log.error(e, "Timeout reached. I shall ride eternal, shiny and chrome!");
                      exiting = true;
                    } else {
                      log.warn("Timeout but not started");
                    }
                  }
                  if (exiting) {
                    // Outside of synchronized block
                    exit();
                  }
                }
              }
            }
          }
      );
      Futures.addCallback(future, new FutureCallback<Object>()
      {
        @Override
        public void onSuccess(@Nullable Object result)
        {
          log.debug("Resource watch finished");
        }

        @Override
        public void onFailure(Throwable t)
        {
          if (t.getCause() instanceof InterruptedException) {
            log.debug("Deadhand resource watcher interrupted");
          } else {
            log.error(t, "Error watching deadhand resource");
          }
        }
      });
      resource.refresh();
      started.set(true);
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (started) {
      started.set(false);
      watchdogService.shutdownNow();
    }
  }
}
