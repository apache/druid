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

package io.druid.curator.announcement;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.RetryUtils;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.concurrent.Execs;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Announces things on Zookeeper.
 */
public class Announcer
{
  private static final Logger log = new Logger(Announcer.class);

  private final CuratorFramework curator;

  private final ConcurrentMap<String, PersistentEphemeralNode> buggers = new MapMaker().makeMap();
  private final ConcurrentMap<String, byte[]> announcements = new MapMaker().makeMap();
  private final ListeningExecutorService announceActionExecutor;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final Lock canLeaveEOS = new ReentrantLock(false);
  private final Runnable eosRunnable = new Runnable()
  {
    @Override
    public void run()
    {
      canLeaveEOS.lock();
      canLeaveEOS.unlock();
    }
  };


  public Announcer(
      CuratorFramework curator,
      String nameFormat
  )
  {
    this.curator = curator;
    this.announceActionExecutor = MoreExecutors.listeningDecorator(
        Execs.singleThreaded(Preconditions.checkNotNull(nameFormat, "nameFormat"))
    );
    canLeaveEOS.lock();
    announceActionExecutor.submit(eosRunnable);
  }

  @LifecycleStart
  public synchronized void start()
  {
    if (started.get()) {
      return;
    }
    if (!started.compareAndSet(false, true)) {
      // Lost a race
      return;
    }
    log.debug("Starting");
    final ListenableFuture future = announceActionExecutor.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            // NOOP
          }
        }
    );
    canLeaveEOS.unlock();
    try {
      future.get();
    }
    catch (InterruptedException e) {
      log.info("Interrupted while waiting for start notification");
      Thread.currentThread().interrupt();
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @LifecycleStop
  public synchronized void stop()
  {
    if (!started.get()) {
      return;
    }

    if (!started.compareAndSet(true, false)) {
      // lost a race
      return;
    }

    log.debug("Stopping");

    for (String path : announcements.keySet()) {
      unannounce(path);
    }

    canLeaveEOS.lock();
    final CountDownLatch exitGate = new CountDownLatch(1);
    announceActionExecutor.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            exitGate.countDown();
            eosRunnable.run();
          }
        }
    );
    try {
      exitGate.await();
    }
    catch (InterruptedException e) {
      log.info("Interrupted while waiting for exit signal");
      Thread.currentThread().interrupt();
    }
  }


  public void announce(final String path, final byte[] bytes, boolean atomic)
  {

    addEventToQueue(
        new Runnable()
        {
          @Override
          public void run()
          {
            final byte[] priorAnnouncement = announcements.putIfAbsent(path, bytes);
            if (priorAnnouncement == null) {
              // A new announcement
              final PersistentEphemeralNode persistentEphemeralNode = new PersistentEphemeralNode(
                  curator,
                  PersistentEphemeralNode.Mode.EPHEMERAL,
                  path,
                  bytes
              );
              if (buggers.putIfAbsent(path, persistentEphemeralNode) != null) {
                throw new ISE("Bad state, [%s] found in persistent ephemeral node list but not in announcements");
              }
              persistentEphemeralNode.start();
              try {
                persistentEphemeralNode.waitForInitialCreate(1_000, TimeUnit.MILLISECONDS);
              }
              catch (InterruptedException e) {
                log.info(e, "Interrupted while waiting for initial node at path [%s]", path);
                Thread.currentThread().interrupt();
              }
            } else {
              if (Arrays.equals(priorAnnouncement, bytes)) {
                log.debug("Asked to announce [%s] with the data it already has! Silently ignoring", path);
              } else {
                log.error("Cannot re-announce different values under the same path at [%s]", path);
              }
            }
          }
        },
        atomic
    );
  }

  /**
   * Announces the provided bytes at the given path.  Announcement means that it will create an ephemeral node
   * and monitor it to make sure that it always exists until it is unannounced or this object is closed.
   *
   * @param path  The path to announce at
   * @param bytes The payload to announce
   */
  public void announce(final String path, final byte[] bytes)
  {
    announce(path, bytes, false);
  }

  public void update(final String path, final byte[] bytes)
  {
    update(path, bytes, false);
  }

  public void update(final String path, final byte[] bytes, boolean atomic)
  {
    addEventToQueue(
        new Runnable()
        {
          @Override
          public void run()
          {
            final PersistentEphemeralNode persistentEphemeralNode = buggers.get(path);

            if (persistentEphemeralNode == null) {
              throw new IAE("Cannot update a path[%s] that hasn't been announced!", path);
            }
            final byte[] oldBytes = announcements.get(path);
            if (!Arrays.equals(oldBytes, bytes)) {
              log.debug("Updating path [%s] with %,d bytes", path, bytes.length);
              try {
                persistentEphemeralNode.setData(bytes);
              }
              catch (Exception e) {
                throw Throwables.propagate(e);
              }
            } else {
              log.debug("Path [%s] already has the latest bytes, ignoring update request", path);
            }
          }
        }, atomic
    );
  }

  private void addEventToQueue(final Runnable runnable, boolean waitForComplete)
  {
    final ListenableFuture future = announceActionExecutor.submit(runnable);
    if (waitForComplete) {
      try {
        future.get();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw Throwables.propagate(e);
      }
      catch (ExecutionException e) {
        throw Throwables.propagate(e);
      }
    } else {
      Futures.addCallback(
          future, new FutureCallback()
          {
            @Override
            public void onSuccess(Object result)
            {
              log.debug("Successfully completed task");
            }

            @Override
            public void onFailure(Throwable t)
            {
              log.error(t, "Error in task completion");
            }
          }
      );
    }
  }

  public void unannounce(final String path)
  {
    unannounce(path, false);
  }

  /**
   * Unannounces an announcement created at path.  Note that if all announcements get removed, the Announcer
   * will continue to have ZK watches on paths because clearing them out is a source of ugly race conditions.
   * <p/>
   * If you need to completely clear all the state of what is being watched and announced, stop() the Announcer.
   *
   * @param path the path to unannounce
   */
  public void unannounce(final String path, boolean atomic)
  {
    addEventToQueue(
        new Runnable()
        {
          @Override
          public void run()
          {
            log.info("unannouncing [%s]", path);

            final PersistentEphemeralNode priorEphemeralNode = buggers.get(path);

            if (priorEphemeralNode == null) {
              log.error("Path[%s] not announced, cannot unannounce.", path);
              return;
            }

            try {
              RetryUtils.retry(
                  new Callable<Void>()
                  {
                    @Override
                    public Void call() throws Exception
                    {
                      priorEphemeralNode.close();
                      return null;
                    }
                  },
                  new Predicate<Throwable>()
                  {
                    @Override
                    public boolean apply(Throwable input)
                    {
                      return input instanceof IOException;
                    }
                  },
                  3
              );
            }
            catch (Exception ex) {
              throw Throwables.propagate(ex);
            }
            announcements.remove(path);
            buggers.remove(path);
          }
        }, atomic
    );
  }
}
