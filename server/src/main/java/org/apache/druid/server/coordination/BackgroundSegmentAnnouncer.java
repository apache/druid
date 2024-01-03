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

package org.apache.druid.server.coordination;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class BackgroundSegmentAnnouncer implements AutoCloseable
{
  private static final EmittingLogger log = new EmittingLogger(BackgroundSegmentAnnouncer.class);

  private final int intervalMillis;
  private final DataSegmentAnnouncer announcer;
  private final ScheduledExecutorService exec;
  private final LinkedBlockingQueue<DataSegment> queue;
  private final SettableFuture<Boolean> doneAnnouncing;

  private final Object lock = new Object();

  private volatile boolean finished = false;
  @Nullable
  private volatile ScheduledFuture<?> startedAnnouncing = null;
  @Nullable
  private volatile ScheduledFuture<?> nextAnnoucement = null;

  public BackgroundSegmentAnnouncer(
      DataSegmentAnnouncer announcer,
      ScheduledExecutorService exec,
      int intervalMillis
  )
  {
    this.announcer = announcer;
    this.exec = exec;
    this.intervalMillis = intervalMillis;
    this.queue = new LinkedBlockingQueue<>();
    this.doneAnnouncing = SettableFuture.create();
  }

  public void announceSegment(final DataSegment segment) throws InterruptedException
  {
    if (finished) {
      throw new ISE("Announce segment called after finishAnnouncing");
    }
    queue.put(segment);
  }

  public void startAnnouncing()
  {
    if (intervalMillis <= 0) {
      return;
    }

    log.info("Starting background segment announcing task");

    // schedule background announcing task
    nextAnnoucement = startedAnnouncing = exec.schedule(
        new Runnable()
        {
          @Override
          public void run()
          {
            synchronized (lock) {
              try {
                if (!(finished && queue.isEmpty())) {
                  final List<DataSegment> segments = new ArrayList<>();
                  queue.drainTo(segments);
                  try {
                    announcer.announceSegments(segments);
                    nextAnnoucement = exec.schedule(this, intervalMillis, TimeUnit.MILLISECONDS);
                  }
                  catch (IOException e) {
                    doneAnnouncing.setException(
                        new SegmentLoadingException(e, "Failed to announce segments[%s]", segments)
                    );
                  }
                } else {
                  doneAnnouncing.set(true);
                }
              }
              catch (Exception e) {
                doneAnnouncing.setException(e);
              }
            }
          }
        },
        intervalMillis,
        TimeUnit.MILLISECONDS
    );
  }

  public void finishAnnouncing() throws SegmentLoadingException
  {
    synchronized (lock) {
      finished = true;
      // announce any remaining segments
      try {
        final List<DataSegment> segments = new ArrayList<>();
        queue.drainTo(segments);
        announcer.announceSegments(segments);
      }
      catch (Exception e) {
        throw new SegmentLoadingException(e, "Failed to announce segments[%s]", queue);
      }

      // get any exception that may have been thrown in background announcing
      try {
        // check in case intervalMillis is <= 0
        if (startedAnnouncing != null) {
          startedAnnouncing.cancel(false);
        }
        // - if the task is waiting on the lock, then the queue will be empty by the time it runs
        // - if the task just released it, then the lock ensures any exception is set in doneAnnouncing
        if (doneAnnouncing.isDone()) {
          doneAnnouncing.get();
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SegmentLoadingException(e, "Loading Interrupted");
      }
      catch (ExecutionException e) {
        throw new SegmentLoadingException(e.getCause(), "Background Announcing Task Failed");
      }
    }
    log.info("Completed background segment announcing");
  }

  @Override
  public void close()
  {
    // stop background scheduling
    synchronized (lock) {
      finished = true;
      if (nextAnnoucement != null) {
        nextAnnoucement.cancel(false);
      }
    }
  }
}
