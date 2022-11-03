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

package org.apache.druid.catalog.sync;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.concurrent.Threads;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Push style notifications that allow propagation of data from whatever server is
 * running this notifier to whoever might be listening. Notifications arrive
 * via a queue, then are dispatched via a configured sender. Details of the
 * source of the updates, and how updates are sent, are handled external
 * to this class.
 *
 * The algorithm is simple: each update is processed entirely before the
 * next one is processed. As a result, this class is suitable for
 * low-frequency updates: where the worst-case send times are less than
 * the worst-case update frequency. If updates are faster, they will back
 * up, and the class should be redesigned to allow healthy receivers to
 * continue to get updates while laggards block only themselves.
 *
 * Events can be queued before startup. They will be send once the notifier
 * is started. Events left in the queue at shutdown will be lost.
 *
 * Defined by composition so it can be tested and reused in other
 * contexts.
 */
public class CacheNotifier
{
  private static final EmittingLogger LOG = new EmittingLogger(CacheNotifier.class);

  private final ExecutorService exec;
  private final String callerName;
  private final BlockingQueue<byte[]> updates = new LinkedBlockingQueue<>();
  private final Consumer<byte[]> sender;

  public CacheNotifier(
      final String callerName,
      final Consumer<byte[]> sender
  )
  {
    this.callerName = callerName;
    this.sender = sender;

    this.exec = Execs.singleThreaded(
        StringUtils.format(
            "%s-notifierThread-",
            StringUtils.encodeForFormat(callerName)) + "%d"
    );
  }

  public void start()
  {
    LOG.info("Starting Catalog sync");
    exec.submit(() -> {
      while (!Thread.interrupted()) {
        try {
          sender.accept(updates.take());
        }
        catch (InterruptedException e) {
          return;
        }
        catch (Throwable t) {
          LOG.makeAlert(t, callerName + ": Error occured while handling updates.").emit();
        }
      }
    });
  }

  public void send(byte[] update)
  {
    updates.add(update);
  }

  @VisibleForTesting
  public void stopGracefully()
  {
    try {
      while (!updates.isEmpty()) {
        Threads.sleepFor(100, TimeUnit.MILLISECONDS);
      }
    }
    catch (InterruptedException e) {
      // Ignore
    }
    stop();
  }

  public void stop()
  {
    if (!updates.isEmpty()) {
      LOG.warn("Shutting down Catalog sync with %d unsent notifications", updates.size());
    }
    exec.shutdownNow();
    LOG.info("Catalog sync stopped");
  }
}
