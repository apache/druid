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

package org.apache.druid.java.util.emitter.core;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An emitter than that offers the ability to direct an event to multiple emitters based on the event's feed.
 */
public class SwitchingEmitter implements Emitter
{

  private static final Logger log = new Logger(SwitchingEmitter.class);

  private final Emitter[] defaultEmitters;

  private final Map<String, List<Emitter>> feedToEmitters;
  private final Set<Emitter> knownEmitters;

  /**
   * Constructor for the SwitchingEmitter
   *
   * @param feedToEmitters Map of feed to a list of emitters that correspond to each feed,
   * @param defaultEmitter A list of emitters to use if there isn't a match of feed to an emitter
   */
  public SwitchingEmitter(Map<String, List<Emitter>> feedToEmitters, Emitter[] defaultEmitter)
  {
    this.feedToEmitters = feedToEmitters;
    this.defaultEmitters = defaultEmitter;
    ImmutableSet.Builder<Emitter> emittersSetBuilder = new ImmutableSet.Builder<>();
    emittersSetBuilder.addAll(Arrays.stream(defaultEmitter).iterator());
    for (List<Emitter> emitterList : feedToEmitters.values()) {
      for (Emitter emitter : emitterList) {
        emittersSetBuilder.add(emitter);
      }
    }
    this.knownEmitters = emittersSetBuilder.build();
  }

  /**
   * Start the emitter. This will start all the emitters the SwitchingEmitter uses.
   */
  @Override
  @LifecycleStart
  public void start()
  {
    log.info("Starting Switching Emitter.");

    for (Emitter e : knownEmitters) {
      log.info("Starting emitter %s.", e.getClass().getName());
      e.start();
    }
  }

  /**
   * Emit an event. This method must not throw exceptions or block. The emitters that this uses must also not throw
   * exceptions or block.
   * <p>
   * This emitter will direct events based on feed to a list of emitters specified. If there is no match the event will
   * use a list of default emitters instead.
   * <p>
   * Emitters that this emitter uses that receive too many events and internal queues fill up, should drop events rather
   * than blocking or consuming excessive memory.
   * <p>
   * If an emitter that this emitter uses receives input it considers to be invalid, or has an internal problem, it
   * should deal with that by logging a warning rather than throwing an exception. Emitters that log warnings
   * should consider throttling warnings to avoid excessive logs, since a busy Druid cluster can emit a high volume of
   * events.
   *
   * @param event The event that will be emitted.
   */
  @Override
  public void emit(Event event)
  {
    // linear search is likely faster than hashed lookup
    for (Map.Entry<String, List<Emitter>> feedToEmitters : feedToEmitters.entrySet()) {
      if (feedToEmitters.getKey().equals(event.getFeed())) {
        for (Emitter emitter : feedToEmitters.getValue()) {
          emitter.emit(event);
        }
        return;
      }
    }
    for (Emitter emitter : defaultEmitters) {
      emitter.emit(event);
    }
  }

  /**
   * Triggers this emitter to tell all emitters that this uses to flush.
   * @throws IOException
   */
  @Override
  public void flush() throws IOException
  {
    boolean fail = false;
    log.info("Flushing Switching Emitter.");

    for (Emitter e : knownEmitters) {
      try {
        log.info("Flushing emitter %s.", e.getClass().getName());
        e.flush();
      }
      catch (IOException ex) {
        log.error(ex, "Failed to flush emitter [%s]", e.getClass().getName());
        fail = true;
      }
    }

    if (fail) {
      throw new IOException("failed to flush one or more emitters");
    }
  }

  /**
   * Closes all emitters that the SwitchingEmitter uses
   * @throws IOException
   */
  @Override
  @LifecycleStop
  public void close() throws IOException
  {
    boolean fail = false;
    log.info("Closing Switching Emitter.");

    for (Emitter e : knownEmitters) {
      try {
        log.info("Closing emitter %s.", e.getClass().getName());
        e.close();
      }
      catch (IOException ex) {
        log.error(ex, "Failed to close emitter [%s]", e.getClass().getName());
        fail = true;
      }
    }

    if (fail) {
      throw new IOException("failed to close one or more emitters");
    }
  }
}
