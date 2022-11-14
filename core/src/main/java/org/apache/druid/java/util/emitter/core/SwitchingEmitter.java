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

public class SwitchingEmitter implements Emitter
{

  private static final Logger log = new Logger(SwitchingEmitter.class);

  private final Emitter[] defaultEmitters;

  private final Map<String, List<Emitter>> feedToEmitters;
  private final Set<Emitter> knownEmitters;

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

  @Override
  public void emit(Event event)
  {
    // linear search is likely faster than hashed lookup
    // todo dont use a hashmap here. use something that will be more efficient for this kind of lookups
    for (Map.Entry<String, List<Emitter>> feedToEmitters : feedToEmitters.entrySet()) {
      if (event.getFeed().equals(feedToEmitters.getKey())) {
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
