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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.util.List;

public class ComposingEmitter implements Emitter
{
  private static Logger log = new Logger(ComposingEmitter.class);

  private final List<Emitter> emitters;

  public ComposingEmitter(List<Emitter> emitters)
  {
    this.emitters = Preconditions.checkNotNull(emitters, "null emitters");
  }

  @Override
  @LifecycleStart
  public void start()
  {
    log.info("Starting Composing Emitter.");

    for (Emitter e : emitters) {
      log.info("Starting emitter %s.", e.getClass().getName());
      e.start();
    }
  }

  @Override
  public void emit(Event event)
  {
    for (Emitter e : emitters) {
      e.emit(event);
    }
  }

  @Override
  public void flush() throws IOException
  {
    boolean fail = false;
    log.info("Flushing Composing Emitter.");

    for (Emitter e : emitters) {
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
    log.info("Closing Composing Emitter.");

    for (Emitter e : emitters) {
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

  @Override
  public String toString()
  {
    return "ComposingEmitter{" +
           "emitters=" + emitters +
           '}';
  }
}
