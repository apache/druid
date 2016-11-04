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

import io.druid.java.util.common.logger.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Quite simply, an endpoint endpoint with an accompanying method which will wait for a determined amount of time.
 * It is intended to be used in a watchdog or deadhand system whereby if the timeout is exhausted then something
 * happens.
 */
@Path(DeadhandResource.DEADHAND_PATH)
public class DeadhandResource
{
  public static final String DEADHAND_PATH = "/deadhand/stall";
  private static final Logger log = new Logger(DeadhandResource.class);
  private final AtomicLong heartbeatDetector = new AtomicLong(0L);

  @POST
  public Response doHeartbeat(@Context final HttpServletRequest req)
  {
    log.info("Received stall from [%s]", req != null ? req.getRemoteAddr() : "unknown");
    refresh();
    return Response.ok().build();
  }

  void refresh()
  {
    synchronized (heartbeatDetector) {
      heartbeatDetector.incrementAndGet();
      heartbeatDetector.notifyAll();
    }
  }

  public void waitForHeartbeat(long timeout) throws InterruptedException, TimeoutException
  {
    final long start = System.currentTimeMillis();
    synchronized (heartbeatDetector) {
      final long pre = heartbeatDetector.get();
      do {
        final long t = timeout - (System.currentTimeMillis() - start);
        if (t > 0) {
          heartbeatDetector.wait(t);
        }
        if (heartbeatDetector.get() != pre) {
          log.debug("Heartbeat heard");
          return;
        }
        // See docs about wait regarding spurious wakeup
      } while (System.currentTimeMillis() - start < timeout);
      if (pre == heartbeatDetector.get()) {
        throw new TimeoutException(String.format("Not heard within %d ms", timeout));
      }
      log.debug("Timeout, but heartbeat heard anyways. Phew! that was close");
    }
  }

  // Protected for tests
  protected long getHeartbeatCount()
  {
    return heartbeatDetector.get();
  }
}
