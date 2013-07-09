/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package druid.examples.webStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.InputSupplier;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.emitter.EmittingLogger;

import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class UpdateStream implements Runnable
{
  private static final EmittingLogger log = new EmittingLogger(UpdateStream.class);
  private static final long queueWaitTime = 15L;
  private final TypeReference<HashMap<String, Object>> typeRef;
  private final InputSupplier<BufferedReader> supplier;
  private final int QUEUE_SIZE = 10000;
  private final BlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<Map<String, Object>>(QUEUE_SIZE);
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final String timeDimension;

  public UpdateStream(
      InputSupplier<BufferedReader> supplier,
      String timeDimension
  )
  {
    this.supplier = supplier;
    this.typeRef = new TypeReference<HashMap<String, Object>>()
    {
    };
    this.timeDimension = timeDimension;
  }

  private boolean isValid(String s)
  {
    return !(s.isEmpty());
  }

  @Override
  public void run()
  {
    try {
      BufferedReader reader = supplier.getInput();
      String line;
      while ((line = reader.readLine()) != null) {
        if (isValid(line)) {
          HashMap<String, Object> map = mapper.readValue(line, typeRef);
          if (map.get(timeDimension) != null) {
            queue.offer(map, queueWaitTime, TimeUnit.SECONDS);
            log.debug("Successfully added to queue");
          } else {
            log.error("missing timestamp");
          }
        }
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

  }


  public Map<String, Object> pollFromQueue(long waitTime, TimeUnit unit) throws InterruptedException
  {
    return queue.poll(waitTime, unit);
  }

  public int getQueueSize()
  {
    return queue.size();
  }

  public String getTimeDimension(){
    return timeDimension;
  }

}
