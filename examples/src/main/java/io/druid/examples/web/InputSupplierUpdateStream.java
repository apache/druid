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

package io.druid.examples.web;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.InputSupplier;
import com.metamx.emitter.EmittingLogger;
import io.druid.jackson.DefaultObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class InputSupplierUpdateStream implements UpdateStream
{
  private static final EmittingLogger log = new EmittingLogger(InputSupplierUpdateStream.class);
  private static final long queueWaitTime = 15L;
  private final TypeReference<HashMap<String, Object>> typeRef;
  private final InputSupplier<BufferedReader> supplier;
  private final int QUEUE_SIZE = 10000;
  private final BlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<Map<String, Object>>(QUEUE_SIZE);
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final String timeDimension;
  private final Thread addToQueueThread;

  public InputSupplierUpdateStream(
      final InputSupplier<BufferedReader> supplier,
      final String timeDimension
  )
  {
    addToQueueThread = new Thread()
    {
      public void run()
      {
        while (!isInterrupted()) {
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
                  log.info("missing timestamp");
                }
              }
            }
          }

          catch (InterruptedException e){
            log.info(e, "Thread adding events to the queue interrupted");
            return;
          }
          catch (JsonMappingException e) {
            log.info(e, "Error in converting json to map");
          }
          catch (JsonParseException e) {
            log.info(e, "Error in parsing json");
          }
          catch (IOException e) {
            log.info(e, "Error in connecting to InputStream");
          }
        }
      }
    };
    addToQueueThread.setDaemon(true);

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

  public void start()
  {
    addToQueueThread.start();

  }

  public void stop()
  {
    addToQueueThread.interrupt();
  }


  public Map<String, Object> pollFromQueue(long waitTime, TimeUnit unit) throws InterruptedException
  {
    return queue.poll(waitTime, unit);
  }

  public int getQueueSize()
  {
    return queue.size();
  }

  public String getTimeDimension()
  {
    return timeDimension;
  }

}
