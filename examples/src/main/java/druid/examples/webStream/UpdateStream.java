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
import com.google.common.io.InputSupplier;
import com.metamx.emitter.EmittingLogger;

import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class UpdateStream implements Runnable
{
  private static final EmittingLogger log = new EmittingLogger(UpdateStream.class);
  private static final long queueWaitTime = 15L;
  private final TypeReference<HashMap<String, Object>> typeRef;
  private final InputSupplier<BufferedReader> supplier;
  private final BlockingQueue<Map<String, Object>> queue;
  private final ObjectMapper mapper;

  public UpdateStream(
      InputSupplier<BufferedReader> supplier,
      BlockingQueue<Map<String, Object>> queue,
      ObjectMapper mapper
  )
  {
    this.supplier = supplier;
    this.queue = queue;
    this.mapper = mapper;
    this.typeRef = new TypeReference<HashMap<String, Object>>()
    {
    };
  }

  private boolean isValid(String s)
  {
    return !(s.isEmpty());
  }

  @Override
  public void run() throws RuntimeException
  {
    try {
      BufferedReader reader = supplier.getInput();
      String line;
      while ((line = reader.readLine()) != null) {
        if (isValid(line)) {
          HashMap<String, Object> map = mapper.readValue(line, typeRef);
          queue.offer(map, queueWaitTime, TimeUnit.SECONDS);
          log.info("Successfully added to queue");
        }
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

  }
}
