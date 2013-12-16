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

package io.druid.examples.web;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RenamingKeysUpdateStream implements UpdateStream
{

  private final InputSupplierUpdateStream updateStream;
  private Map<String, String> renamedDimensions;

  public RenamingKeysUpdateStream(
      InputSupplierUpdateStream updateStream,
      Map<String, String> renamedDimensions
  )
  {
    this.renamedDimensions = renamedDimensions;
    this.updateStream = updateStream;
  }

  public Map<String, Object> pollFromQueue(long waitTime, TimeUnit unit) throws InterruptedException
  {
    return renameKeys(updateStream.pollFromQueue(waitTime, unit));
  }


  private Map<String, Object> renameKeys(Map<String, Object> update)
  {
    if (renamedDimensions != null) {
      Map<String, Object> renamedMap = Maps.newHashMap();
      for (String key : renamedDimensions.keySet()) {
        if (update.get(key) != null) {
          Object obj = update.get(key);
          renamedMap.put(renamedDimensions.get(key), obj);
        }
      }
      return renamedMap;
    } else {
      return update;
    }
  }

  public String getTimeDimension()
  {
    if (renamedDimensions != null && renamedDimensions.get(updateStream.getTimeDimension()) != null) {
      return renamedDimensions.get(updateStream.getTimeDimension());
    }
    return updateStream.getTimeDimension();

  }

  public void start()
  {
    updateStream.start();
  }

  public void stop(){
    updateStream.stop();
  }
}
