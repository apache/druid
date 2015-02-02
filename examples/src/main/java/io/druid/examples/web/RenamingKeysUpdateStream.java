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
