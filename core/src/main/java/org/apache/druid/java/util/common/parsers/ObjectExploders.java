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

package org.apache.druid.java.util.common.parsers;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

public class ObjectExploders
{
  private ObjectExploders()
  {
    // No instantiation.
  }

  public static <T> ObjectExploder<T> create(
      final List<JSONExplodeSpec> explodeSpecList,
      final ExploderMaker<T> exploderMaker
  )
  {

    return new ObjectExploder<T>()
    {

      @Override
      public List<T> explode(final List<T> obj)
      {
        List<T> allObjs = obj;
        for (JSONExplodeSpec explodeSpec : explodeSpecList) {
          List<T> newAllObjs = new ArrayList<>();
          final String path = explodeSpec.getExplodePath();
          for (T curObj : allObjs) {
            List<Object> arrayToExplode = exploderMaker.getExplodeArray(curObj, path);
            for (Object entry : arrayToExplode) {
              T newObj = exploderMaker.setObj(curObj, entry, path);
              newAllObjs.add(newObj);
            }
          }
          allObjs = newAllObjs;
        }
        List<T> retval = new ArrayList<>();
        for (T curObj : allObjs) {
          retval.add(curObj);
        }
        return retval;
      }
    };
  }

  public interface ExploderMaker<T>
  {
    List<Object> getExplodeArray(T node, String expr);

    T setObj(T node, Object value, String expr);

    Object valueConversionFunction(JsonNode val);

  }
}
