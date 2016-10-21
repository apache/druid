/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common.collect;


import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class Utils
{
  public static <K, V> Map<K, V> zipMap(K[] keys, V[] values) {
    Preconditions.checkArgument(values.length == keys.length,
                                "number of values[%s] different than number of keys[%s]",
                                values.length, keys.length);

    return zipMapPartial(keys, values);
  }
  
  public static <K, V> Map<K, V> zipMapPartial(K[] keys, V[] values)
  {
    Preconditions.checkArgument(values.length <= keys.length,
                                "number of values[%s] exceeds number of keys[%s]",
                                values.length, keys.length);

    Map<K, V> retVal = new LinkedHashMap<>();

    for(int i = 0; i < values.length; ++i)
    {
      retVal.put(keys[i], values[i]);
    }

    return retVal;
  }

  /** Create a Map from iterables of keys and values. Will throw an exception if there are more keys than values,
   *  or more values than keys. */
  public static <K, V> Map<K, V> zipMap(Iterable<K> keys, Iterable<V> values) {
    Map<K, V> retVal = new LinkedHashMap<>();

    Iterator<K> keysIter = keys.iterator();
    Iterator<V> valsIter = values.iterator();

    while (keysIter.hasNext()) {
      final K key = keysIter.next();

      Preconditions.checkArgument(valsIter.hasNext(),
                                  "number of values[%s] less than number of keys, broke on key[%s]",
                                  retVal.size(), key);

      retVal.put(key, valsIter.next());
    }

    Preconditions.checkArgument(!valsIter.hasNext(),
                                "number of values[%s] exceeds number of keys[%s]",
                                retVal.size() + Iterators.size(valsIter), retVal.size());

    return retVal;
  }

  /** Create a Map from iterables of keys and values. If there are more keys than values, or more values than keys,
    * the excess will be omitted. */
  public static <K, V> Map<K, V> zipMapPartial(Iterable<K> keys, Iterable<V> values)
  {
    Map<K, V> retVal = new LinkedHashMap<>();

    Iterator<K> keysIter = keys.iterator();
    Iterator<V> valsIter = values.iterator();

    while (keysIter.hasNext()) {
      final K key = keysIter.next();

      if(valsIter.hasNext())
      {
        retVal.put(key, valsIter.next());
      }
      else {
        break;
      }

    }

    return retVal;
  }
}
