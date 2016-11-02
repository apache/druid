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

package io.druid.query.lookup.namespace;

import com.google.common.base.Function;
import org.apache.commons.collections.keyvalue.MultiKey;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class ExtractionNamespaceCacheFactory<T extends ExtractionNamespace>
{

  /**
   * This function is called once if `ExtractionNamespace.getUpdateMs() == 0`, or every update if
   * `ExtractionNamespace.getUpdateMs() > 0`
   * For ExtractionNamespace which have the NamespaceExtractionCacheManager handle regular updates, this function
   * is used to populate the namespace cache each time.
   * For ExtractionNamespace implementations which do not have regular updates, this function can be used to
   * initialize resources.
   * If the returned version is the same as what is passed in as lastVersion, then no swap takes place, and the swap
   * is discarded.
   *
   * This method is used to support previously implemented {@link #populateCache(String, ExtractionNamespace, String, Map) getCachePopulator()}
   * that did not support multiple maps in one lookup.
   * This method injects the result of that getCachePopulator() as a default map of lookup.
   * If you want to support multiple maps in one lookup, you should override this method as onheap and offheap factories do.
   *
   * @param id                  The ID of ExtractionNamespace
   * @param extractionNamespace The ExtractionNamespace for which to populate data.
   * @param lastVersion         The version which was last cached
   * @param swap                The temporary Map into which data may be placed and will be "swapped" with the proper
   *                            namespace Map in NamespaceExtractionCacheManager. Implementations which cannot offer
   *                            a swappable cache of the data may ignore this but must make sure `buildFnMap(...)` returns
   *                            a proper Function.
   *
   * @return return the (new) version string used in the populating
   */
  public String populateCache(
      String id,
      T extractionNamespace,
      String lastVersion,
      ConcurrentMap<MultiKey, Map<String, String>> swap,
      Function<MultiKey, Map<String, String>> mapAllocator
      ) throws Exception
  {
    MultiKey key = new MultiKey(id, KeyValueMap.DEFAULT_MAPNAME);
    Map<String, String> cache = swap.get(key);
    if (cache == null)
    {
      cache = mapAllocator.apply(key);
      swap.put(key, cache);
    }
    return populateCache(id, extractionNamespace, lastVersion, cache);
  }

  /**
   * For minimal changes of other extensions that already defined getCachePopulator()
   * based on the previous implementation of globally cached lookup
   *
   * {@link #populateCache(String, ExtractionNamespace, String, ConcurrentMap, Function) getMapCachePopulator}
   * will wrap this method to make mapName support lookup.
   *
   * @param id                  The ID of ExtractionNamespace
   * @param extractionNamespace The ExtractionNamespace for which to populate data.
   * @param lastVersion         The version which was last cached
   * @param swap                The temporary Map into which data may be placed and will be "swapped" with the proper
   *                            namespace Map in NamespaceExtractionCacheManager. Implementations which cannot offer
   *                            a swappable cache of the data may ignore this but must make sure `buildFnMap(...)` returns
   *                            a proper Function.
   * @return return the (new) version string used in the populating
   */
  public String populateCache(String id, T extractionNamespace, String lastVersion, Map<String, String> swap)
  {
    return null;
  }
}
