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

package io.druid.query.extraction.namespace;

import com.google.common.base.Function;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 *
 */
public interface ExtractionNamespaceFunctionFactory<T extends ExtractionNamespace>
{

  /**
   * Create a function for the given namespace which will do the manipulation requested in the extractionNamespace.
   * A simple implementation would simply use the cache supplied by the `NamespaceExtractionCacheManager`.
   * More advanced implementations may need more than just what can be cached by `NamespaceExtractionCacheManager`.
   *
   * @param extractionNamespace The ExtractionNamespace for which a manipulating function is needed.
   *
   * @return A function which will perform an extraction in accordance with the desires of the ExtractionNamespace
   */
  Function<String, String> buildFn(T extractionNamespace, Map<String, String> cache);


  /**
   * @param extractionNamespace The ExtractionNamespace for which a manipulating reverse function is needed.
   * @param cache view of the cache containing the function mapping.
   *
   * @return A function that will perform reverse lookup.
   */
  Function<String, List<String>> buildReverseFn(T extractionNamespace, final Map<String, String> cache);

  /**
   * This function is called once if `ExtractionNamespace.getUpdateMs() == 0`, or every update if
   * `ExtractionNamespace.getUpdateMs() > 0`
   * For ExtractionNamespace which have the NamespaceExtractionCacheManager handle regular updates, this function
   * is used to populate the namespace cache each time.
   * For ExtractionNamespace implementations which do not have regular updates, this function can be used to
   * initialize resources.
   * If the result of the Callable is the same as what is passed in as lastVersion, then no swap takes place, and the swap is discarded.
   *
   * @param extractionNamespace The ExtractionNamespace for which to populate data.
   * @param lastVersion         The version which was last cached
   * @param swap                The temporary Map into which data may be placed and will be "swapped" with the proper
   *                            namespace Map in NamespaceExtractionCacheManager. Implementations which cannot offer
   *                            a swappable cache of the data may ignore this but must make sure `buildFn(...)` returns
   *                            a proper Function.
   *
   * @return A callable that will be used to refresh resources of the namespace and return the version string used in
   * the populating
   */
  Callable<String> getCachePopulator(T extractionNamespace, String lastVersion, Map<String, String> swap);
}
