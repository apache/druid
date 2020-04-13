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

package org.apache.druid.query.topn;

import org.apache.druid.segment.historical.HistoricalCursor;

final class TopNUtils
{
  /**
   * Returns Object, so javac couldn't remove cast in methods like
   * {@link HistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopNScannerPrototype#scanAndAggregate}. That cast is
   * needed, because when TopNScannerPrototype is specialized, occurrences of {@link org.apache.druid.segment.data.Offset} are
   * replaced with the specific Offset subtype in the TopNScannerPrototype bytecode, via {@link
   * org.apache.druid.query.monomorphicprocessing.SpecializationService#getSpecializationState(Class, String,
   * com.google.common.collect.ImmutableMap)}, providing ImmutableMap.of(Offset.class, specificOffsetSubtype) as the
   * classRemapping argument.
   *
   * Casting to the specific Offset subtype helps Hotspot JIT (OpenJDK 8) to generate better assembly. It shouldn't be
   * so, because the Offset subtype is still always the same (otherwise cast wouldn't be possible), so JIT should
   * generate equivalent code. In OpenJDK 9 Hotspot could be improved and this "casting hack" is not needed anymore.
   *
   * TODO check if offset.clone() is also necessary for generating better assembly, or it could be removed.
   *
   * See also javadoc comments to methods, where this method is used.
   */
  static Object copyOffset(HistoricalCursor cursor)
  {
    return cursor.getOffset().clone();
  }

  private TopNUtils()
  {
  }
}
