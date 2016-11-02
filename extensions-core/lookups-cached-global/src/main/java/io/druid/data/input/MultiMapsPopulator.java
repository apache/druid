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

package io.druid.data.input;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.io.ByteSource;
import com.google.common.io.LineProcessor;
import io.druid.java.util.common.parsers.Parser;
import org.apache.commons.collections.keyvalue.MultiKey;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple class that takes a `ByteSource` and uses a `Parser<K, Map<K, V>>` to populate multiple `Map<K, V>`s
 * The `ByteSource` must be UTF-8 encoded
 *
 * If this is handy for other use cases please move this class into a common module
 */
public class MultiMapsPopulator<K, V>
{
  private final Parser<MultiKey, Map<K, V>> parser;
  private Function<MultiKey, Map<K, V>> mapAllocator;

  public MultiMapsPopulator(
      Parser<MultiKey, Map<K, V>> parser,
      Function<MultiKey, Map<K, V>> mapAllocator
  )
  {
    this.parser = parser;
    this.mapAllocator = mapAllocator;
  }

  /**
   * Read through the `source` line by line and populate `map`s with the data returned from the `parser`
   *
   * @param source The ByteSource to read lines from
   * @param maps    The map to populate
   *
   * @return The number of entries parsed
   *
   * @throws IOException
   */
  public MapPopulator.PopulateResult populate(final ByteSource source, final ConcurrentMap<MultiKey, Map<K, V>> maps) throws IOException
  {
    return source.asCharSource(Charsets.UTF_8).readLines(
        new LineProcessor<MapPopulator.PopulateResult>()
        {
          private int lines = 0;
          private int entries = 0;

          @Override
          public boolean processLine(String line) throws IOException
          {
            Map<MultiKey, Map<K, V>> parseResult = parser.parse(line);
            for (Map.Entry<MultiKey, Map<K, V>> entry: parseResult.entrySet()) {
              Map<K, V> map = maps.get(entry.getKey());
              if (map == null)
              {
                map = mapAllocator.apply(entry.getKey());
                maps.put(entry.getKey(), map);
              }
              Map<K, V> kvMap = entry.getValue();
              map.putAll(kvMap);
              entries += kvMap.size();
            }
            lines++;
            return true;
          }

          @Override
          public MapPopulator.PopulateResult getResult()
          {
            return new MapPopulator.PopulateResult(lines, entries);
          }
        }
    );
  }
}
