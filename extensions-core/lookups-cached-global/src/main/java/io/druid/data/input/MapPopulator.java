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
import com.google.common.io.ByteSource;
import com.google.common.io.LineProcessor;

import io.druid.java.util.common.parsers.Parser;

import java.io.IOException;
import java.util.Map;

/**
 * Simple class that takes a `ByteSource` and uses a `Parser<K, V>` to populate a `Map<K, V>`
 * The `ByteSource` must be UTF-8 encoded
 * <p>
 * If this is handy for other use cases pleaes move this class into a common module
 */
public class MapPopulator<K, V>
{
  private final Parser<K, V> parser;

  public MapPopulator(
      Parser<K, V> parser
  )
  {
    this.parser = parser;
  }

  /**
   * Read through the `source` line by line and populate `map` with the data returned from the `parser`
   *
   * @param source The ByteSource to read lines from
   * @param map    The map to populate
   *
   * @return The number of entries parsed
   *
   * @throws IOException
   */
  public long populate(final ByteSource source, final Map<K, V> map) throws IOException
  {
    return source.asCharSource(Charsets.UTF_8).readLines(
        new LineProcessor<Long>()
        {
          private long count = 0L;

          @Override
          public boolean processLine(String line) throws IOException
          {
            map.putAll(parser.parse(line));
            ++count;
            return true;
          }

          @Override
          public Long getResult()
          {
            return count;
          }
        }
    );
  }
}
