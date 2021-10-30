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

package org.apache.druid.data.input;

import com.google.common.io.ByteSource;
import com.google.common.io.LineProcessor;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.Parser;
import org.skife.jdbi.v2.ResultIterator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

/**
 * Simple class that takes a `ByteSource` and uses a `Parser<K, V>` to populate a `Map<K, V>`
 * The `ByteSource` must be UTF-8 encoded
 * <p>
 * If this is handy for other use cases pleaes move this class into a common module
 */
public class MapPopulator<K, V>
{
  private static final Logger LOG = new Logger(MapPopulator.class);
  private final Parser<K, V> parser;

  public MapPopulator(
      Parser<K, V> parser
  )
  {
    this.parser = parser;
  }

  public static class PopulateResult
  {
    private final int lines;
    private final int entries;
    private final int bytes;

    public PopulateResult(int lines, int entries, int bytes)
    {
      this.lines = lines;
      this.entries = entries;
      this.bytes = bytes;
    }

    public int getLines()
    {
      return lines;
    }

    public int getEntries()
    {
      return entries;
    }

    public int getBytes() {
      return bytes;
    }
  }

  /**
   * Read through the `source` line by line and populate `map` with the data returned from the `parser`
   *
   * @param source The ByteSource to read lines from
   * @param map    The map to populate
   *
   * @return number of lines read and entries parsed
   *
   * @throws IOException
   */
  public PopulateResult populate(final ByteSource source, final Map<K, V> map) throws IOException
  {
    return populateAndWarnAtByteLimit(source, map, null, null);
  }

  public PopulateResult populateAndWarnAtByteLimit(
      final ByteSource source,
      final Map<K, V> map,
      final Long byteLimit,
      final String name)
      throws IOException
  {
    return source.asCharSource(StandardCharsets.UTF_8).readLines(
        new LineProcessor<PopulateResult>()
        {
          private int lines = 0;
          private int entries = 0;
          private int bytes = 0;
          int byteLimitMultiple = 1;

          @Override
          public boolean processLine(String line)
          {
            if (lines == Integer.MAX_VALUE) {
              throw new ISE("Cannot read more than %,d lines", Integer.MAX_VALUE);
            }
            final Map<K, V> kvMap = parser.parseToMap(line);
            if (null != byteLimit) {
              for (Map.Entry<K, V> e : kvMap.entrySet()) {
                bytes += getByteLengthOfKeyAndValuePair(e.getKey(), e.getValue());
              }
              if (bytes != 0 && (bytes > byteLimit * byteLimitMultiple)) {
                LOG.warn("[%s] exceeded the byteLimit of [%,d]. Current bytes [%,d]",
                         name,
                         byteLimit,
                         bytes
                );
                byteLimitMultiple++;
              }
            }
            map.putAll(kvMap);
            lines++;
            entries += kvMap.size();
            return true;
          }

          @Override
          public PopulateResult getResult()
          {
            return new PopulateResult(lines, entries, bytes);
          }
        }
    );
  }

  public PopulateResult populateAndWarnAtByteLimit(
      final Iterator<Pair<K, V>> iterator,
      final Map<K, V> map,
      final Long byteLimit,
      final String name)
  {
    int lines = 0;
    int entries = 0;
    int bytes = 0;
    int byteLimitMultiple = 1;
    while (iterator.hasNext()) {
      Pair<K,V> pair = iterator.next();
      if (null != byteLimit) {
        bytes += getByteLengthOfKeyAndValuePair(pair.lhs, pair.rhs);
        if (bytes != 0 && (bytes > byteLimit * byteLimitMultiple)) {
          LOG.warn("[%s] exceeded the byteLimit of [%,d]. Current bytes [%,d]",
                   name,
                   byteLimit,
                   bytes
          );
          byteLimitMultiple++;
        }
      }
      map.put(pair.lhs, pair.rhs);
      entries++;
    }
    return new PopulateResult(lines, entries, bytes);
  }

  private long getByteLengthOfKeyAndValuePair(K key, V value) {
    if ((key instanceof String) && (value instanceof String)) {
      return((String) (key)).length() + ((String) (value)).length();
    } else {
      LOG.warn(
          "cannot bytes when populating map because key and value classes are not "
          + "instance of String. Key class: [%s], Value class: [%s]",
          key.getClass().getName(),
          value.getClass().getName());
      return 0;
    }
  }
}
