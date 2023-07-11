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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteSource;
import com.google.common.io.LineProcessor;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.Parser;

import javax.annotation.Nullable;
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
  private static final String DOUBLE_CLASS_NAME = Double.class.getName();
  private static final String FLOAT_CLASS_NAME = Float.class.getName();
  private static final String INTEGER_CLASS_NAME = Integer.class.getName();
  private static final String LONG_CLASS_NAME = Long.class.getName();
  private static final String STRING_CLASS_NAME = String.class.getName();
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
    private final long bytes;

    public PopulateResult(int lines, int entries, long bytes)
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

    public long getBytes()
    {
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
    return populateAndWarnAtByteLimit(source, map, -1L, null);
  }

  /**
   * Read through the `source` line by line and populate `map` with the data returned from the `parser`. Warning
   * messages will be logged if the `byteLimit` > 0, and the number of bytes read into the map exceed the byte limit.
   * Note: in order to compute the byte length properly, the key and value types of map must both be instances of
   * String, otherwise no byte length is computed.
   *
   * @param source    The ByteSource to read lines from
   * @param map       The map to populate
   * @param byteLimit The limit of number of bytes after which a warning should be shown in the log. if < 0, indicates
   *                  no limit.
   * @param name      The name of the map that is being populated. Used to identify the map in log messages written.
   *
   * @return number of lines read and entries parsed
   *
   * @throws IOException
   */
  public PopulateResult populateAndWarnAtByteLimit(
      final ByteSource source,
      final Map<K, V> map,
      final long byteLimit,
      final String name
  ) throws IOException
  {
    return source.asCharSource(StandardCharsets.UTF_8).readLines(
        new LineProcessor<PopulateResult>()
        {
          private int lines = 0;
          private int entries = 0;
          private long bytes = 0L;
          private long byteLimitMultiple = 1L;
          private boolean keyAndValueByteSizesCanBeDetermined = true;

          @Override
          public boolean processLine(String line)
          {
            if (lines == Integer.MAX_VALUE) {
              throw new ISE("Cannot read more than %,d lines", Integer.MAX_VALUE);
            }
            final Map<K, V> kvMap = parser.parseToMap(line);
            if (kvMap == null) {
              return true;
            }
            map.putAll(kvMap);
            lines++;
            entries += kvMap.size();
            // this top level check so that we dont keep logging inability to determine
            // byte length for all (key, value) pairs.
            if (0 < byteLimit && keyAndValueByteSizesCanBeDetermined) {
              for (Map.Entry<K, V> e : kvMap.entrySet()) {
                keyAndValueByteSizesCanBeDetermined = canKeyAndValueTypesByteSizesBeDetermined(
                    e.getKey(),
                    e.getValue()
                );
                if (keyAndValueByteSizesCanBeDetermined) {
                  bytes += getByteLengthOfObject(e.getKey());
                  bytes += getByteLengthOfObject(e.getValue());
                  if (bytes > byteLimit * byteLimitMultiple) {
                    LOG.warn(
                        "[%s] exceeded the byteLimit of [%,d]. Current bytes [%,d]",
                        name,
                        byteLimit,
                        bytes
                    );
                    byteLimitMultiple++;
                  }
                }
              }
            }
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

  /**
   * Read through the `iterator` and populate `map` with the data iterated over. Warning
   * messages will be logged if the `byteLimit` > 0, and the number of bytes read into the map exceed the byte limit.
   * Note: in order to compute the byte length properly, the key and value types of map must both be instances of
   * String, otherwise no byte length is computed.
   *
   * @param iterator  The iterator to iterate over
   * @param map       The map to populate
   * @param byteLimit The limit of number of bytes after which a warning should be shown in the log. if < 0, indicates
   *                  no limit.
   * @param name      The name of the map that is being populated. Used to identify the map in log messages written.
   *
   * @return number of entries parsed and bytes stored in the map.
   */
  public static <K, V> PopulateResult populateAndWarnAtByteLimit(
      final Iterator<Pair<K, V>> iterator,
      final Map<K, V> map,
      final long byteLimit,
      final String name
  )
  {
    int lines = 0;
    int entries = 0;
    long bytes = 0L;
    long byteLimitMultiple = 1L;
    boolean keyAndValueByteSizesCanBeDetermined = true;
    while (iterator.hasNext()) {
      Pair<K, V> pair = iterator.next();
      K lhs = null != pair ? pair.lhs : null;
      V rhs = null != pair ? pair.rhs : null;
      map.put(lhs, rhs);
      entries++;
      // this top level check so that we dont keep logging inability to determine
      // byte length for all pairs.
      if (0 < byteLimit && keyAndValueByteSizesCanBeDetermined) {
        keyAndValueByteSizesCanBeDetermined = canKeyAndValueTypesByteSizesBeDetermined(lhs, rhs);
        if (keyAndValueByteSizesCanBeDetermined) {
          bytes += getByteLengthOfObject(lhs);
          bytes += getByteLengthOfObject(rhs);
          if (bytes > byteLimit * byteLimitMultiple) {
            LOG.warn(
                "[%s] exceeded the byteLimit of [%,d]. Current bytes [%,d]",
                name,
                byteLimit,
                bytes
            );
            byteLimitMultiple++;
          }
        }

      }
    }
    return new PopulateResult(lines, entries, bytes);
  }

  /**
   * only works for objects of type String, Double, Float, Integer, or Long.
   * @param o the object to get the number of bytes of.
   * @return the number of bytes of the object.
   */
  @VisibleForTesting
  static long getByteLengthOfObject(@Nullable Object o)
  {
    if (null != o) {
      if (o.getClass().getName().equals(STRING_CLASS_NAME)) {
        // Each String object has ~40 bytes of overhead
        return ((long) ((String) (o)).length() * Character.BYTES) + 40;
      } else if (o.getClass().getName().equals(DOUBLE_CLASS_NAME)) {
        return 8;
      } else if (o.getClass().getName().equals(FLOAT_CLASS_NAME)) {
        return 4;
      } else if (o.getClass().getName().equals(INTEGER_CLASS_NAME)) {
        return 4;
      } else if (o.getClass().getName().equals(LONG_CLASS_NAME)) {
        return 8;
      }
    }
    return 0;
  }

  @VisibleForTesting
  static <K, V> boolean canKeyAndValueTypesByteSizesBeDetermined(@Nullable K key, @Nullable V value)
  {
    boolean canBeDetermined = (null == key
                               || key.getClass().getName().equals(STRING_CLASS_NAME)
                               || key.getClass().getName().equals(DOUBLE_CLASS_NAME)
                               || key.getClass().getName().equals(FLOAT_CLASS_NAME)
                               || key.getClass().getName().equals(INTEGER_CLASS_NAME)
                               || key.getClass().getName().equals(LONG_CLASS_NAME))
                              && (null == value
                                  || value.getClass().getName().equals(STRING_CLASS_NAME)
                                  || value.getClass().getName().equals(DOUBLE_CLASS_NAME)
                                  || value.getClass().getName().equals(FLOAT_CLASS_NAME)
                                  || value.getClass().getName().equals(INTEGER_CLASS_NAME)
                                  || value.getClass().getName().equals(LONG_CLASS_NAME));
    if (!canBeDetermined) {
      LOG.warn(
          "cannot compute number of bytes when populating map because key and value classes are neither "
          + "Double, Float, Integer, Long, or String. Key class: [%s], Value class: [%s]",
          null != key ? key.getClass().getName() : null,
          null != value ? value.getClass().getName() : null);
    }
    return canBeDetermined;
  }
}
