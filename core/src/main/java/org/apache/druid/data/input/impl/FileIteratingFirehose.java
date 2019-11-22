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

package org.apache.druid.data.input.impl;

import org.apache.commons.io.LineIterator;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileIteratingFirehose implements Firehose
{
  private final Iterator<LineIterator> lineIterators;
  private final StringInputRowParser parser;

  private LineIterator lineIterator = null;

  private final Closeable closer;

  public FileIteratingFirehose(
      Iterator<LineIterator> lineIterators,
      StringInputRowParser parser
  )
  {
    this(lineIterators, parser, null);
  }

  public FileIteratingFirehose(
      Iterator<LineIterator> lineIterators,
      StringInputRowParser parser,
      Closeable closer
  )
  {
    this.lineIterators = lineIterators;
    this.parser = parser;
    this.closer = closer;
  }

  @Override
  public boolean hasMore() throws IOException
  {
    while ((lineIterator == null || !lineIterator.hasNext()) && lineIterators.hasNext()) {
      lineIterator = getNextLineIterator();
    }

    return lineIterator != null && lineIterator.hasNext();
  }

  @Nullable
  @Override
  public InputRow nextRow() throws IOException
  {
    if (!hasMore()) {
      throw new NoSuchElementException();
    }

    return parser.parse(lineIterator.next());
  }

  @Override
  public InputRowListPlusRawValues nextRowWithRaw() throws IOException
  {
    if (!hasMore()) {
      throw new NoSuchElementException();
    }

    String raw = lineIterator.next();
    try {
      return InputRowListPlusRawValues.of(parser.parse(raw), parser.parseString(raw));
    }
    catch (ParseException e) {
      return InputRowListPlusRawValues.of(parser.parseString(raw), e);
    }
  }

  private LineIterator getNextLineIterator() throws IOException
  {
    if (lineIterator != null) {
      lineIterator.close();
    }

    final LineIterator iterator = lineIterators.next();
    parser.startFileFromBeginning();
    return iterator;
  }

  @Override
  public void close() throws IOException
  {
    try (Closeable ignore = closer;
         Closeable ignore2 = lineIterator) {
      // close both via try-with-resources
    }
  }
}
