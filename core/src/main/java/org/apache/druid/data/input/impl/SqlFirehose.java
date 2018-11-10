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

import com.google.common.collect.Iterators;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.prefetch.JsonIterator;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.utils.Runnables;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class SqlFirehose implements Firehose
{
  private final Iterator<JsonIterator<Map<String, Object>>> resultIterator;
  private final InputRowParser parser;
  private final Closeable closer;
  private JsonIterator<Map<String, Object>> lineIterator = null;

  public SqlFirehose(
      Iterator lineIterators,
      InputRowParser<Map<String, Object>> parser,
      Closeable closer
  )
  {
    this.resultIterator = lineIterators;
    this.parser = parser;
    this.closer = closer;
  }

  @Override
  public boolean hasMore()
  {
    while ((lineIterator == null || !lineIterator.hasNext()) && resultIterator.hasNext()) {
      lineIterator = getNextLineIterator();
    }

    return lineIterator != null && lineIterator.hasNext();
  }

  @Nullable
  @Override
  public InputRow nextRow()
  {
    Map<String, Object> mapToParse = lineIterator.next();
    return (InputRow) Iterators.getOnlyElement(parser.parseBatch(mapToParse).iterator());
  }

  private JsonIterator getNextLineIterator()
  {
    if (lineIterator != null) {
      lineIterator = null;
    }

    final JsonIterator iterator = resultIterator.next();
    return iterator;
  }

  @Override
  public Runnable commit()
  {
    return Runnables.getNoopRunnable();
  }

  @Override
  public void close() throws IOException
  {
    Closer firehoseCloser = Closer.create();
    if (lineIterator != null) {
      firehoseCloser.register(lineIterator);
    }
    firehoseCloser.register(closer);
    firehoseCloser.close();
  }
}
