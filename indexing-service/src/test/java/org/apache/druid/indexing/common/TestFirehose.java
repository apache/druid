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

package org.apache.druid.indexing.common;

import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowPlusRaw;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.AbstractTextFilesFirehoseFactory;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public class TestFirehose implements Firehose
{
  public static class TestFirehoseFactory implements FirehoseFactory<InputRowParser>
  {
    private boolean waitForClose = true;
    private List<Object> seedRows;

    public TestFirehoseFactory()
    {
    }

    public TestFirehoseFactory(boolean waitForClose, List<Object> seedRows)
    {
      this.waitForClose = waitForClose;
      this.seedRows = seedRows;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Firehose connect(InputRowParser parser, File temporaryDirectory) throws ParseException
    {
      return new TestFirehose(parser, waitForClose, seedRows);
    }
  }

  public static class TestAbstractTextFilesFirehoseFactory extends AbstractTextFilesFirehoseFactory
  {
    private boolean waitForClose;
    private List<Object> seedRows;

    public TestAbstractTextFilesFirehoseFactory(boolean waitForClose, List<Object> seedRows)
    {
      this.waitForClose = waitForClose;
      this.seedRows = seedRows;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Firehose connect(StringInputRowParser parser, File temporaryDirectory) throws ParseException
    {
      return new TestFirehose(parser, waitForClose, seedRows);
    }

    @Override
    protected Collection initObjects()
    {
      return null;
    }

    @Override
    protected InputStream openObjectStream(Object object)
    {
      return null;
    }

    @Override
    protected InputStream wrapObjectStream(Object object, InputStream stream)
    {
      return null;
    }

    @Override
    public FiniteFirehoseFactory withSplit(InputSplit split)
    {
      return null;
    }
  }

  public static final String FAIL_DIM = "__fail__";

  private final Deque<Optional<Object>> queue = new ArrayDeque<>();

  private InputRowParser parser;
  private boolean closed;
  private Iterator<InputRow> parsedInputRows = new ArrayList<InputRow>().iterator();
  private Object raw = null;

  private TestFirehose(InputRowParser parser, boolean waitForClose, List<Object> seedRows)
  {
    this.parser = parser;
    this.closed = !waitForClose;

    if (parser instanceof StringInputRowParser) {
      ((StringInputRowParser) parser).startFileFromBeginning();
    }

    if (seedRows != null) {
      seedRows.stream().map(Optional::ofNullable).forEach(queue::add);
    }
  }

  public void addRows(List<Object> rows)
  {
    synchronized (this) {
      rows.stream().map(Optional::ofNullable).forEach(queue::add);
      notifyAll();
    }
  }

  @Override
  public boolean hasMore()
  {
    try {
      synchronized (this) {
        while (queue.isEmpty() && !closed) {
          wait();
        }
        return !queue.isEmpty() || parsedInputRows.hasNext();
      }
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public InputRow nextRow()
  {
    synchronized (this) {
      if (!hasMore()) {
        throw new NoSuchElementException();
      }
      if (parsedInputRows.hasNext()) {
        return parsedInputRows.next();
      }
      raw = queue.removeFirst().orElse(null);
      parsedInputRows = parser instanceof StringInputRowParser
                        ? ((StringInputRowParser) parser).parseBatch((String) raw).iterator()
                        : parser.parseBatch(raw).iterator();
      if (parsedInputRows.hasNext()) {
        InputRow row = parsedInputRows.next();
        if (row != null && row.getRaw(FAIL_DIM) != null) {
          throw new ParseException(FAIL_DIM);
        }
        return row;
      } else {
        throw new ParseException(null);
      }
    }
  }

  @Override
  public InputRowPlusRaw nextRowWithRaw()
  {
    synchronized (this) {
      try {
        final InputRow row = nextRow();
        final Object next = raw;
        if (row != null && row.getRaw(FAIL_DIM) != null) {
          throw new ParseException(FAIL_DIM);
        }
        return InputRowPlusRaw.of(row, next != null ? StringUtils.toUtf8(next.toString()) : null);
      }
      catch (ParseException e) {
        final Object next = raw;
        return InputRowPlusRaw.of(next != null ? StringUtils.toUtf8(next.toString()) : null, e);
      }
    }
  }

  @Override
  public void close()
  {
    synchronized (this) {
      closed = true;
      notifyAll();
    }
  }
}
