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

package org.apache.druid.indexer.hbase;

import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class HBaseFirehose implements Firehose
{

  protected final Logger log = LogManager.getLogger(getClass());

  protected final Iterator<Scan> scanIterator;
  protected final InputRowParser<Result> parser;

  protected ResultScanner scanner;
  protected Result result;
  protected long readRows;
  private boolean initialized;

  public HBaseFirehose(Iterator<Scan> scanIterator, InputRowParser<Result> parser)
  {
    this.scanIterator = scanIterator;
    this.parser = parser;
  }

  @Override
  public boolean hasMore()
  {
    if (!initialized) {
      nextResult();
      initialized = true;
    }

    boolean more = result != null;
    if (!more) {
      if (log.isInfoEnabled()) {
        log.info("Total read rows: {}", readRows);
      }
    }

    return more;
  }

  private ResultScanner getNextScanner() throws IOException
  {
    if (scanner != null) {
      scanner.close();
    }

    Scan scan = scanIterator.next();

    if (log.isDebugEnabled()) {
      log.debug("scan: {}", scan);
    }

    scanner = createScanner(scan);

    return scanner;
  }

  protected abstract ResultScanner createScanner(Scan scan) throws IOException;

  @Override
  public InputRow nextRow()
  {
    InputRow inputRow;

    if (result == null) {
      result = nextResult();
    }

    if (log.isTraceEnabled()) {
      log.trace("result[{}]: {}", readRows, result);
    }

    if (result == null) {
      throw new NoSuchElementException();
    }

    inputRow = parser.parseBatch(result).get(0);
    readRows++;
    result = nextResult();

    return inputRow;
  }

  private Result nextResult()
  {
    try {
      if ((scanner == null || (result = scanner.next()) == null) && scanIterator.hasNext()) {
        scanner = getNextScanner();

        result = scanner.next();
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return result;
  }

  @SuppressWarnings("deprecation")
  @Override
  public InputRowListPlusRawValues nextRowWithRaw()
  {
    if (!hasMore()) {
      throw new NoSuchElementException();
    }

    try {
      InputRow inputRow = parser.parse(result);
      return InputRowListPlusRawValues.of(inputRow, null);
    }
    catch (ParseException e) {
      return InputRowListPlusRawValues.of(null, e);
    }
    finally {
      result = null;
    }
  }

  @Override
  public void close() throws IOException
  {
    if (log.isInfoEnabled()) {
      log.info("Read rows until close: {}", readRows);
    }
  }

}
