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

import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexer.hbase.input.HBaseInputRowParser;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class HBaseTableFirehose extends HBaseFirehose
{

  private static final Logger LOG = LogManager.getLogger(HBaseTableFirehose.class);

  private final Table table;
  private final Closeable closer;

  public HBaseTableFirehose(Table table, Iterator<Scan> scanIterator, HBaseInputRowParser parser)
  {
    this(table, scanIterator, parser, null);

    if (log.isInfoEnabled()) {
      log.info("{} created with {} parser.", getClass(), parser.getClass()); // TransformingInputRowParser
    }
  }

  public HBaseTableFirehose(Table table, Iterator<Scan> scanIterator, InputRowParser<Result> parser,
      Closeable closer)
  {
    super(scanIterator, parser);

    this.table = table;
    this.closer = closer;

    if (LOG.isInfoEnabled()) {
      LOG.info("{} created with {} parser.", getClass(), parser.getClass()); // TransformingInputRowParser
    }
  }

  @Override
  public void close() throws IOException
  {
    super.close();

    try (Closeable ignore = scanner;
        Closeable ignore2 = table;
        Closeable ignore3 = closer) {
    }
  }

  @Override
  protected ResultScanner createScanner(Scan scan) throws IOException
  {
    return table.getScanner(scan);
  }

}
