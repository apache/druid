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
import org.apache.druid.indexer.hbase.input.ScanInfo;
import org.apache.druid.indexer.hbase.input.SnapshotScanInfo;
import org.apache.druid.indexer.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableSnapshotScanner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

public class HBaseSnapshotFirehose extends HBaseFirehose
{

  private static final Logger LOG = LogManager.getLogger(HBaseSnapshotFirehose.class);

  private final Configuration conf;
  private final SnapshotScanInfo scanInfo;
  private final Path restoreDirPath;

  public HBaseSnapshotFirehose(Configuration conf, ScanInfo scanInfo, Iterator<Scan> scanIterator,
      InputRowParser<Result> parser) throws IOException
  {
    super(scanIterator, parser);

    this.conf = conf;

    String hbaseRootDir = conf.get(HConstants.HBASE_DIR);
    if (hbaseRootDir == null) {
      conf.set(HConstants.HBASE_DIR, "/hbase");
    }

    this.scanInfo = (SnapshotScanInfo) scanInfo;
    String restoreDir = this.scanInfo.getRestoreDir();
    this.restoreDirPath = new Path(restoreDir == null ? conf.get("hadoop.tmp.dir") : restoreDir);

    if (LOG.isInfoEnabled()) {
      LOG.info("{} created with {} parser and restore dir is {}, root dir is {}", getClass(), parser.getClass(),
          restoreDirPath, hbaseRootDir); // TransformingInputRowParser
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.druid.data.input.Firehose#close()
   */
  @Override
  public void close() throws IOException
  {
    super.close();

    try (Closeable ignore = scanner) {
    }
  }

  @Override
  protected ResultScanner createScanner(Scan scan) throws IOException
  {
    URI originUri = HBaseUtil.replaceFileSystem(conf, restoreDirPath);
    ResultScanner resultScanner = new TableSnapshotScanner(conf, restoreDirPath, scanInfo.getName(),
        scan);
    HBaseUtil.restoreFileSystem(conf, originUri);

    return resultScanner;
  }
}
