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

package org.apache.druid.indexer.hbase.path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.druid.indexer.hbase.input.HBaseConnectionConfig;
import org.apache.druid.indexer.hbase.input.ScanInfo;
import org.apache.druid.indexer.hbase.input.SnapshotScanInfo;
import org.apache.druid.indexer.hbase.input.TableScanInfo;
import org.apache.druid.indexer.hbase.util.HBaseUtil;
import org.apache.druid.indexer.path.PathSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapreduce.Job;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;

public class HBasePathSpec implements PathSpec
{
  private final HBaseConnectionConfig connectionConfig;
  private final ScanInfo scanInfo;
  private final Map<String, Object> hbaseClientConfig;

  @JsonCreator
  public HBasePathSpec(
      @Nullable @JsonProperty("connectionConfig") HBaseConnectionConfig connectionConfig,
      @JsonProperty("scanInfo") ScanInfo scanInfo,
      @Nullable @JsonProperty("hbaseClientConfig") Map<String, Object> hbaseClientConfig)
  {
    this.connectionConfig = connectionConfig;
    this.scanInfo = scanInfo;
    this.hbaseClientConfig = hbaseClientConfig;
  }

  @JsonProperty
  public HBaseConnectionConfig getConnectionConfig()
  {
    return connectionConfig;
  }

  @JsonProperty
  public ScanInfo getScanInfo()
  {
    return scanInfo;
  }

  @JsonProperty
  public Map<String, Object> getHbaseClientConfig()
  {
    return hbaseClientConfig;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.druid.indexer.path.PathSpec#addInputPaths(org.apache.druid.
   * indexer. HadoopDruidIndexerConfig, org.apache.hadoop.mapreduce.Job)
   */
  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    Configuration conf = job.getConfiguration();
    HBaseUtil.applySpecConfig(conf, connectionConfig, hbaseClientConfig);
    HBaseUtil.authenticate(conf, connectionConfig);

    Scan scan = HBaseUtil.getScanList(scanInfo, 1, Collections.emptyList()).get(0);
    conf.set(TableInputFormat.SCAN, convertScanToString(scan));

    if (scanInfo instanceof TableScanInfo) {
      job.setInputFormatClass(TableInputFormat.class);
      conf.set(TableInputFormat.INPUT_TABLE, scanInfo.getName());
    } else {
      job.setInputFormatClass(TableSnapshotInputFormat.class);
      SnapshotScanInfo snapshotScanInfo = (SnapshotScanInfo) scanInfo;
      String restoreDir = snapshotScanInfo.getRestoreDir();
      Path restoreDirPath = new Path(restoreDir == null ? conf.get("hadoop.tmp.dir") : restoreDir);

      URI originUri = HBaseUtil.replaceFileSystem(conf, restoreDirPath);
      TableSnapshotInputFormat.setInput(job, snapshotScanInfo.getName(), restoreDirPath);
      HBaseUtil.restoreFileSystem(conf, originUri);
    }

    TableMapReduceUtil.initCredentials(job);

    return job;
  }

  static String convertScanToString(Scan scan) throws IOException
  {
    ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
    return Base64.encodeBytes(proto.toByteArray());
  }
}
