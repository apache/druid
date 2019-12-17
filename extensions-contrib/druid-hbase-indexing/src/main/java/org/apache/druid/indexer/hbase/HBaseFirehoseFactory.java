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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.indexer.hbase.input.HBaseConnectionConfig;
import org.apache.druid.indexer.hbase.input.ScanInfo;
import org.apache.druid.indexer.hbase.input.TableScanInfo;
import org.apache.druid.indexer.hbase.util.HBaseUtil;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.transform.TransformingInputRowParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HBaseFirehoseFactory
    implements FiniteFirehoseFactory<TransformingInputRowParser<Result>, Scan>
{
  private static final Logger LOG = LogManager.getLogger(HBaseFirehoseFactory.class);

  private final HBaseConnectionConfig connectionConfig;
  private final ScanInfo scanInfo;
  private final boolean splitByRegion;
  private final int taskCount;
  private final Map<String, Object> hbaseClientConfig;

  private List<HRegionInfo> regionInfoList;

  @JsonCreator
  public HBaseFirehoseFactory(
      @JsonProperty("connectionConfig") HBaseConnectionConfig connectionConfig,
      @JsonProperty("scanInfo") ScanInfo scanInfo,
      @JsonProperty("splitByRegion") boolean splitByRegion,
      @JsonProperty("taskCount") int taskCount,
      @Nullable @JsonProperty("hbaseClientConfig") Map<String, Object> hbaseClientConfig)
  {
    this.connectionConfig = connectionConfig;
    this.scanInfo = scanInfo;
    this.splitByRegion = splitByRegion;
    this.taskCount = taskCount;
    this.hbaseClientConfig = hbaseClientConfig;
  }

  @Override
  public Firehose connect(TransformingInputRowParser<Result> parser, File temporaryDirectory)
      throws IOException, ParseException
  {
    List<Scan> scanList = getSplits(null).map(inputSplit -> {
      return inputSplit.get();
    }).collect(Collectors.toList());

    Firehose firehose = null;

    if (scanInfo instanceof TableScanInfo) {
      Connection connection = HBaseUtil.getConnection(connectionConfig, hbaseClientConfig);
      firehose = new HBaseTableFirehose(connection.getTable(TableName.valueOf(scanInfo.getName())),
          scanList.iterator(), parser, connection);
    } else {
      Configuration conf = HBaseUtil.getConfiguration(connectionConfig, hbaseClientConfig);
      HBaseUtil.authenticate(conf, connectionConfig);
      firehose = new HBaseSnapshotFirehose(conf, scanInfo, scanList.iterator(), parser);
    }

    return firehose;
  }

  @Override
  public Stream<InputSplit<Scan>> getSplits(SplitHintSpec splitHintSpec) throws IOException
  {
    List<Scan> scanList = null;

    if (regionInfoList == null) {
      regionInfoList = HBaseUtil.getRegionInfo(connectionConfig, scanInfo, hbaseClientConfig);
    }

    if (splitByRegion) {
      scanList = HBaseUtil.getScanList(regionInfoList);
    } else {
      scanList = HBaseUtil.getScanList(scanInfo, taskCount, regionInfoList);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("scan splitted({}): {}", scanList.size(), scanList);
    }

    return scanList.stream().map(InputSplit::new);
  }

  @Override
  public int getNumSplits(SplitHintSpec splitHintSpec) throws IOException
  {
    int numSplits = taskCount;
    if (splitByRegion) {
      if (regionInfoList == null) {
        regionInfoList = HBaseUtil.getRegionInfo(connectionConfig, scanInfo, hbaseClientConfig);
      }

      numSplits = regionInfoList.size();
    }

    return numSplits;
  }

  @Override
  public FiniteFirehoseFactory<TransformingInputRowParser<Result>, Scan> withSplit(
      InputSplit<Scan> split)
  {
    Scan scan = split.get();
    ScanInfo newScanInfo = scanInfo.createNewScan(convertToString(scan.getStartRow()),
        convertToString(scan.getStopRow()));

    return new HBaseFirehoseFactory(connectionConfig, newScanInfo, false, 1, hbaseClientConfig);
  }

  private String convertToString(byte[] value)
  {
    return Bytes.toStringBinary(value);
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

  @JsonProperty
  public boolean isSplitByRegion()
  {
    return splitByRegion;
  }

  @JsonProperty
  public int getTaskCount()
  {
    return taskCount;
  }

}
