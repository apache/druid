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

package org.apache.druid.indexer.hbase2.util;

import org.apache.druid.indexer.hbase2.input.HBaseColumnSchema;
import org.apache.druid.indexer.hbase2.input.HBaseConnectionConfig;
import org.apache.druid.indexer.hbase2.input.KerberosConfig;
import org.apache.druid.indexer.hbase2.input.ScanInfo;
import org.apache.druid.indexer.hbase2.input.SnapshotScanInfo;
import org.apache.druid.indexer.hbase2.input.TableScanInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class HBaseUtil
{
  private static final Logger LOG = LogManager.getLogger(HBaseUtil.class);

  public static final Pattern LIBRARY_PATTERN = Pattern.compile("([a-zA-Z\\-]+)-((\\d+\\.?)+)-?\\w*(\\.jar)?");

  // from org.apache.hadoop.hbase.util.Bytes
  private static final char[] HEX_CHARS_UPPER = {
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
  };
  private static final Map<Character, Integer> HEX_TO_INT_MAP = IntStream.range(0, 16).mapToObj(i -> {
    return new Pair<Character, Integer>(HEX_CHARS_UPPER[i], i);
  }).collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

  public static int getRegionCount(HBaseConnectionConfig connectionConfig, ScanInfo scanInfo,
      Map<String, Object> hbaseClientConfig)
      throws IOException
  {
    return getRegionInfo(connectionConfig, scanInfo, hbaseClientConfig).size();
  }

  public static List<RegionInfo> getRegionInfo(HBaseConnectionConfig connectionConfig,
      ScanInfo scanInfo, Map<String, Object> hbaseClientConfig) throws IOException
  {
    List<RegionInfo> regionInfoList;
    if (scanInfo instanceof TableScanInfo) {
      try (Connection connection = getConnection(connectionConfig, hbaseClientConfig);
          Admin admin = connection.getAdmin()) {
        regionInfoList = admin.getRegions(TableName.valueOf(scanInfo.getName()));
      }
    } else {
      Configuration conf = getConfiguration(connectionConfig, hbaseClientConfig);
      HBaseUtil.authenticate(conf, connectionConfig);
      Path rootDir = new Path(conf.get(HConstants.HBASE_DIR), "/hbase");
      SnapshotScanInfo snapshotScanInfo = (SnapshotScanInfo) scanInfo;
      String restoreDir = snapshotScanInfo.getRestoreDir();
      Path restoreDirPath = new Path(restoreDir == null ? conf.get("hadoop.tmp.dir") : restoreDir,
          UUID.randomUUID().toString());
      try {
        final RestoreSnapshotHelper.RestoreMetaChanges meta = RestoreSnapshotHelper
            .copySnapshotForScanner(conf, rootDir.getFileSystem(conf), rootDir, restoreDirPath,
                snapshotScanInfo.getName());

        regionInfoList = meta.getRegionsToAdd();
      }
      finally {
        try (FileSystem fs = restoreDirPath.getFileSystem(conf)) {
          fs.delete(restoreDirPath, true);
        }
        catch (IOException e) {
          LOG.warn("Could not delete restore directory for the snapshot: {}", e);
        }
      }
    }

    return regionInfoList;
  }

  public static Connection getConnection(HBaseConnectionConfig connectionConfig, Map<String, Object> hbaseClientConfig)
      throws IOException
  {
    Configuration conf = getConfiguration(connectionConfig, hbaseClientConfig);

    authenticate(conf, connectionConfig);

    return ConnectionFactory.createConnection(conf);
  }

  public static void authenticate(Configuration conf, HBaseConnectionConfig connectionConfig) throws IOException
  {
    if (connectionConfig != null) {
      UserGroupInformation.setConfiguration(conf);
      if (UserGroupInformation.isSecurityEnabled()) {
        KerberosConfig kerberosConfig = connectionConfig.getKerberosConfig();
        if (kerberosConfig != null && kerberosConfig.getPrincipal() != null
            && kerberosConfig.getKeytab() != null) {
          String principal = kerberosConfig.getPrincipal();

          if (UserGroupInformation.getCurrentUser().hasKerberosCredentials() == false
              || !UserGroupInformation.getCurrentUser().getUserName().equals(principal)) {
            UserGroupInformation.loginUserFromKeytab(principal, kerberosConfig.getKeytab());

            if (LOG.isInfoEnabled()) {
              LOG.info("login kerberos user: {}", UserGroupInformation.getLoginUser());
            }
          }
        }
      }
    }
  }

  public static Configuration getConfiguration(HBaseConnectionConfig connectionConfig,
      Map<String, Object> hbaseClientConfig)
  {
    Configuration conf = HBaseConfiguration.create();
    applySpecConfig(conf, connectionConfig, hbaseClientConfig);

    return conf;
  }

  public static void applySpecConfig(Configuration conf, HBaseConnectionConfig connectionConfig,
      Map<String, Object> hbaseClientConfig)
  {
    if (connectionConfig != null) {
      String zkQuorums = connectionConfig.getZookeeperQuorum();
      if (zkQuorums != null) {
        conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorums);
      }

      KerberosConfig kerberosConfig = connectionConfig.getKerberosConfig();
      if (kerberosConfig != null && kerberosConfig.getPrincipal() != null
          && kerberosConfig.getKeytab() != null) {
        String userPrincipal = kerberosConfig.getPrincipal();
        String realm = userPrincipal.substring(userPrincipal.indexOf('@') + 1);
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("hbase.security.authentication", "Kerberos");
        conf.set("hbase.master.kerberos.principal", "hbase/_HOST@" + realm);
        conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@" + realm);
      }
    }

    if (hbaseClientConfig != null) {
      hbaseClientConfig.entrySet().forEach(e -> {
        conf.set(e.getKey(), e.getValue().toString());
      });
    }

    // To avoid ClassNotFoundException thrown by
    // org.apache.hadoop.security.ShellBasedUnixGroupsMapping.
    // Ref:
    // https://github.com/apache/hadoop/blob/release-2.8.3-RC0/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/conf/Configuration.java#L732-L738
    Thread.currentThread().setContextClassLoader(conf.getClassLoader());
  }

  public static List<Scan> getScanList(ScanInfo scanInfo, int splitCount, List<RegionInfo> regionInfoList)
      throws IOException
  {
    List<Scan> scanList;

    String startKey = scanInfo.getStartKey();
    byte[] startKeyBytes = startKey == null ? HConstants.EMPTY_START_ROW
        : deserializeKey(scanInfo.getStartKey());
    String endKey = scanInfo.getEndKey();
    byte[] endKeyBytes = endKey == null ? HConstants.EMPTY_END_ROW : deserializeKey(endKey);

    if (splitCount > 1) {
      // If the number of regions divided by splitCount is greater than 1, the
      // rounding value is the region number. If it's less than 1, divide each
      // region back by dividing 1 by that value.
      double regionGroups = (double) regionInfoList.size() / splitCount;
      if (regionGroups > 1) {
        scanList = getScanListByRegionGroup(regionInfoList, (int) Math.ceil(regionGroups));
      } else if (regionGroups == 1) {
        scanList = getScanList(regionInfoList);
      } else {
        int regionFragments = (int) Math.round(1 / regionGroups);
        scanList = getScanListByBrokenRegion(regionInfoList, regionFragments);
      }
    } else {
      scanList = Collections.singletonList(createScan(startKeyBytes, endKeyBytes));
    }

    return scanList;
  }

  private static List<Scan> getScanListByBrokenRegion(List<RegionInfo> regionInfoList, int regionFragments)
  {
    List<Scan> scanList = regionInfoList.stream().map(ri -> {
      byte[] endKey = Arrays.equals(ri.getEndKey(), HConstants.EMPTY_BYTE_ARRAY) ? Bytes.createMaxByteArray(20)
          : ri.getEndKey();
      return getScanListByKey(ri.getStartKey(), endKey, regionFragments);
    }).flatMap(Collection::stream).collect(Collectors.toList());

    return scanList;
  }

  private static List<Scan> getScanListByRegionGroup(List<RegionInfo> regionInfoList, int regionGroups)
  {
    int regionCount = regionInfoList.size();
    List<Scan> scanList = IntStream.range(0, regionInfoList.size()).filter(i -> i % regionGroups == 0)
        .mapToObj(i -> {
          RegionInfo startRegion = regionInfoList.get(i);
          RegionInfo endRegion = (i + 1) >= regionCount ? startRegion : regionInfoList.get(i + 1);
          return createScan(startRegion.getStartKey(), endRegion.getEndKey());
        }).collect(Collectors.toList());

    return scanList;
  }

  private static List<Scan> getScanListByKey(byte[] startKey, byte[] endKey, int splitCount)
  {
    byte[][] keyRanges = Bytes.split(startKey, endKey, splitCount - 1);
    return IntStream.range(0, keyRanges.length - 1).mapToObj(i -> {
      return createScan(keyRanges[i], keyRanges[i + 1]);
    }).collect(Collectors.toList());
  }

  public static List<Scan> getScanListByRegion(HBaseConnectionConfig connectionConfig,
      ScanInfo scanInfo, Map<String, Object> hbaseClientConfig) throws IOException
  {
    return getRegionInfo(connectionConfig, scanInfo, hbaseClientConfig).stream().map(ri -> {
      return createScan(ri.getStartKey(), ri.getEndKey());
    }).collect(Collectors.toList());
  }

  public static List<Scan> getScanList(List<RegionInfo> regionInfoList)
  {
    return regionInfoList.stream().map(ri -> {
      return createScan(ri.getStartKey(), ri.getEndKey());
    }).collect(Collectors.toList());
  }

  private static Scan createScan(byte[] startKey, byte[] endKey)
  {
    Scan scan = new Scan().withStartRow(startKey).withStopRow(endKey);
    scan.setCacheBlocks(false);
    return scan;
  }

  // Restore the value of org.apache.hadoop.hbase.util.Bytes#toStringBinary.
  private static byte[] deserializeKey(String binaryString) throws IOException
  {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      char[] binaryChars = binaryString.toCharArray();
      for (int i = 0, limit = binaryChars.length; i < limit; i++) {
        char ch = binaryChars[i];
        int value = ch;
        if (ch == '\\' && ((i + 1) < limit && 'x' == binaryChars[i + 1])) {
          if ((i + 3) < limit) {
            value = (HEX_TO_INT_MAP.get(binaryChars[i + 2]) * 0x10 + HEX_TO_INT_MAP.get(binaryChars[i + 3]));
            bos.write(new byte[] {(byte) value});
            i += 3;
          }
        }
        bos.write(new byte[] {(byte) value});
      }

      return bos.toByteArray();
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> T getColumnValue(Result result, HBaseColumnSchema columnSchema)
  {
    Object value = null;
    byte[] valueBytes = result.getValue(columnSchema.getColumnFamiy(), columnSchema.getQualifier());
    if (valueBytes != null) {
      String type = columnSchema.getType();

      if ("string".equals(type)) {
        value = Bytes.toString(valueBytes);
      } else if ("int".equals(type)) {
        value = Bytes.toInt(valueBytes);
      } else if ("long".equals(type)) {
        value = Bytes.toLong(valueBytes);
      } else if ("double".equals(type)) {
        value = Bytes.toDouble(valueBytes);
      } else if ("float".equals(type)) {
        value = Bytes.toFloat(valueBytes);
      } else if ("boolean".equals(type)) {
        value = Bytes.toBoolean(valueBytes);
      } else {
        throw new RuntimeException("not supported type. [" + type + "]");
      }
    }

    return (T) value;
  }

  public static <T> List<T> removeConflictingLibrary(Stream<T> stream, Map<String, String> removingLibMap)
  {
    return stream.filter(lib -> {
      boolean result = true;
      String[] tokens = lib.toString().split("/");
      String file = tokens[tokens.length - 1];

      Matcher m = LIBRARY_PATTERN.matcher(file);
      if (m.find()) {
        final String artifact = m.group(1);
        final String version = m.group(2);

        result = removingLibMap.entrySet().stream()
            .noneMatch(e -> {
              return artifact.startsWith(e.getKey()) && version.startsWith(e.getValue());
            });

        if (!result) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("{} was removed from the classpath due to conflicting.", file);
          }
        }
      }

      return result;
    }).collect(Collectors.toList());
  }
}
