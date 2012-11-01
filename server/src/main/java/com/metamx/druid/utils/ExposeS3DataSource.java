/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.utils;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeSet;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.cli.CommandLine;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.metamx.common.Granularity;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.common.s3.S3Utils;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.zk.StringZkSerializer;

/**
 */
public class ExposeS3DataSource
{
  private static final Logger log = new Logger(ExposeS3DataSource.class);
  private static final Joiner JOINER = Joiner.on("/");
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  public static void main(String[] args) throws ServiceException, IOException, NoSuchAlgorithmException
  {
    CLI cli = new CLI();
    cli.addOption(new RequiredOption(null, "s3Bucket", true, "s3 bucket to pull data from"));
    cli.addOption(
        new RequiredOption(
            null, "s3Path", true, "base input path in s3 bucket.  Everything until the date strings."
        )
    );
    cli.addOption(new RequiredOption(null, "timeInterval", true, "ISO8601 interval of dates to index"));
    cli.addOption(
        new RequiredOption(
            null,
            "granularity",
            true,
            String.format(
                "granularity of index, supported granularities: [%s]", Arrays.asList(Granularity.values())
            )
        )
    );
    cli.addOption(new RequiredOption(null, "zkCluster", true, "Cluster string to connect to ZK with."));
    cli.addOption(new RequiredOption(null, "zkBasePath", true, "The base path to register index changes to."));

    CommandLine commandLine = cli.parse(args);

    if (commandLine == null) {
      return;
    }

    String s3Bucket = commandLine.getOptionValue("s3Bucket");
    String s3Path = commandLine.getOptionValue("s3Path");
    String timeIntervalString = commandLine.getOptionValue("timeInterval");
    String granularity = commandLine.getOptionValue("granularity");
    String zkCluster = commandLine.getOptionValue("zkCluster");
    String zkBasePath = commandLine.getOptionValue("zkBasePath");

    Interval timeInterval = new Interval(timeIntervalString);
    Granularity gran = Granularity.valueOf(granularity.toUpperCase());
    final RestS3Service s3Client = new RestS3Service(
        new AWSCredentials(
            System.getProperty("com.metamx.aws.accessKey"),
            System.getProperty("com.metamx.aws.secretKey")
        )
    );
    ZkClient zkClient = new ZkClient(
        new ZkConnection(zkCluster),
        Integer.MAX_VALUE,
        new StringZkSerializer()
    );

    zkClient.waitUntilConnected();

    for (Interval interval : gran.getIterable(timeInterval)) {
      log.info("Processing interval[%s]", interval);
      String s3DatePath = JOINER.join(s3Path, gran.toPath(interval.getStart()));
      if (!s3DatePath.endsWith("/")) {
        s3DatePath += "/";
      }

      StorageObjectsChunk chunk = s3Client.listObjectsChunked(s3Bucket, s3DatePath, "/", 2000, null, true);
      TreeSet<String> commonPrefixes = Sets.newTreeSet();
      commonPrefixes.addAll(Arrays.asList(chunk.getCommonPrefixes()));

      if (commonPrefixes.isEmpty()) {
        log.info("Nothing at s3://%s/%s", s3Bucket, s3DatePath);
        continue;
      }

      String latestPrefix = commonPrefixes.last();

      log.info("Latest segments at [s3://%s/%s]", s3Bucket, latestPrefix);

      chunk = s3Client.listObjectsChunked(s3Bucket, latestPrefix, "/", 2000, null, true);
      Integer partitionNumber;
      if (chunk.getCommonPrefixes().length == 0) {
        partitionNumber = null;
      } else {
        partitionNumber = -1;
        for (String partitionPrefix : chunk.getCommonPrefixes()) {
          String[] splits = partitionPrefix.split("/");
          partitionNumber = Math.max(partitionNumber, Integer.parseInt(splits[splits.length - 1]));
        }
      }

      log.info("Highest segment partition[%,d]", partitionNumber);

      if (partitionNumber == null) {
        final S3Object s3Obj = new S3Object(new S3Bucket(s3Bucket), String.format("%sdescriptor.json", latestPrefix));
        updateWithS3Object(zkBasePath, s3Client, zkClient, s3Obj);
      } else {
        for (int i = partitionNumber; i >= 0; --i) {
          final S3Object partitionObject = new S3Object(
              new S3Bucket(s3Bucket),
              String.format("%s%s/descriptor.json", latestPrefix, i)
          );

          updateWithS3Object(zkBasePath, s3Client, zkClient, partitionObject);
        }
      }
    }
  }

  private static void updateWithS3Object(
      String zkBasePath, RestS3Service s3Client, ZkClient zkClient, S3Object partitionObject
  ) throws ServiceException, IOException
  {
    log.info("Looking for object[%s]", partitionObject);

    String descriptor;
    try {
      descriptor = S3Utils.getContentAsString(s3Client, partitionObject);
    }
    catch (S3ServiceException e) {
      log.info(e, "Problem loading descriptor for partitionObject[%s]: %s", partitionObject, e.getMessage());
      return;
    }
    Map<String, Object> map = jsonMapper.readValue(descriptor, new TypeReference<Map<String, Object>>(){});
    DataSegment segment;
    if (map.containsKey("partitionNum") && "single".equals(MapUtils.getMap(map, "shardSpec").get("type"))) {
      MapUtils.getMap(map, "shardSpec").put("partitionNum", map.get("partitionNum"));
      segment = jsonMapper.convertValue(map, DataSegment.class);
    }
    else {
      segment = jsonMapper.readValue(descriptor, DataSegment.class);
    }

    final String dataSourceBasePath = JOINER.join(zkBasePath, segment.getDataSource());
    if (!zkClient.exists(dataSourceBasePath)) {
      zkClient.createPersistent(
          dataSourceBasePath, jsonMapper.writeValueAsString(ImmutableMap.of("created", new DateTime().toString()))
      );
    }

    String zkPath = JOINER.join(zkBasePath, segment.getDataSource(), segment.getIdentifier());
    if (!zkClient.exists(zkPath)) {
      log.info("Adding descriptor to zkPath[%s]: %s", zkPath, descriptor);
      zkClient.createPersistent(zkPath, descriptor);
    }
  }
}
