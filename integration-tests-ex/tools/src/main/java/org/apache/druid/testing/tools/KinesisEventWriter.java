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

package org.apache.druid.testing.tools;

import org.apache.commons.codec.digest.DigestUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.io.FileInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KinesisEventWriter implements StreamEventWriter
{
  private static final Pattern REGION_PATTERN = Pattern.compile("kinesis\\.([a-z0-9-]+)\\.amazonaws\\.com");
  private static final int MAX_BATCH_SIZE = 500; // Kinesis limit for PutRecords

  private final KinesisClient kinesisClient;
  private final List<PendingRecord> pendingRecords = new ArrayList<>();
  private String currentStreamName;

  public KinesisEventWriter(String endpoint, boolean aggregate) throws Exception
  {
    String pathToConfigFile = System.getProperty("override.config.path");
    Properties prop = new Properties();
    prop.load(new FileInputStream(pathToConfigFile));

    StaticCredentialsProvider credentials = StaticCredentialsProvider.create(
        AwsBasicCredentials.create(
            prop.getProperty("druid_kinesis_accessKey"),
            prop.getProperty("druid_kinesis_secretKey")
        )
    );

    KinesisClientBuilder builder = KinesisClient.builder()
        .credentialsProvider(credentials);

    if (endpoint != null && !endpoint.isEmpty()) {
      URI endpointUri = URI.create(endpoint);
      if (endpointUri.getScheme() != null) {
        builder.endpointOverride(endpointUri);
        Region region = parseRegionFromEndpoint(endpoint);
        if (region != null) {
          builder.region(region);
        }
      }
    }

    this.kinesisClient = builder.build();
    // Note: aggregate parameter is kept for API compatibility but aggregation
    // is not implemented in v2 SDK migration (KPL aggregation was proprietary)
  }

  private static Region parseRegionFromEndpoint(String endpoint)
  {
    if (endpoint == null) {
      return null;
    }
    String lowerEndpoint = endpoint.toLowerCase(Locale.ENGLISH);
    Matcher matcher = REGION_PATTERN.matcher(lowerEndpoint);
    if (matcher.find()) {
      return Region.of(matcher.group(1));
    }
    // For LocalStack or custom endpoints
    if (lowerEndpoint.contains("localhost") || lowerEndpoint.contains("127.0.0.1")) {
      return Region.US_EAST_1;
    }
    return null;
  }

  @Override
  public boolean supportTransaction()
  {
    return false;
  }

  @Override
  public boolean isTransactionEnabled()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void initTransaction()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commitTransaction()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(String streamName, byte[] event)
  {
    synchronized (pendingRecords) {
      currentStreamName = streamName;
      pendingRecords.add(new PendingRecord(streamName, event));

      // Auto-flush if batch is full
      if (pendingRecords.size() >= MAX_BATCH_SIZE) {
        flushInternal();
      }
    }
  }

  @Override
  public void close()
  {
    flush();
    kinesisClient.close();
  }

  @Override
  public void flush()
  {
    synchronized (pendingRecords) {
      flushInternal();
    }
  }

  private void flushInternal()
  {
    if (pendingRecords.isEmpty()) {
      return;
    }

    // Group records by stream name
    List<PutRecordsRequestEntry> entries = new ArrayList<>();
    String streamName = currentStreamName;

    for (PendingRecord record : pendingRecords) {
      entries.add(PutRecordsRequestEntry.builder()
          .data(SdkBytes.fromByteArray(record.data))
          .partitionKey(DigestUtils.sha1Hex(record.data))
          .build());

      // Flush batch when we hit the limit
      if (entries.size() >= MAX_BATCH_SIZE) {
        sendBatch(streamName, entries);
        entries.clear();
      }
    }

    // Send remaining records
    if (!entries.isEmpty()) {
      sendBatch(streamName, entries);
    }

    pendingRecords.clear();
  }

  private void sendBatch(String streamName, List<PutRecordsRequestEntry> entries)
  {
    ITRetryUtil.retryUntil(
        () -> {
          try {
            kinesisClient.putRecords(PutRecordsRequest.builder()
                .streamName(streamName)
                .records(entries)
                .build());
            return true;
          }
          catch (Exception e) {
            return false;
          }
        },
        true,
        1000,
        30,
        "Sending batch to Kinesis"
    );
  }

  protected KinesisClient getKinesisClient()
  {
    return kinesisClient;
  }

  private static class PendingRecord
  {
    final String streamName;
    final byte[] data;

    PendingRecord(String streamName, byte[] data)
    {
      this.streamName = streamName;
      this.data = data;
    }
  }
}
