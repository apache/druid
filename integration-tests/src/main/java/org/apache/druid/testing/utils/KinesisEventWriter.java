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

package org.apache.druid.testing.utils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.util.AwsHostNameUtils;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.Properties;

public class KinesisEventWriter implements StreamEventWriter
{
  private final KinesisProducer kinesisProducer;

  public KinesisEventWriter(String endpoint, boolean aggregate) throws Exception
  {
    String pathToConfigFile = System.getProperty("override.config.path");
    Properties prop = new Properties();
    prop.load(new FileInputStream(pathToConfigFile));

    AWSStaticCredentialsProvider credentials = new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
            prop.getProperty("druid_kinesis_accessKey"),
            prop.getProperty("druid_kinesis_secretKey")
        )
    );

    KinesisProducerConfiguration kinesisProducerConfiguration = new KinesisProducerConfiguration()
        .setCredentialsProvider(credentials)
        .setRegion(AwsHostNameUtils.parseRegion(endpoint, null))
        .setRequestTimeout(600000L)
        .setConnectTimeout(300000L)
        .setRecordTtl(9223372036854775807L)
        .setMetricsLevel("none")
        .setAggregationEnabled(aggregate);

    this.kinesisProducer = new KinesisProducer(kinesisProducerConfiguration);
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
    kinesisProducer.addUserRecord(
        streamName,
        DigestUtils.sha1Hex(event),
        ByteBuffer.wrap(event)
    );
  }

  @Override
  public void close()
  {
    flush();
  }

  @Override
  public void flush()
  {
    kinesisProducer.flushSync();
    ITRetryUtil.retryUntil(
        () -> kinesisProducer.getOutstandingRecordsCount() == 0,
        true,
        10000,
        30,
        "Waiting for all Kinesis writes to be flushed"
    );
  }
}
