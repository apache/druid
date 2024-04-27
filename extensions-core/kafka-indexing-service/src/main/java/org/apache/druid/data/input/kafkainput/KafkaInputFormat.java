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

package org.apache.druid.data.input.kafkainput;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.DateTimes;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Objects;

public class KafkaInputFormat implements InputFormat
{
  private static final String DEFAULT_HEADER_COLUMN_PREFIX = "kafka.header.";
  private static final String DEFAULT_TIMESTAMP_COLUMN_NAME = "kafka.timestamp";
  private static final String DEFAULT_TOPIC_COLUMN_NAME = "kafka.topic";
  private static final String DEFAULT_KEY_COLUMN_NAME = "kafka.key";
  public static final String DEFAULT_AUTO_TIMESTAMP_STRING = "__kif_auto_timestamp";

  // Since KafkaInputFormat blends data from header, key and payload, timestamp spec can be pointing to an attribute within one of these
  // 3 sections. To handle scenarios where there is no timestamp value either in key or payload, we induce an artifical timestamp value
  // to avoid unnecessary parser barf out. Users in such situations can use the inputFormat's kafka record timestamp as its primary timestamp.
  private final TimestampSpec dummyTimestampSpec = new TimestampSpec(DEFAULT_AUTO_TIMESTAMP_STRING, "auto", DateTimes.EPOCH);

  private final KafkaHeaderFormat headerFormat;
  private final InputFormat valueFormat;
  private final InputFormat keyFormat;
  private final String headerColumnPrefix;
  private final String keyColumnName;
  private final String timestampColumnName;
  private final String topicColumnName;

  public KafkaInputFormat(
      @JsonProperty("headerFormat") @Nullable KafkaHeaderFormat headerFormat,
      @JsonProperty("keyFormat") @Nullable InputFormat keyFormat,
      @JsonProperty("valueFormat") InputFormat valueFormat,
      @JsonProperty("headerColumnPrefix") @Nullable String headerColumnPrefix,
      @JsonProperty("keyColumnName") @Nullable String keyColumnName,
      @JsonProperty("timestampColumnName") @Nullable String timestampColumnName,
      @JsonProperty("topicColumnName") @Nullable String topicColumnName
  )
  {
    this.headerFormat = headerFormat;
    this.keyFormat = keyFormat;
    this.valueFormat = Preconditions.checkNotNull(valueFormat, "valueFormat must not be null");
    this.headerColumnPrefix = headerColumnPrefix != null ? headerColumnPrefix : DEFAULT_HEADER_COLUMN_PREFIX;
    this.keyColumnName = keyColumnName != null ? keyColumnName : DEFAULT_KEY_COLUMN_NAME;
    this.timestampColumnName = timestampColumnName != null ? timestampColumnName : DEFAULT_TIMESTAMP_COLUMN_NAME;
    this.topicColumnName = topicColumnName != null ? topicColumnName : DEFAULT_TOPIC_COLUMN_NAME;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    final SettableByteEntity<KafkaRecordEntity> settableByteEntitySource;
    if (source instanceof SettableByteEntity) {
      settableByteEntitySource = (SettableByteEntity<KafkaRecordEntity>) source;
    } else {
      settableByteEntitySource = new SettableByteEntity<>();
      settableByteEntitySource.setEntity((KafkaRecordEntity) source);
    }
    InputRowSchema newInputRowSchema = new InputRowSchema(
        dummyTimestampSpec,
        inputRowSchema.getDimensionsSpec(),
        inputRowSchema.getColumnsFilter(),
        inputRowSchema.getMetricNames()
    );
    return new KafkaInputReader(
        inputRowSchema,
        settableByteEntitySource,
        (headerFormat == null) ?
            null :
            record -> headerFormat.createReader(record.getRecord().headers(), headerColumnPrefix),
        (keyFormat == null) ?
            null :
            record ->
                (record.getRecord().key() == null) ?
                    null :
                    JsonInputFormat.withLineSplittable(keyFormat, false).createReader(
                        newInputRowSchema,
                        new ByteEntity(record.getRecord().key()),
                        temporaryDirectory
                    ),
        JsonInputFormat.withLineSplittable(valueFormat, false).createReader(
            newInputRowSchema,
            source,
            temporaryDirectory
        ),
        keyColumnName,
        timestampColumnName,
        topicColumnName
    );
  }

  @Nullable
  @JsonProperty
  public KafkaHeaderFormat getHeaderFormat()
  {
    return headerFormat;
  }

  @JsonProperty
  public InputFormat getValueFormat()
  {
    return valueFormat;
  }

  @Nullable
  @JsonProperty
  public InputFormat getKeyFormat()
  {
    return keyFormat;
  }

  @Nullable
  @JsonProperty
  public String getHeaderColumnPrefix()
  {
    return headerColumnPrefix;
  }

  @Nullable
  @JsonProperty
  public String getKeyColumnName()
  {
    return keyColumnName;
  }

  @Nullable
  @JsonProperty
  public String getTimestampColumnName()
  {
    return timestampColumnName;
  }

  @Nullable
  @JsonProperty
  public String getTopicColumnName()
  {
    return topicColumnName;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KafkaInputFormat that = (KafkaInputFormat) o;
    return Objects.equals(headerFormat, that.headerFormat)
           && Objects.equals(valueFormat, that.valueFormat)
           && Objects.equals(keyFormat, that.keyFormat)
           && Objects.equals(headerColumnPrefix, that.headerColumnPrefix)
           && Objects.equals(keyColumnName, that.keyColumnName)
           && Objects.equals(timestampColumnName, that.timestampColumnName)
           && Objects.equals(topicColumnName, that.topicColumnName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(headerFormat, valueFormat, keyFormat,
                        headerColumnPrefix, keyColumnName, timestampColumnName, topicColumnName
    );
  }
}
