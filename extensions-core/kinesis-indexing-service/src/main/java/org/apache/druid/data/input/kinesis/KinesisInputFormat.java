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

package org.apache.druid.data.input.kinesis;

import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.DateTimes;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Objects;

/**
 * Kinesis aware InputFormat. Allows for reading kinesis specific values that are stored in the {@link Record}. At
 * this time, this input format only supports reading the main record payload ({@link Record#data}) and
 * {@link Record#approximateArrivalTimestamp}, but can be extended easily to read other fields.
 */
public class KinesisInputFormat implements InputFormat
{
  private static final String DEFAULT_TIMESTAMP_COLUMN_NAME = "kinesis.timestamp";
  public static final String DEFAULT_AUTO_TIMESTAMP_STRING = "__kif_auto_timestamp";

  // Since KinesisInputFormat blends data from headers, and payload, timestamp spec can be pointing to an attribute within one of these
  // 2 sections. To handle scenarios where there is no timestamp value either in payload or headers, we induce an artifical timestamp value
  // to avoid unnecessary parser barf out. Users in such situations can use the inputFormat's kinesis record timestamp as its primary timestamp.
  private final TimestampSpec dummyTimestampSpec = new TimestampSpec(DEFAULT_AUTO_TIMESTAMP_STRING, "auto", DateTimes.EPOCH);

  private final InputFormat valueFormat;
  private final String timestampColumnName;

  public KinesisInputFormat(
      @JsonProperty("valueFormat") InputFormat valueFormat,
      @JsonProperty("timestampColumnName") @Nullable String timestampColumnName
  )
  {
    this.valueFormat = Preconditions.checkNotNull(valueFormat, "valueFormat must not be null");
    this.timestampColumnName = timestampColumnName != null ? timestampColumnName : DEFAULT_TIMESTAMP_COLUMN_NAME;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    final SettableByteEntity<KinesisRecordEntity> settableByteEntitySource;
    if (source instanceof SettableByteEntity) {
      settableByteEntitySource = (SettableByteEntity<KinesisRecordEntity>) source;
    } else {
      settableByteEntitySource = new SettableByteEntity<>();
      settableByteEntitySource.setEntity((KinesisRecordEntity) source);
    }
    InputRowSchema newInputRowSchema = new InputRowSchema(
        dummyTimestampSpec,
        inputRowSchema.getDimensionsSpec(),
        inputRowSchema.getColumnsFilter(),
        inputRowSchema.getMetricNames()
    );
    return new KinesisInputReader(
        inputRowSchema,
        settableByteEntitySource,
        JsonInputFormat.withLineSplittable(valueFormat, false).createReader(
            newInputRowSchema,
            source,
            temporaryDirectory
        ),
        timestampColumnName
    );
  }

  @JsonProperty
  public InputFormat getValueFormat()
  {
    return valueFormat;
  }

  @Nullable
  @JsonProperty
  public String getTimestampColumnName()
  {
    return timestampColumnName;
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
    KinesisInputFormat that = (KinesisInputFormat) o;
    return Objects.equals(valueFormat, that.valueFormat)
           && Objects.equals(timestampColumnName, that.timestampColumnName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(valueFormat, timestampColumnName);
  }
}
