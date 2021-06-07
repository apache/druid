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

package org.apache.druid.data.input.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Extension of {@link InputRowSchema} with a custom {@link TimestampSpec} to support timestamp extraction for
 * complex timestamp types
 */
public class ProtobufInputRowSchema extends InputRowSchema
{
  public ProtobufInputRowSchema(InputRowSchema inputRowSchema)
  {
    super(new ProtobufTimestampSpec(inputRowSchema.getTimestampSpec()), inputRowSchema.getDimensionsSpec(), inputRowSchema.getColumnsFilter());
  }

  static class ProtobufTimestampSpec extends TimestampSpec
  {
    public ProtobufTimestampSpec(TimestampSpec timestampSpec)
    {
      super(timestampSpec.getTimestampColumn(), timestampSpec.getTimestampFormat(), timestampSpec.getMissingValue());
    }

    /**
     * Extracts the timestamp from the record. If the timestamp column is of complex type such as {@link Timestamp}, then the timestamp
     * is first serialized to string via {@link JsonFormat}. Directly calling {@code toString()} on {@code Timestamp}
     * returns an unparseable string.
     */
    @Override
    @Nullable
    public DateTime extractTimestamp(@Nullable Map<String, Object> input)
    {
      Object rawTimestamp = getRawTimestamp(input);
      if (rawTimestamp instanceof Message) {
        try {
          String timestampStr = JsonFormat.printer().print((Message) rawTimestamp);
          return parseDateTime(timestampStr);
        }
        catch (InvalidProtocolBufferException e) {
          throw new ParseException(e, "Protobuf message could not be parsed");
        }
      } else {
        return parseDateTime(rawTimestamp);
      }
    }
  }
}
