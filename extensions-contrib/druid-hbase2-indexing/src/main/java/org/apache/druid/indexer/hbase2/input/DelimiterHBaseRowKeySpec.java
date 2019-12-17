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

package org.apache.druid.indexer.hbase2.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DelimiterHBaseRowKeySpec extends HBaseRowKeySpec
{

  private final String delimiter;
  private final byte delimiterByte;

  private final List<HBaseRowKeySchema> rowkeySchemaList;

  @JsonCreator
  public DelimiterHBaseRowKeySpec(@JsonProperty("delimiter") String delimiter,
      @JsonProperty("columns") List<HBaseRowKeySchema> rowkeySchemaList)
  {
    super(rowkeySchemaList);
    this.delimiter = delimiter;
    delimiterByte = Bytes.toBytes(delimiter)[0];
    this.rowkeySchemaList = rowkeySchemaList;
  }

  @JsonProperty
  public String getDelimiter()
  {
    return delimiter;
  }

  @Override
  public Map<String, Object> getRowKeyColumns(Result result)
  {
    byte[] rowKey = result.getRow();
    int[] position = {0};
    return rowkeySchemaList.stream().map(s -> {
      return new Pair<String, Object>(s.getName(), getColumnValueOfRowKey(rowKey, s, position));
    }).collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }

  @SuppressWarnings("unchecked")
  private <T> T getColumnValueOfRowKey(byte[] values, HBaseRowKeySchema rowKeySchema, int[] from)
  {
    Object value;
    String type = rowKeySchema.getType();

    if ("string".equals(type)) {
      byte[] temp = Arrays.copyOfRange(values, from[0], values.length);
      int delimiterPosition = Bytes.indexOf(temp, delimiterByte);
      if (delimiterPosition > 0) {
        value = Bytes.toString(temp, 0, delimiterPosition);
      } else {
        value = Bytes.toString(temp);
      }

      from[0] = delimiterPosition + 1;
    } else if ("int".equals(type)) {
      value = Bytes.toInt(values, from[0]);
      from[0] += Integer.BYTES + 1;
    } else if ("long".equals(type)) {
      value = Bytes.toLong(values, from[0]);
      from[0] += Long.BYTES + 1;
    } else if ("double".equals(type)) {
      value = Bytes.toDouble(values, from[0]);
      from[0] += Double.BYTES + 1;
    } else if ("float".equals(type)) {
      value = Bytes.toFloat(values, from[0]);
      from[0] += Float.BYTES + 1;
    } else if ("boolean".equals(type)) {
      value = Bytes.toBoolean(Arrays.copyOfRange(values, from[0], from[0] + 1));
      from[0] += 2;
    } else {
      throw new RuntimeException("not supported type: " + type);
    }

    return (T) value;
  }
}
