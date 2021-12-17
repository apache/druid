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

package org.apache.druid.query.groupby;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.ComparableStringArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ResultRowDeserializer extends JsonDeserializer<ResultRow>
{
  final List<ColumnType> types;
  final GroupByQuery query;

  public ResultRowDeserializer(final List<ColumnType> types, final GroupByQuery query)
  {
    this.types = types;
    this.query = query;
  }

  public static ResultRowDeserializer fromQuery(
      final GroupByQuery query
  )
  {
    RowSignature rowSignature = query.getResultRowSignature();
    final List<ColumnType> types = new ArrayList<>(rowSignature.size());

    for (String name : rowSignature.getColumnNames()) {
      final ColumnType type = rowSignature.getColumnType(name)
                                          .orElseThrow(() -> new ISE("No type for column [%s]", name));

      types.add(type);
    }

    return new ResultRowDeserializer(types, query);

  }

  @Override
  public ResultRow deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
  {
    // Deserializer that can deserialize either array- or map-based rows.
    if (jp.isExpectedStartObjectToken()) {
      final Row row = jp.readValueAs(Row.class);
      return ResultRow.fromLegacyRow(row, query);
    } else if (jp.isExpectedStartArrayToken()) {
      final Object[] retVal = new Object[types.size()];

      for (int i = 0; i < types.size(); i++) {
        final JsonToken token = jp.nextToken();
        switch (types.get(i).getType()) {
          case STRING:
            if (token == JsonToken.VALUE_NULL) {
              retVal[i] = null;
            } else if (token == JsonToken.VALUE_STRING) {
              retVal[i] = jp.getText();
            } else {
              throw ctxt.instantiationException(
                  ResultRow.class,
                  StringUtils.format("Unexpected token [%s] when reading string", token)
              );
            }
            break;

          case LONG:
            retVal[i] = token == JsonToken.VALUE_NULL ? null : jp.getLongValue();
            break;
          case DOUBLE:
            retVal[i] = token == JsonToken.VALUE_NULL ? null : jp.getDoubleValue();
            break;
          case FLOAT:
            retVal[i] = token == JsonToken.VALUE_NULL ? null : jp.getFloatValue();
            break;
          case ARRAY:
            if (types.get(i).equals(ColumnType.STRING_ARRAY)) {
              final List<String> strings = new ArrayList<>();
              while (jp.nextToken() != JsonToken.END_ARRAY) {
                strings.add(jp.getText());
              }
              retVal[i] = ComparableStringArray.of(strings.toArray(new String[0]));
              break;
            }

          default:
            throw new ISE("Can't handle type [%s]", types.get(i).asTypeString());
        }
      }
      if (jp.nextToken() != JsonToken.END_ARRAY) {
        throw ctxt.wrongTokenException(jp, ResultRow.class, JsonToken.END_ARRAY, null);
      }
      return ResultRow.of(retVal);
    } else {
      return (ResultRow) ctxt.handleUnexpectedToken(ResultRow.class, jp);
    }
  }
}
