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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class KafkaInputReader implements InputEntityReader
{
  private static final Logger log = new Logger(KafkaInputReader.class);

  private final InputRowSchema inputRowSchema;
  private final KafkaRecordEntity record;
  private final KafkaHeaderReader headerParser;
  private final InputEntityReader keyParser;
  private final InputEntityReader valueParser;
  private final String keyColumnName;
  private final String timestampColumnName;

  /**
   *
   * @param inputRowSchema Actual schema from the ingestion spec
   * @param record kafka record containing header, key & value
   * @param headerParser Header parser for parsing the header section, kafkaInputFormat allows users to skip header parsing section and hence an be null
   * @param keyParser Key parser for key section, can be null as well
   * @param valueParser Value parser is a required section in kafkaInputFormat, but because of tombstone records we can have a null parser here.
   * @param keyColumnName Default key column name
   * @param timestampColumnName Default kafka record's timestamp column name
   */
  public KafkaInputReader(
      InputRowSchema inputRowSchema,
      KafkaRecordEntity record,
      KafkaHeaderReader headerParser,
      InputEntityReader keyParser,
      InputEntityReader valueParser,
      String keyColumnName,
      String timestampColumnName
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.record = record;
    this.headerParser = headerParser;
    this.keyParser = keyParser;
    this.valueParser = valueParser;
    this.keyColumnName = keyColumnName;
    this.timestampColumnName = timestampColumnName;
  }

  private List<String> getFinalDimensionList(HashSet<String> newDimensions)
  {
    final List<String> schemaDimensions = inputRowSchema.getDimensionsSpec().getDimensionNames();
    if (!schemaDimensions.isEmpty()) {
      return schemaDimensions;
    } else {
      return Lists.newArrayList(
          Sets.difference(newDimensions, inputRowSchema.getDimensionsSpec().getDimensionExclusions())
      );
    }
  }

  private CloseableIterator<InputRow> buildBlendedRows(InputEntityReader valueParser, Map<String, Object> headerKeyList) throws IOException
  {
    return valueParser.read().map(
        r -> {
          MapBasedInputRow valueRow;
          try {
            // Return type for the value parser should be of type MapBasedInputRow
            // Parsers returning other types are not compatible currently.
            valueRow = (MapBasedInputRow) r;
          }
          catch (ClassCastException e) {
            throw new ParseException("Unsupported input format in valueFormat. KafkaInputformat only supports input format that return MapBasedInputRow rows");
          }
          Map<String, Object> event = new HashMap<>(headerKeyList);
          /* Currently we prefer payload attributes if there is a collision in names.
              We can change this beahvior in later changes with a config knob. This default
              behavior lets easy porting of existing inputFormats to the new one without any changes.
            */
          event.putAll(valueRow.getEvent());

          HashSet<String> newDimensions = new HashSet<String>(valueRow.getDimensions());
          newDimensions.addAll(headerKeyList.keySet());
          // Remove the dummy timestamp added in KafkaInputFormat
          newDimensions.remove(KafkaInputFormat.DEFAULT_AUTO_TIMESTAMP_STRING);

          return new MapBasedInputRow(
              inputRowSchema.getTimestampSpec().extractTimestamp(event),
              getFinalDimensionList(newDimensions),
              event
          );
        }
    );
  }

  private CloseableIterator<InputRow> buildRowsWithoutValuePayload(Map<String, Object> headerKeyList)
  {
    HashSet<String> newDimensions = new HashSet<String>(headerKeyList.keySet());
    InputRow row = new MapBasedInputRow(
        inputRowSchema.getTimestampSpec().extractTimestamp(headerKeyList),
        getFinalDimensionList(newDimensions),
        headerKeyList
    );
    List<InputRow> rows = Collections.singletonList(row);
    return CloseableIterators.withEmptyBaggage(rows.iterator());
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    Map<String, Object> mergeMap = new HashMap<>();
    if (headerParser != null) {
      List<Pair<String, Object>> headerList = headerParser.read();
      for (Pair<String, Object> ele : headerList) {
        mergeMap.put(ele.lhs, ele.rhs);
      }
    }

    // Add kafka record timestamp to the mergelist, we will skip record timestamp if the same key exists already in the header list
    mergeMap.putIfAbsent(timestampColumnName, record.getRecord().timestamp());

    if (keyParser != null) {
      try (CloseableIterator<InputRow> keyIterator = keyParser.read()) {
        // Key currently only takes the first row and ignores the rest.
        if (keyIterator.hasNext()) {
          // Return type for the key parser should be of type MapBasedInputRow
          // Parsers returning other types are not compatible currently.
          MapBasedInputRow keyRow = (MapBasedInputRow) keyIterator.next();
          // Add the key to the mergeList only if the key string is not already present
          mergeMap.putIfAbsent(keyColumnName, keyRow.getEvent().entrySet().stream().findFirst().get().getValue());
        }
      }
      catch (ClassCastException e) {
        throw new IOException("Unsupported input format in keyFormat. KafkaInputformat only supports input format that return MapBasedInputRow rows");
      }
    }

    if (valueParser != null) {
      return buildBlendedRows(valueParser, mergeMap);
    } else {
      return buildRowsWithoutValuePayload(mergeMap);
    }
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}
