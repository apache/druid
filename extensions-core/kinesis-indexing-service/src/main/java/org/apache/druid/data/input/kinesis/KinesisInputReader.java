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

import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KinesisInputReader implements InputEntityReader
{

  private final InputRowSchema inputRowSchema;
  private final SettableByteEntity<KinesisRecordEntity> source;
  private final InputEntityReader valueParser;
  private final String partitionKeyColumnName;
  private final String timestampColumnName;

  public KinesisInputReader(
      InputRowSchema inputRowSchema,
      SettableByteEntity<KinesisRecordEntity> source,
      InputEntityReader valueParser,
      String partitionKeyColumnName,
      String timestampColumnName
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.source = source;
    this.valueParser = valueParser;
    this.partitionKeyColumnName = partitionKeyColumnName;
    this.timestampColumnName = timestampColumnName;

  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    final KinesisRecordEntity record = source.getEntity();
    final Map<String, Object> mergedHeaderMap = extractHeaders(record);

    if (record.getRecord().getData() != null) {
      return buildBlendedRows(valueParser, mergedHeaderMap);
    } else {
      return CloseableIterators.withEmptyBaggage(buildInputRowsForMap(mergedHeaderMap).iterator());
    }
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    final KinesisRecordEntity record = source.getEntity();
    InputRowListPlusRawValues headers = extractHeaderSample(record);
    if (record.getRecord().getData() != null) {
      return buildBlendedRowsSample(valueParser, headers.getRawValues());
    } else {
      final List<InputRowListPlusRawValues> rows = Collections.singletonList(headers);
      return CloseableIterators.withEmptyBaggage(rows.iterator());
    }
  }

  private Map<String, Object> extractHeaders(KinesisRecordEntity record)
  {
    final Map<String, Object> mergedHeaderMap = new HashMap<>();
    mergedHeaderMap.put(timestampColumnName, record.getRecord().getApproximateArrivalTimestamp().getTime());
    mergedHeaderMap.put(partitionKeyColumnName, record.getRecord().getPartitionKey());
    return mergedHeaderMap;
  }

  private CloseableIterator<InputRow> buildBlendedRows(
      InputEntityReader valueParser,
      Map<String, Object> headerKeyList
  ) throws IOException
  {
    return valueParser.read().map(
        r -> {
          final HashSet<String> newDimensions = new HashSet<>(r.getDimensions());
          final Map<String, Object> event = buildBlendedEventMap(r::getRaw, newDimensions, headerKeyList);
          newDimensions.addAll(headerKeyList.keySet());
          // Remove the dummy timestamp added in KinesisInputFormat
          newDimensions.remove(KinesisInputFormat.DEFAULT_AUTO_TIMESTAMP_STRING);

          final DateTime timestamp = MapInputRowParser.parseTimestamp(inputRowSchema.getTimestampSpec(), event);
          return new MapBasedInputRow(
              timestamp,
              MapInputRowParser.findDimensions(
                  inputRowSchema.getTimestampSpec(),
                  inputRowSchema.getDimensionsSpec(),
                  newDimensions
              ),
              event
          );
        }
    );
  }

  private InputRowListPlusRawValues extractHeaderSample(KinesisRecordEntity record)
  {
    Map<String, Object> mergedHeaderMap = extractHeaders(record);
    return InputRowListPlusRawValues.of(buildInputRowsForMap(mergedHeaderMap), mergedHeaderMap);
  }

  private CloseableIterator<InputRowListPlusRawValues> buildBlendedRowsSample(
      InputEntityReader valueParser,
      Map<String, Object> headerKeyList
  ) throws IOException
  {
    return valueParser.sample().map(
        rowAndValues -> {
          if (rowAndValues.getParseException() != null) {
            return rowAndValues;
          }
          List<InputRow> newInputRows = Lists.newArrayListWithCapacity(rowAndValues.getInputRows().size());
          List<Map<String, Object>> newRawRows = Lists.newArrayListWithCapacity(rowAndValues.getRawValues().size());

          for (Map<String, Object> raw : rowAndValues.getRawValuesList()) {
            newRawRows.add(buildBlendedEventMap(raw::get, raw.keySet(), headerKeyList));
          }
          for (InputRow r : rowAndValues.getInputRows()) {
            if (r != null) {
              final HashSet<String> newDimensions = new HashSet<>(r.getDimensions());
              final Map<String, Object> event = buildBlendedEventMap(
                  r::getRaw,
                  newDimensions,
                  headerKeyList
              );
              newDimensions.addAll(headerKeyList.keySet());
              // Remove the dummy timestamp added in KinesisInputFormat
              newDimensions.remove(KinesisInputFormat.DEFAULT_AUTO_TIMESTAMP_STRING);
              newInputRows.add(
                  new MapBasedInputRow(
                      inputRowSchema.getTimestampSpec().extractTimestamp(event),
                      MapInputRowParser.findDimensions(
                          inputRowSchema.getTimestampSpec(),
                          inputRowSchema.getDimensionsSpec(),
                          newDimensions
                      ),
                      event
                  )
              );
            }
          }
          return InputRowListPlusRawValues.ofList(newRawRows, newInputRows, null);
        }
    );
  }

  private List<InputRow> buildInputRowsForMap(Map<String, Object> headerKeyList)
  {
    return Collections.singletonList(
        new MapBasedInputRow(
            inputRowSchema.getTimestampSpec().extractTimestamp(headerKeyList),
            MapInputRowParser.findDimensions(
                inputRowSchema.getTimestampSpec(),
                inputRowSchema.getDimensionsSpec(),
                headerKeyList.keySet()
            ),
            headerKeyList
        )
    );
  }

  private Map<String, Object> buildBlendedEventMap(
      Function<String, Object> getRowValue,
      Set<String> rowDimensions,
      Map<String, Object> fallback
  )
  {
    final Set<String> keySet = new HashSet<>(fallback.keySet());
    keySet.addAll(rowDimensions);

    return new AbstractMap<>()
    {
      @Override
      public Object get(Object key)
      {
        final String skey = (String) key;
        final Object val = getRowValue.apply(skey);
        if (val == null) {
          return fallback.get(skey);
        }
        return val;
      }

      @Override
      public Set<String> keySet()
      {
        return keySet;
      }

      @Override
      public Set<Entry<String, Object>> entrySet()
      {
        return keySet().stream()
            .map(
                field -> new Entry<String, Object>()
                {
                  @Override
                  public String getKey()
                  {
                    return field;
                  }

                  @Override
                  public Object getValue()
                  {
                    return get(field);
                  }

                  @Override
                  public Object setValue(final Object value)
                  {
                    throw new UnsupportedOperationException();
                  }
                }
            )
            .collect(Collectors.toCollection(LinkedHashSet::new));
      }
    };
  }
}
