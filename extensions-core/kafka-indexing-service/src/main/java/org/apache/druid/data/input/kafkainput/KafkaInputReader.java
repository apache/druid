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
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

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

public class KafkaInputReader implements InputEntityReader
{
  private final InputRowSchema inputRowSchema;
  private final SettableByteEntity<KafkaRecordEntity> source;
  private final Function<KafkaRecordEntity, KafkaHeaderReader> headerParserSupplier;
  private final Function<KafkaRecordEntity, InputEntityReader> keyParserSupplier;
  private final InputEntityReader valueParser;
  private final String keyColumnName;
  private final String timestampColumnName;
  private final String topicColumnName;

  /**
   *
   * @param inputRowSchema Actual schema from the ingestion spec
   * @param source kafka record containing header, key & value that is wrapped inside SettableByteEntity
   * @param headerParserSupplier Function to get Header parser for parsing the header section, kafkaInputFormat allows users to skip header parsing section and hence an be null
   * @param keyParserSupplier Function to get Key parser for key section, can be null as well. Key parser supplier can also return a null key parser.
   * @param valueParser Value parser is a required section in kafkaInputFormat. It cannot be null.
   * @param keyColumnName Default key column name
   * @param timestampColumnName Default kafka record's timestamp column name
   */
  public KafkaInputReader(
      InputRowSchema inputRowSchema,
      SettableByteEntity<KafkaRecordEntity> source,
      @Nullable Function<KafkaRecordEntity, KafkaHeaderReader> headerParserSupplier,
      @Nullable Function<KafkaRecordEntity, InputEntityReader> keyParserSupplier,
      InputEntityReader valueParser,
      String keyColumnName,
      String timestampColumnName,
      String topicColumnName
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.source = source;
    this.headerParserSupplier = headerParserSupplier;
    this.keyParserSupplier = keyParserSupplier;
    this.valueParser = valueParser;
    this.keyColumnName = keyColumnName;
    this.timestampColumnName = timestampColumnName;
    this.topicColumnName = topicColumnName;
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    final KafkaRecordEntity record = source.getEntity();
    final Map<String, Object> mergedHeaderMap = extractHeaderAndKeys(record);

    // Ignore tombstone records that have null values.
    if (record.getRecord().value() != null) {
      return buildBlendedRows(valueParser, mergedHeaderMap);
    } else {
      return CloseableIterators.withEmptyBaggage(buildInputRowsForMap(mergedHeaderMap).iterator());
    }
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    final KafkaRecordEntity record = source.getEntity();
    InputRowListPlusRawValues keysAndHeader = extractHeaderAndKeysSample(record);
    if (record.getRecord().value() != null) {
      return buildBlendedRowsSample(valueParser, keysAndHeader.getRawValues());
    } else {
      final List<InputRowListPlusRawValues> rows = Collections.singletonList(keysAndHeader);
      return CloseableIterators.withEmptyBaggage(rows.iterator());
    }
  }

  private Map<String, Object> extractHeader(KafkaRecordEntity record)
  {
    final Map<String, Object> mergedHeaderMap = new HashMap<>();
    if (headerParserSupplier != null) {
      KafkaHeaderReader headerParser = headerParserSupplier.apply(record);
      List<Pair<String, Object>> headerList = headerParser.read();
      for (Pair<String, Object> ele : headerList) {
        mergedHeaderMap.put(ele.lhs, ele.rhs);
      }
    }

    // Add kafka record timestamp to the mergelist, we will skip record timestamp if the same key exists already in
    // the header list
    mergedHeaderMap.putIfAbsent(timestampColumnName, record.getRecord().timestamp());

    // Add kafka record topic to the mergelist, only if the key doesn't already exist
    mergedHeaderMap.putIfAbsent(topicColumnName, record.getRecord().topic());

    return mergedHeaderMap;
  }

  private Map<String, Object> extractHeaderAndKeys(KafkaRecordEntity record) throws IOException
  {
    final Map<String, Object> mergedHeaderMap = extractHeader(record);
    final InputEntityReader keyParser = (keyParserSupplier == null) ? null : keyParserSupplier.apply(record);
    if (keyParser != null) {
      try (CloseableIterator<InputRow> keyIterator = keyParser.read()) {
        // Key currently only takes the first row and ignores the rest.
        if (keyIterator.hasNext()) {
          // Return type for the key parser should be of type MapBasedInputRow
          // Parsers returning other types are not compatible currently.
          MapBasedInputRow keyRow = (MapBasedInputRow) keyIterator.next();
          // Add the key to the mergeList only if the key string is not already present
          mergedHeaderMap.putIfAbsent(
              keyColumnName,
              keyRow.getEvent().entrySet().stream().findFirst().get().getValue()
          );
        }
      }
      catch (ClassCastException e) {
        throw new IOException(
            "Unsupported keyFormat. KafkaInputformat only supports input format that return MapBasedInputRow rows"
        );
      }
    }
    return mergedHeaderMap;
  }

  private CloseableIterator<InputRow> buildBlendedRows(
      InputEntityReader valueParser,
      Map<String, Object> headerKeyList
  ) throws IOException
  {
    return valueParser.read().map(
        r -> {
          final MapBasedInputRow valueRow;
          try {
            // Return type for the value parser should be of type MapBasedInputRow
            // Parsers returning other types are not compatible currently.
            valueRow = (MapBasedInputRow) r;
          }
          catch (ClassCastException e) {
            throw new ParseException(
                null,
                "Unsupported input format in valueFormat. KafkaInputFormat only supports input format that return MapBasedInputRow rows"
            );
          }

          final Map<String, Object> event = buildBlendedEventMap(valueRow.getEvent(), headerKeyList);
          final HashSet<String> newDimensions = new HashSet<>(valueRow.getDimensions());
          newDimensions.addAll(headerKeyList.keySet());
          // Remove the dummy timestamp added in KafkaInputFormat
          newDimensions.remove(KafkaInputFormat.DEFAULT_AUTO_TIMESTAMP_STRING);

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

  private InputRowListPlusRawValues extractHeaderAndKeysSample(KafkaRecordEntity record) throws IOException
  {
    Map<String, Object> mergedHeaderMap = extractHeader(record);
    InputEntityReader keyParser = (keyParserSupplier == null) ? null : keyParserSupplier.apply(record);
    if (keyParser != null) {
      try (CloseableIterator<InputRowListPlusRawValues> keyIterator = keyParser.sample()) {
        // Key currently only takes the first row and ignores the rest.
        if (keyIterator.hasNext()) {
          // Return type for the key parser should be of type MapBasedInputRow
          // Parsers returning other types are not compatible currently.
          InputRowListPlusRawValues keyRow = keyIterator.next();
          // Add the key to the mergeList only if the key string is not already present
          mergedHeaderMap.putIfAbsent(
              keyColumnName,
              keyRow.getRawValues().entrySet().stream().findFirst().get().getValue()
          );
          return InputRowListPlusRawValues.of(buildInputRowsForMap(mergedHeaderMap), mergedHeaderMap);
        }
      }
      catch (ClassCastException e) {
        throw new IOException(
            "Unsupported keyFormat. KafkaInputformat only supports input format that return MapBasedInputRow rows"
        );
      }
    }
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
          ParseException parseException = null;

          for (Map<String, Object> raw : rowAndValues.getRawValuesList()) {
            newRawRows.add(buildBlendedEventMap(raw, headerKeyList));
          }
          for (InputRow r : rowAndValues.getInputRows()) {
            MapBasedInputRow valueRow = null;
            try {
              valueRow = (MapBasedInputRow) r;
            }
            catch (ClassCastException e) {
              parseException = new ParseException(
                  null,
                  "Unsupported input format in valueFormat. KafkaInputFormat only supports input format that return MapBasedInputRow rows"
              );
            }
            if (valueRow != null) {
              final Map<String, Object> event = buildBlendedEventMap(valueRow.getEvent(), headerKeyList);
              final HashSet<String> newDimensions = new HashSet<>(valueRow.getDimensions());
              newDimensions.addAll(headerKeyList.keySet());
              // Remove the dummy timestamp added in KafkaInputFormat
              newDimensions.remove(KafkaInputFormat.DEFAULT_AUTO_TIMESTAMP_STRING);
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
          return InputRowListPlusRawValues.ofList(newRawRows, newInputRows, parseException);
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

  /**
   * Builds a map that blends two {@link Map}, presenting the combined keyset of both maps, and preferring to read
   * from the first map and falling back to the second map if the value is not present.
   *
   * This strategy is used rather than just copying the values of the keyset into a new map so that any 'flattening'
   * machinery (such as {@link Map} created by {@link org.apache.druid.java.util.common.parsers.ObjectFlatteners}) is
   * still in place to be lazily evaluated instead of eagerly copying.
   */
  private static Map<String, Object> buildBlendedEventMap(Map<String, Object> map, Map<String, Object> fallback)
  {
    final Set<String> keySet = new HashSet<>(fallback.keySet());
    keySet.addAll(map.keySet());

    return new AbstractMap<String, Object>()
    {
      @Override
      public Object get(Object key)
      {
        return map.getOrDefault((String) key, fallback.get(key));
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
