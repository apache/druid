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

package org.apache.druid.indexing.overlord.sampler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.druid.client.indexing.SamplerResponse;
import org.apache.druid.client.indexing.SamplerResponse.SamplerResponseRow;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimedShutoffInputSourceReader;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.input.InputRowSchemas;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAddResult;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.indexing.DataSchema;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class InputSourceSampler
{
  private static final String SAMPLER_DATA_SOURCE = "sampler";


  private static final DataSchema DEFAULT_DATA_SCHEMA =
      DataSchema.builder()
                .withDataSource(SAMPLER_DATA_SOURCE)
                .withTimestamp(new TimestampSpec(null, null, null))
                .withDimensions(DimensionsSpec.builder().build())
                .build();

  // We want to be able to sort the list of processed results back into the same order that we read them from the input
  // source so that the rows in the data loader are not always changing. To do this, we add a temporary column to the
  // InputRow (in SamplerInputRow) and tag each row with a sortKey. We use an aggregator so that it will not affect
  // rollup, and we use a longMin aggregator so that as rows get rolled up, the earlier rows stay stable and later
  // rows may get rolled into these rows. After getting the results back from the IncrementalIndex, we sort by this
  // column and then exclude it from the response.
  private static final AggregatorFactory INTERNAL_ORDERING_AGGREGATOR = new LongMinAggregatorFactory(
      SamplerInputRow.SAMPLER_ORDERING_COLUMN,
      SamplerInputRow.SAMPLER_ORDERING_COLUMN
  );

  private final ObjectMapper jsonMapper;

  @Inject
  public InputSourceSampler(@Json ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  public SamplerResponse sample(
      final InputSource inputSource,
      // inputFormat can be null only if inputSource.needsFormat() = false or parser is specified.
      @Nullable final InputFormat inputFormat,
      @Nullable final DataSchema dataSchema,
      @Nullable final SamplerConfig samplerConfig
  )
  {
    Preconditions.checkNotNull(inputSource, "inputSource required");
    if (inputSource.needsFormat()) {
      Preconditions.checkNotNull(inputFormat, "inputFormat required");
    }
    final DataSchema nonNullDataSchema = dataSchema == null
                                         ? DEFAULT_DATA_SCHEMA
                                         : dataSchema;
    final SamplerConfig nonNullSamplerConfig = samplerConfig == null
                                               ? SamplerConfig.empty()
                                               : samplerConfig;

    final Closer closer = Closer.create();
    final File tempDir = FileUtils.createTempDir();
    closer.register(() -> FileUtils.deleteDirectory(tempDir));

    try {
      final InputSourceReader reader = buildReader(
          nonNullSamplerConfig,
          nonNullDataSchema,
          inputSource,
          inputFormat,
          tempDir
      );
      try (final CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample();
           final IncrementalIndex index = buildIncrementalIndex(nonNullSamplerConfig, nonNullDataSchema);
           final Closer closer1 = closer) {
        List<SamplerResponseRow> responseRows = new ArrayList<>(nonNullSamplerConfig.getNumRows());
        int numRowsIndexed = 0;

        while (
            responseRows.size() < nonNullSamplerConfig.getNumRows() &&
            index.getBytesInMemory().get() < nonNullSamplerConfig.getMaxBytesInMemory() &&
            iterator.hasNext()
        ) {
          final InputRowListPlusRawValues inputRowListPlusRawValues = iterator.next();

          final List<Map<String, Object>> rawColumnsList = inputRowListPlusRawValues.getRawValuesList();

          final ParseException parseException = inputRowListPlusRawValues.getParseException();
          if (parseException != null) {
            if (rawColumnsList != null) {
              // add all rows to response
              responseRows.addAll(rawColumnsList.stream()
                                                .map(rawColumns -> new SamplerResponseRow(
                                                    rawColumns,
                                                    null,
                                                    true,
                                                    parseException.getMessage()
                                                ))
                                                .collect(Collectors.toList()));
            } else {
              // no data parsed, add one response row
              responseRows.add(new SamplerResponseRow(null, null, true, parseException.getMessage()));
            }
            continue;
          }

          List<InputRow> inputRows = inputRowListPlusRawValues.getInputRows();
          if (inputRows == null) {
            continue;
          }

          for (int i = 0; i < inputRows.size(); i++) {
            // InputRowListPlusRawValues guarantees the size of rawColumnsList and inputRows are the same
            Map<String, Object> rawColumns = rawColumnsList == null ? null : rawColumnsList.get(i);
            InputRow row = inputRows.get(i);

            //keep the index of the row to be added to responseRows for further use
            final int rowIndex = responseRows.size();
            IncrementalIndexAddResult addResult = index.add(new SamplerInputRow(row, rowIndex));
            if (addResult.hasParseException()) {
              responseRows.add(new SamplerResponseRow(
                  rawColumns,
                  null,
                  true,
                  addResult.getParseException().getMessage()
              ));
            } else {
              // store the raw value; will be merged with the data from the IncrementalIndex later
              responseRows.add(new SamplerResponseRow(rawColumns, null, null, null));
              numRowsIndexed++;
            }
          }
        }

        final List<String> columnNames = index.getColumnNames();
        columnNames.remove(SamplerInputRow.SAMPLER_ORDERING_COLUMN);


        for (Row row : index) {
          Map<String, Object> parsed = new LinkedHashMap<>();

          parsed.put(ColumnHolder.TIME_COLUMN_NAME, row.getTimestampFromEpoch());
          columnNames.stream()
                     .filter(k -> !ColumnHolder.TIME_COLUMN_NAME.equals(k))
                     .forEach(k -> parsed.put(k, row.getRaw(k)));

          Number sortKey = row.getMetric(SamplerInputRow.SAMPLER_ORDERING_COLUMN);
          if (sortKey != null) {
            SamplerResponseRow theRow = responseRows.get(sortKey.intValue()).withParsed(parsed);
            responseRows.set(sortKey.intValue(), theRow);

          }
        }

        // make sure size of responseRows meets the input
        if (responseRows.size() > nonNullSamplerConfig.getNumRows()) {
          responseRows = responseRows.subList(0, nonNullSamplerConfig.getNumRows());
        }

        if (nonNullSamplerConfig.getMaxClientResponseBytes() > 0) {
          long estimatedResponseSize = 0;
          boolean limited = false;
          int rowCounter = 0;
          int parsedCounter = 0;
          for (SamplerResponseRow row : responseRows) {
            rowCounter++;
            if (row.getInput() != null) {
              parsedCounter++;
            }
            estimatedResponseSize += jsonMapper.writeValueAsBytes(row).length;
            if (estimatedResponseSize > nonNullSamplerConfig.getMaxClientResponseBytes()) {
              limited = true;
              break;
            }
          }
          if (limited) {
            responseRows = responseRows.subList(0, rowCounter);
            numRowsIndexed = parsedCounter;
          }
        }

        int numRowsRead = responseRows.size();

        List<DimensionSchema> logicalDimensionSchemas = new ArrayList<>();
        List<DimensionSchema> physicalDimensionSchemas = new ArrayList<>();

        RowSignature.Builder signatureBuilder = RowSignature.builder();
        for (final String dimensionName : index.getDimensionNames(true)) {
          if (ColumnHolder.TIME_COLUMN_NAME.equals(dimensionName)) {
            signatureBuilder.addTimeColumn();
          } else if (!SamplerInputRow.SAMPLER_ORDERING_COLUMN.equals(dimensionName)) {
            final IncrementalIndex.DimensionDesc dimensionDesc = index.getDimension(dimensionName);
            signatureBuilder.add(dimensionDesc.getName(), ColumnType.fromCapabilities(dimensionDesc.getCapabilities()));
            // use explicitly specified dimension schema if it exists
            if (dataSchema != null &&
                dataSchema.getDimensionsSpec() != null &&
                dataSchema.getDimensionsSpec().getSchema(dimensionDesc.getName()) != null) {
              logicalDimensionSchemas.add(dataSchema.getDimensionsSpec().getSchema(dimensionDesc.getName()));
            } else {
              logicalDimensionSchemas.add(
                  DimensionSchema.getDefaultSchemaForBuiltInType(
                      dimensionDesc.getName(),
                      dimensionDesc.getCapabilities()
                  )
              );
            }
            physicalDimensionSchemas.add(
                dimensionDesc.getIndexer().getFormat().getColumnSchema(dimensionDesc.getName())
            );
          }
        }
        for (AggregatorFactory aggregatorFactory : index.getMetricAggs()) {
          if (!SamplerInputRow.SAMPLER_ORDERING_COLUMN.equals(aggregatorFactory.getName())) {
            signatureBuilder.add(
                aggregatorFactory.getName(),
                ColumnType.fromCapabilities(index.getColumnCapabilities(aggregatorFactory.getName()))
            );
          }
        }

        return new SamplerResponse(
            numRowsRead,
            numRowsIndexed,
            logicalDimensionSchemas,
            physicalDimensionSchemas,
            signatureBuilder.build(),
            responseRows.stream()
                        .filter(Objects::nonNull)
                        .filter(x -> x.getParsed() != null || x.isUnparseable() != null)
                        .collect(Collectors.toList())
        );
      }
    }
    catch (Exception e) {
      throw new SamplerException(e, "Failed to sample data: %s", e.getMessage());
    }
  }

  private InputSourceReader buildReader(
      SamplerConfig samplerConfig,
      DataSchema dataSchema,
      InputSource inputSource,
      @Nullable InputFormat inputFormat,
      File tempDir
  )
  {
    final InputRowSchema inputRowSchema = InputRowSchemas.fromDataSchema(dataSchema);

    InputSourceReader reader = inputSource.reader(inputRowSchema, inputFormat, tempDir);

    if (samplerConfig.getTimeoutMs() > 0) {
      reader = new TimedShutoffInputSourceReader(reader, DateTimes.nowUtc().plusMillis(samplerConfig.getTimeoutMs()));
    }

    return dataSchema.getTransformSpec().decorate(reader);
  }

  private IncrementalIndex buildIncrementalIndex(SamplerConfig samplerConfig, DataSchema dataSchema)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withTimestampSpec(dataSchema.getTimestampSpec())
        .withQueryGranularity(dataSchema.getGranularitySpec().getQueryGranularity())
        .withDimensionsSpec(dataSchema.getDimensionsSpec())
        .withMetrics(ArrayUtils.addAll(dataSchema.getAggregators(), INTERNAL_ORDERING_AGGREGATOR))
        .withRollup(dataSchema.getGranularitySpec().isRollup())
        .build();

    return new OnheapIncrementalIndex.Builder().setIndexSchema(schema)
                                         .setMaxRowCount(samplerConfig.getNumRows())
                                         .build();
  }
}
