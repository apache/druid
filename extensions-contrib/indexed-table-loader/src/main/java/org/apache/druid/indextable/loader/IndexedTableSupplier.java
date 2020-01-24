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

package org.apache.druid.indextable.loader;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.RowBasedIndexedTable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

/**
 * This class loads an indexed table from a {@link InputFormat}
 */
class IndexedTableSupplier implements Supplier<IndexedTable>
{
  private final List<String> keys;
  private final IndexTask.IndexIngestionSpec ingestionSpec;
  private final Supplier<File> tmpDirSupplier;

  IndexedTableSupplier(
      List<String> keys,
      IndexTask.IndexIngestionSpec ingestionSpec,
      Supplier<File> tmpDirSupplier
  )
  {
    this.keys = keys;
    this.ingestionSpec = ingestionSpec;
    this.tmpDirSupplier = tmpDirSupplier;
  }

  /**
   *
   * @return An IndexedTable that is built from reading the ingestionSpec
   */
  @Override
  public IndexedTable get()
  {
    File tmpDir = tmpDirSupplier.get();
    try {
      DataSchema dataSchema = ingestionSpec.getDataSchema();
      InputSourceReader inputSourceReader = getInputSourceReader(dataSchema, tmpDir);

      List<InputRow> table = new ArrayList<>();
      CloseableIterator<InputRow> rowIterator = inputSourceReader.read();
      Map<String, ValueType> rowSignature = new HashMap<>();
      DimensionsSpec dimensionsSpec = dataSchema.getDimensionsSpec();
      dimensionsSpec.getDimensions()
                    .forEach(dimensionSchema -> rowSignature.put(
                        dimensionSchema.getName(),
                        IncrementalIndex.TYPE_MAP.get(dimensionSchema.getValueType())
                    ));

      while (rowIterator.hasNext()) {
        InputRow row = rowIterator.next();
        table.add(row);
      }
      return new RowBasedIndexedTable<>(
          table,
          new RowAdapter<InputRow>()
          {
            @Override
            public ToLongFunction<InputRow> timestampFunction()
            {
              return InputRow::getTimestampFromEpoch;
            }

            @Override
            public Function<InputRow, Object> columnFunction(String columnName)
            {
              return inputRow -> inputRow.getRaw(columnName);
            }
          },
          rowSignature,
          keys
      );
    }
    catch (IOException e) {
      throw new RuntimeException("Could not load IndexedTable - " + ingestionSpec.getDataSchema().getDataSource(), e);
    }
    finally {
      FileUtils.deleteQuietly(tmpDir);
    }
  }

  @VisibleForTesting
  /* TODO: de-duplicate with IndexTask - search for 'inputSource.reader(' */
  InputSourceReader getInputSourceReader(DataSchema dataSchema, File tmpDir)
  {
    final List<String> metricsNames = Arrays.stream(dataSchema.getAggregators())
                                            .map(AggregatorFactory::getName)
                                            .collect(Collectors.toList());
    InputSource inputSource = ingestionSpec.getIOConfig().getNonNullInputSource(dataSchema.getParser());
    InputSourceReader inputSourceReader = dataSchema.getTransformSpec().decorate(
        inputSource.reader(
            new InputRowSchema(
                dataSchema.getTimestampSpec(),
                dataSchema.getDimensionsSpec(),
                metricsNames
            ),
            inputSource.needsFormat() ? getInputFormat(ingestionSpec) : null,
            tmpDir
        )
    );
    return inputSourceReader;
  }
  private static InputFormat getInputFormat(IndexTask.IndexIngestionSpec ingestionSchema)
  {
    final InputRowParser parser = ingestionSchema.getDataSchema().getParser();
    return ingestionSchema.getIOConfig().getNonNullInputFormat(
        parser == null ? null : parser.getParseSpec()
    );
  }
}
