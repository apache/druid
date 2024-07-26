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

package org.apache.druid.data.input.impl;

import org.apache.druid.data.input.BytesCountingInputEntity;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.impl.systemfield.SystemFieldDecoratorFactory;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.RowAdapter;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * InputSourceReader iterating multiple {@link InputEntity}s. This class could be used for
 * most of {@link org.apache.druid.data.input.InputSource}s.
 */
public class InputEntityIteratingReader implements InputSourceReader
{
  private final InputRowSchema inputRowSchema;
  private final InputFormat inputFormat;
  private final CloseableIterator<InputEntity> sourceIterator;
  private final SystemFieldDecoratorFactory systemFieldDecoratorFactory;
  private final File temporaryDirectory;

  public InputEntityIteratingReader(
      InputRowSchema inputRowSchema,
      InputFormat inputFormat,
      CloseableIterator<? extends InputEntity> sourceIterator,
      SystemFieldDecoratorFactory systemFieldDecoratorFactory,
      File temporaryDirectory
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.inputFormat = inputFormat;
    this.sourceIterator = (CloseableIterator<InputEntity>) sourceIterator;
    this.systemFieldDecoratorFactory = systemFieldDecoratorFactory;
    this.temporaryDirectory = temporaryDirectory;
  }

  @Override
  public CloseableIterator<InputRow> read(InputStats inputStats)
  {
    return createIterator(entity -> {
      // InputEntityReader is stateful and so a new one should be created per entity.
      final Function<InputRow, InputRow> systemFieldDecorator = systemFieldDecoratorFactory.decorator(entity);
      try {
        final InputEntity entityToRead = inputStats == null ? entity : new BytesCountingInputEntity(entity, inputStats);
        final InputEntityReader reader = inputFormat.createReader(inputRowSchema, entityToRead, temporaryDirectory);
        return reader.read().map(systemFieldDecorator);
      }
      catch (IOException e) {
        throw new RuntimeException(entity.getUri() != null ?
                                   "Error occurred while trying to read uri: " + entity.getUri() :
                                   "Error occurred while reading input", e);
      }
    });
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample()
  {
    return createIterator(entity -> {
      // InputEntityReader is stateful and so a new one should be created per entity.
      final Function<InputRow, InputRow> systemFieldDecorator = systemFieldDecoratorFactory.decorator(entity);
      try {
        final InputEntityReader reader = inputFormat.createReader(inputRowSchema, entity, temporaryDirectory);
        return reader.sample()
            .map(i -> InputRowListPlusRawValues.ofList(i.getRawValuesList(),
                i.getInputRows() == null
                    ? null
                    : i.getInputRows().stream().map(
                        systemFieldDecorator).collect(Collectors.toList()),
                i.getParseException()
            ));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public RowAdapter<InputRow> rowAdapter()
  {
    return inputFormat.createRowAdapter(inputRowSchema);
  }

  private <R> CloseableIterator<R> createIterator(Function<InputEntity, CloseableIterator<R>> rowPopulator)
  {
    return sourceIterator.flatMap(rowPopulator);
  }
}
