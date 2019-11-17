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

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusJson;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * InputSourceReader iterating multiple {@link InputEntity}s. This class could be used for
 * most of {@link org.apache.druid.data.input.InputSource}s.
 */
public class InputEntityIteratingReader implements InputSourceReader
{
  private final InputRowSchema inputRowSchema;
  private final InputFormat inputFormat;
  private final Iterator<InputEntity> sourceIterator;
  private final File temporaryDirectory;

  InputEntityIteratingReader(
      InputRowSchema inputRowSchema,
      InputFormat inputFormat,
      Stream<InputEntity> sourceStream,
      File temporaryDirectory
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.inputFormat = inputFormat;
    this.sourceIterator = sourceStream.iterator();
    this.temporaryDirectory = temporaryDirectory;
  }

  @Override
  public CloseableIterator<InputRow> read()
  {
    return createIterator(entity -> {
      // InputEntityReader is stateful and so a new one should be created per entity.
      final InputEntityReader reader = inputFormat.createReader(inputRowSchema, entity, temporaryDirectory);
      try {
        return reader.read();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public CloseableIterator<InputRowListPlusJson> sample()
  {
    return createIterator(entity -> {
      // InputEntityReader is stateful and so a new one should be created per entity.
      final InputEntityReader reader = inputFormat.createReader(inputRowSchema, entity, temporaryDirectory);
      try {
        return reader.sample();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private <R> CloseableIterator<R> createIterator(Function<InputEntity, CloseableIterator<R>> rowPopulator)
  {
    return CloseableIterators.withEmptyBaggage(sourceIterator).flatMap(rowPopulator);
  }
}
