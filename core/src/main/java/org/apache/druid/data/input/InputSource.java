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

package org.apache.druid.data.input;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.InputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.guice.annotations.ExtensionPoint;

import javax.annotation.Nullable;
import java.io.File;

/**
 * InputSource abstracts the storage system where input data is stored.
 * It creates an {@link InputSourceReader} to read data from the given input source.
 * The most common use case would be:
 *
 * <pre>{@code
 *   InputSourceReader reader = inputSource.reader();
 *   try (CloseableIterator<InputRow> iterator = reader.read()) {
 *     while (iterator.hasNext()) {
 *       InputRow row = iterator.next();
 *       processRow(row);
 *     }
 *   }
 * }</pre>
 */
@ExtensionPoint
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @Type(name = "local", value = LocalInputSource.class),
    @Type(name = "http", value = HttpInputSource.class)
})
public interface InputSource
{
  /**
   * Returns true if this inputSource can be processed in parallel using ParallelIndexSupervisorTask.
   */
  boolean isSplittable();

  /**
   * Returns true if this inputSource supports different {@link InputFormat}s.
   */
  boolean needsFormat();

  /**
   * Create an {@link InputSourceReader}.
   *
   * @param inputRowSchema     for {@link InputRow}
   * @param inputFormat        to parse data. It can be null if {@link #needsFormat()} = true
   * @param temporaryDirectory to store temp data. It will be cleaned up automatically once the task is finished.
   */
  InputSourceReader reader(
      InputRowSchema inputRowSchema,
      @Nullable InputFormat inputFormat,
      @Nullable File temporaryDirectory
  );
}
