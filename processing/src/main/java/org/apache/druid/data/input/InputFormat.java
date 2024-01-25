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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.NestedInputFormat;
import org.apache.druid.data.input.impl.RegexInputFormat;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;

/**
 * InputFormat abstracts the file format of input data.
 * It creates a {@link InputEntityReader} to read data and parse it into {@link InputRow}.
 * The created InputEntityReader is used by {@link InputSourceReader}.
 * <p>
 * See {@link NestedInputFormat} for nested input formats such as JSON.
 */
@UnstableApi
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = InputFormat.TYPE_PROPERTY)
@JsonSubTypes(value = {
    @Type(name = CsvInputFormat.TYPE_KEY, value = CsvInputFormat.class),
    @Type(name = JsonInputFormat.TYPE_KEY, value = JsonInputFormat.class),
    @Type(name = RegexInputFormat.TYPE_KEY, value = RegexInputFormat.class),
    @Type(name = DelimitedInputFormat.TYPE_KEY, value = DelimitedInputFormat.class)
})
public interface InputFormat
{
  String TYPE_PROPERTY = "type";

  /**
   * Trait to indicate that a file can be split into multiple {@link InputSplit}s.
   * <p>
   * This method is not being used anywhere for now, but should be considered
   * in {@link SplittableInputSource#createSplits} in the future.
   */
  @SuppressWarnings("unused")
  @JsonIgnore
  boolean isSplittable();

  InputEntityReader createReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      File temporaryDirectory
  );

  /**
   * Computes the weighted size of a given input object of the underyling input format type, weighted
   * for its cost during ingestion. The weight calculated is dependent on the format type and compression type
   * ({@link CompressionUtils.Format}) used if any. Uncompressed newline delimited json is used as baseline
   * with scale factor 1. This means that when computing the byte weight that an uncompressed newline delimited
   * json input object has towards ingestion, we take the file size as is, 1:1.
   *
   * @param path The path of the input object. Used to tell whether any compression is used.
   * @param size The size of the input object in bytes.
   *
   * @return The weighted size of the input object.
   */
  default long getWeightedSize(String path, long size)
  {
    return size;
  }

  /**
   * Returns an adapter that can read the rows from {@link #createReader(InputRowSchema, InputEntity, File)},
   * given a particular {@link InputRowSchema}. Note that {@link RowAdapters#standardRow()} always works, but the
   * one returned by this method may be more performant.
   */
  @SuppressWarnings("unused") // inputRowSchema is currently unused, but may be used in the future for ColumnsFilter
  default RowAdapter<InputRow> createRowAdapter(InputRowSchema inputRowSchema)
  {
    return RowAdapters.standardRow();
  }
}
