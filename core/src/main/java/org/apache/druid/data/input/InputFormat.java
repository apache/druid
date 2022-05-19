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

import java.io.File;

/**
 * InputFormat abstracts the file format of input data.
 * It creates a {@link InputEntityReader} to read data and parse it into {@link InputRow}.
 * The created InputEntityReader is used by {@link InputSourceReader}.
 * <p>
 * See {@link NestedInputFormat} for nested input formats such as JSON.
 */
@UnstableApi
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @Type(name = "csv", value = CsvInputFormat.class),
    @Type(name = "json", value = JsonInputFormat.class),
    @Type(name = "regex", value = RegexInputFormat.class),
    @Type(name = "tsv", value = DelimitedInputFormat.class)
})
public interface InputFormat
{
  /**
   * Trait to indicate that a file can be split into multiple {@link InputSplit}s.
   * <p>
   * This method is not being used anywhere for now, but should be considered
   * in {@link SplittableInputSource#createSplits} in the future.
   */
  @JsonIgnore
  boolean isSplittable();

  InputEntityReader createReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      File temporaryDirectory
  );
}
