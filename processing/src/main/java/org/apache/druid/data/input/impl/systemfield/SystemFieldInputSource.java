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

package org.apache.druid.data.input.impl.systemfield;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Set;

/**
 * An {@link InputSource} that can generate system fields.
 *
 * Implementations of {@link InputSource#reader(InputRowSchema, InputFormat, File)} tend to create a decorator factory
 * using {@link SystemFieldDecoratorFactory#fromInputSource(SystemFieldInputSource)} on "this" and then pass it to
 * {@link org.apache.druid.data.input.impl.InputEntityIteratingReader}.
 */
public interface SystemFieldInputSource extends InputSource
{
  String SYSTEM_FIELDS_PROPERTY = "systemFields";

  /**
   * System fields that this input source is configured to return.
   *
   * This is not the same set that {@link #getSystemFieldValue(InputEntity, SystemField)} returns nonnull for. For
   * example, if a {@link org.apache.druid.data.input.impl.LocalInputSource} is configured to return
   * {@link SystemField#BUCKET} then it will show up in this list, even though its value is always null. For another
   * example in a different direction, if a {@link org.apache.druid.data.input.impl.LocalInputSource} is *not*
   * configured to return {@link SystemField#URI}, then it will *not* show up in this list, even though its value
   * from {@link #getSystemFieldValue(InputEntity, SystemField)} would be nonnull.
   */
  @JsonProperty(SYSTEM_FIELDS_PROPERTY)
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Set<SystemField> getConfiguredSystemFields();

  /**
   * Compute the value of a system field for a particular {@link InputEntity}.
   */
  @Nullable
  Object getSystemFieldValue(InputEntity entity, SystemField field);
}
