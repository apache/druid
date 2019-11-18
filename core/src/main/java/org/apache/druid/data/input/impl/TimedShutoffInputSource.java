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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.File;

/**
 * A wrapping InputSource that will close the underlying InputSource at {@link #shutoffTime}.
 * This InputSource is supposed to be used only for InputSourceSampler.
 */
public class TimedShutoffInputSource implements InputSource
{
  private final InputSource delegate;
  private final DateTime shutoffTime;

  public TimedShutoffInputSource(
      @JsonProperty("delegate") InputSource delegate,
      @JsonProperty("shutoffTime") DateTime shutoffTime
  )
  {
    this.delegate = delegate;
    this.shutoffTime = shutoffTime;
  }

  @JsonProperty
  public InputSource getDelegate()
  {
    return delegate;
  }

  @JsonProperty
  public DateTime getShutoffTime()
  {
    return shutoffTime;
  }

  @Override
  public boolean isSplittable()
  {
    return delegate.isSplittable();
  }

  @Override
  public boolean needsFormat()
  {
    return delegate.needsFormat();
  }

  @Override
  public InputSourceReader reader(
      InputRowSchema inputRowSchema,
      @Nullable InputFormat inputFormat,
      File temporaryDirectory
  )
  {
    return new TimedShutoffInputSourceReader(
        delegate.reader(inputRowSchema, inputFormat, temporaryDirectory),
        shutoffTime
    );
  }
}
