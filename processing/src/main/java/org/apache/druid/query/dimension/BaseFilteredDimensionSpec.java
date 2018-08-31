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

package org.apache.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.column.ValueType;

/**
 */
public abstract class BaseFilteredDimensionSpec implements DimensionSpec
{
  protected final DimensionSpec delegate;

  public BaseFilteredDimensionSpec(
      @JsonProperty("delegate") DimensionSpec delegate
  )
  {
    this.delegate = Preconditions.checkNotNull(delegate, "delegate must not be null");
  }

  @JsonProperty
  public DimensionSpec getDelegate()
  {
    return delegate;
  }

  @Override
  public String getDimension()
  {
    return delegate.getDimension();
  }

  @Override
  public String getOutputName()
  {
    return delegate.getOutputName();
  }

  @Override
  public ValueType getOutputType()
  {
    return delegate.getOutputType();
  }

  @Override
  public ExtractionFn getExtractionFn()
  {
    return delegate.getExtractionFn();
  }

  @Override
  public boolean mustDecorate()
  {
    return true;
  }

  @Override
  public boolean preservesOrdering()
  {
    return delegate.preservesOrdering();
  }
}
