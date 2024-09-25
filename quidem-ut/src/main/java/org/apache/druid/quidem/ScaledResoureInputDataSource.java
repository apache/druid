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

package org.apache.druid.quidem;

import org.apache.commons.lang.math.Fraction;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.indexing.common.task.FilteringCloseableInputRowIterator;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.incremental.NoopRowIngestionMeters;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import java.io.File;
import java.io.IOException;
import java.util.function.Predicate;

public class ScaledResoureInputDataSource extends AbstractInputSource
{
  private ResourceInputSource resourceInputSource;
  private Fraction fraction;

  public ScaledResoureInputDataSource(Fraction fraction, ResourceInputSource resourceInputSource)
  {
    this.fraction = fraction;
    this.resourceInputSource = resourceInputSource;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public boolean needsFormat()
  {
    return true;
  }

  @Override
  public InputSourceReader reader(InputRowSchema inputRowSchema, InputFormat inputFormat, File temporaryDirectory)
  {
    InputSourceReader reader = resourceInputSource.reader(inputRowSchema, inputFormat, temporaryDirectory);
    return new FilteredReader(reader, this::filterPredicate);
  }

  public boolean filterPredicate(InputRow inputRow)
  {
    return (Math.abs(inputRow.hashCode() % fraction.getDenominator())) < fraction.getNumerator();
  }

  static class FilteredReader implements InputSourceReader
  {
    private InputSourceReader reader;
    private Predicate<InputRow> filterPredicate;

    public FilteredReader(InputSourceReader reader, Predicate<InputRow> filterPredicate)
    {
      this.reader = reader;
      this.filterPredicate = filterPredicate;
    }

    @Override
    public CloseableIterator<InputRow> read(InputStats inputStats) throws IOException
    {
      return new FilteringCloseableInputRowIterator(
          reader.read(inputStats),
          filterPredicate,
          NoopRowIngestionMeters.INSTANCE,
          new ParseExceptionHandler(NoopRowIngestionMeters.INSTANCE, false, 0, 0)
      );
    }

    @Override
    public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
    {
      return reader.sample();
    }
  }
}
