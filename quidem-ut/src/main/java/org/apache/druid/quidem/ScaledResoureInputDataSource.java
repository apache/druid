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

import org.apache.commons.lang3.math.Fraction;
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
  private final ResourceInputSource resourceInputSource;
  private final Fraction fraction;
  private final int maxRows;

  public ScaledResoureInputDataSource(Fraction fraction, int maxRows, ResourceInputSource resourceInputSource)
  {
    this.fraction = fraction;
    this.maxRows = maxRows;
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
    return new FilteredReader(reader, this::filterPredicate, maxRows);
  }

  public boolean filterPredicate(InputRow inputRow)
  {
    return (Math.abs(inputRow.hashCode() % fraction.getDenominator())) < fraction.getNumerator();
  }

  static class FilteredReader implements InputSourceReader
  {
    private InputSourceReader reader;
    private Predicate<InputRow> filterPredicate;
    private int maxRows;

    public FilteredReader(InputSourceReader reader, Predicate<InputRow> filterPredicate, int maxRows)
    {
      this.reader = reader;
      this.filterPredicate = filterPredicate;
      this.maxRows = maxRows;
    }

    @Override
    public CloseableIterator<InputRow> read(InputStats inputStats) throws IOException
    {
      NoopRowIngestionMeters rowIngestionMeters = new NoopRowIngestionMeters();
      FilteringCloseableInputRowIterator filteredIterator = new FilteringCloseableInputRowIterator(
          reader.read(inputStats),
          filterPredicate,
          rowIngestionMeters,
          new ParseExceptionHandler(rowIngestionMeters, false, 0, 0)
      );
      return new LimitedCloseableIterator<>(filteredIterator, maxRows);
    }

    @Override
    public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
    {
      return reader.sample();
    }
  }

  static class LimitedCloseableIterator<InputRow> implements CloseableIterator<InputRow>
  {
    private final CloseableIterator<InputRow> delegate;
    private final int maxRows;
    private int count = 0;

    public LimitedCloseableIterator(CloseableIterator<InputRow> delegate, int maxRows)
    {
      this.delegate = delegate;
      this.maxRows = maxRows;
    }

    @Override
    public boolean hasNext()
    {
      return count < maxRows && delegate.hasNext();
    }

    @Override
    public InputRow next()
    {
      if (count >= maxRows) {
        throw new IllegalStateException("Exceeded maxRows");
      }
      count++;
      return delegate.next();
    }

    @Override
    public void close() throws IOException
    {
      delegate.close();
    }
  }
}
