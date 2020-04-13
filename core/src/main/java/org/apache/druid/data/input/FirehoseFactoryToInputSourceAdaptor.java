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

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.impl.FirehoseToInputSourceReaderAdaptor;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.SplittableInputSource;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;

public class FirehoseFactoryToInputSourceAdaptor extends AbstractInputSource implements SplittableInputSource
{
  private final FiniteFirehoseFactory firehoseFactory;
  private final InputRowParser inputRowParser;

  public FirehoseFactoryToInputSourceAdaptor(FiniteFirehoseFactory firehoseFactory, InputRowParser inputRowParser)
  {
    this.firehoseFactory = firehoseFactory;
    this.inputRowParser = Preconditions.checkNotNull(inputRowParser, "inputRowParser");
  }

  public FiniteFirehoseFactory getFirehoseFactory()
  {
    return firehoseFactory;
  }

  public InputRowParser getInputRowParser()
  {
    return inputRowParser;
  }

  @Override
  public boolean isSplittable()
  {
    return firehoseFactory.isSplittable();
  }

  @Override
  public Stream<InputSplit> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
      throws IOException
  {
    if (firehoseFactory.isSplittable()) {
      return firehoseFactory.getSplits(splitHintSpec);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec) throws IOException
  {
    if (firehoseFactory.isSplittable()) {
      return firehoseFactory.getNumSplits(splitHintSpec);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public SplittableInputSource withSplit(InputSplit split)
  {
    if (firehoseFactory.isSplittable()) {
      return new FirehoseFactoryToInputSourceAdaptor(
          firehoseFactory.withSplit(split),
          inputRowParser
      );
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public boolean needsFormat()
  {
    return false;
  }

  @Override
  protected InputSourceReader fixedFormatReader(InputRowSchema inputRowSchema, @Nullable File temporaryDirectory)
  {
    return new FirehoseToInputSourceReaderAdaptor(firehoseFactory, inputRowParser, temporaryDirectory);
  }
}
