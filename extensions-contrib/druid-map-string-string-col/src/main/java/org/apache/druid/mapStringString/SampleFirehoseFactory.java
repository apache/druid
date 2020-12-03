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

package org.apache.druid.mapStringString;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

public class SampleFirehoseFactory<T extends InputRowParser> implements FiniteFirehoseFactory<T, Object>
{
  @Override
  public Firehose connect(T parser, @Nullable File temporaryDirectory) throws IOException, ParseException
  {
    return new SampleFirehose(parser);
  }

  @Override
  public Stream<InputSplit<Object>> getSplits(
      @Nullable SplitHintSpec splitHintSpec
  ) throws IOException
  {
    return Stream.of(new InputSplit(null));
  }

  @Override
  public int getNumSplits(@Nullable SplitHintSpec splitHintSpec) throws IOException
  {
    return 1;
  }

  @Override
  public FiniteFirehoseFactory withSplit(InputSplit split)
  {
    return this;
  }

  private static class SampleFirehose implements Firehose
  {
    private static final Logger LOGGER = new Logger(SampleFirehose.class);
    private final LineIterator lineIterator;
    private final InputRowParser parser;

    public SampleFirehose(InputRowParser parser)
    {
      try {
        lineIterator = IOUtils.lineIterator(
            new FileInputStream(
                "/Users/hgupta/work/druid/examples/quickstart/tutorial/wikiticker-2015-09-12-sampled.json"),
            StandardCharsets.UTF_8
        );
      }
      catch (IOException ex) {
        throw new RE(ex, "Data file not found");
      }
      this.parser = parser;
    }

    @Override
    public boolean hasMore()
    {
      return lineIterator.hasNext();
    }

    @Nullable
    @Override
    public InputRow nextRow()
    {
      String line = lineIterator.nextLine();
      byte[] data = line.getBytes(StandardCharsets.UTF_8);
      List<InputRow> rows = parser.parseBatch(ByteBuffer.wrap(data));
      if (rows.get(0) == null) {
        LOGGER.warn("Parser[%s] gave NULL row", parser.getClass().getName());
      }
      InputRow row = rows.get(0);
      LOGGER.warn("Row[%s] dimensions[%s] and tags are [%s]", row.getClass().getName(), row.getDimensions(), row.getRaw("tags"));
      return row;
    }

    @Override
    public void close() throws IOException
    {
      lineIterator.close();
    }
  }
}
