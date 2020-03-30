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

package org.apache.druid.segment.realtime.firehose;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.NoSuchElementException;

/**
 * Firehose that produces data from its own spec
 */
public class InlineFirehose implements Firehose
{
  private final StringInputRowParser parser;
  private final LineIterator lineIterator;

  InlineFirehose(String data, StringInputRowParser parser) throws IOException
  {
    this.parser = parser;

    Charset charset = Charset.forName(parser.getEncoding());
    InputStream stream = new ByteArrayInputStream(data.getBytes(charset));
    lineIterator = IOUtils.lineIterator(stream, charset);
  }

  @Override
  public boolean hasMore()
  {
    return lineIterator.hasNext();
  }

  @Override
  public InputRow nextRow()
  {
    return parser.parse(nextRaw());
  }

  private String nextRaw()
  {
    if (!hasMore()) {
      throw new NoSuchElementException();
    }

    return lineIterator.next();
  }

  @Override
  public InputRowListPlusRawValues nextRowWithRaw()
  {
    String raw = nextRaw();
    try {
      return InputRowListPlusRawValues.of(parser.parse(raw), parser.parseString(raw));
    }
    catch (ParseException e) {
      return InputRowListPlusRawValues.of(parser.parseString(raw), e);
    }
  }

  @Override
  public void close() throws IOException
  {
    lineIterator.close();
  }
}
