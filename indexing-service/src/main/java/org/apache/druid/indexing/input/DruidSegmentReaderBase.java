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

package org.apache.druid.indexing.input;

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

public abstract class DruidSegmentReaderBase extends IntermediateRowParsingReader<Map<String, Object>>
{
  private DruidSegmentInputEntity source;

  public DruidSegmentReaderBase(InputEntity source)
  {
    Preconditions.checkArgument(source instanceof DruidSegmentInputEntity);
    this.source = (DruidSegmentInputEntity) source;
  }

  @Override
  protected InputEntity source()
  {
    return source;
  }

  protected DruidSegmentInputEntity druidSegmentInputEntitySource()
  {
    return source;
  }

  @Override
  protected List<InputRow> parseInputRows(Map<String, Object> intermediateRow) throws IOException, ParseException
  {
    throw new UnsupportedEncodingException(this.getClass().getName());
  }

  @Override
  protected List<Map<String, Object>> toMap(Map<String, Object> intermediateRow) throws IOException
  {
    throw new UnsupportedEncodingException(this.getClass().getName());
  }

  @Override
  protected CloseableIterator<Map<String, Object>> intermediateRowIterator() throws IOException
  {
    throw new UnsupportedEncodingException(this.getClass().getName());
  }

}
