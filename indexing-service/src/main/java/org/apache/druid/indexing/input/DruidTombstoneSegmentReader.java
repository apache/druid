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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.IntermediateRowParsingReader;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * This class will return an empty iterator since a tombstone has no data rows...
 */
public class DruidTombstoneSegmentReader extends IntermediateRowParsingReader<Map<String, Object>>
{
  private DruidSegmentInputEntity source;

  public DruidTombstoneSegmentReader(
      InputEntity source
  )
  {
    this.source = (DruidSegmentInputEntity) source;
    if (!this.source.isFromTombstone()) {
      throw new IAE("DruidSegmentInputEntity must be created from a tombstone but is not.");
    }
  }

  @Override
  protected CloseableIterator<Map<String, Object>> intermediateRowIterator()
  {
    return new CloseableIterator<Map<String, Object>>()
    {
      @Override
      public void close()
      {

      }

      @Override
      public boolean hasNext()
      {
        return false;
      }

      @Override
      public Map<String, Object> next()
      {
        throw new NoSuchElementException();
      }
    };
  }

  @VisibleForTesting
  @Override
  protected List<InputRow> parseInputRows(Map<String, Object> intermediateRow)
  {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  protected List<Map<String, Object>> toMap(Map<String, Object> intermediateRow)
  {
    throw new UnsupportedOperationException(getClass().getName());
  }

}
