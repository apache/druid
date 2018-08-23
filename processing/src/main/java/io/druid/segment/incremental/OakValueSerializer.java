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

package io.druid.segment.incremental;

import io.druid.data.input.InputRow;
import io.druid.data.input.Row;

import java.nio.ByteBuffer;
import java.util.List;
import io.druid.segment.incremental.IncrementalIndex.DimensionDesc;
import com.oath.oak.Serializer;

public class OakValueSerializer implements Serializer<Row>
{

  private List<DimensionDesc> dimensionDescsList;
  private OffheapAggsManager aggsManager;
  private boolean reportParseExceptions;
  private ThreadLocal<InputRow> rowContainer;

  public OakValueSerializer(
          List<DimensionDesc> dimensionDescsList,
          OffheapAggsManager aggsManager,
          boolean reportParseExceptions,
          ThreadLocal<InputRow> rowContainer
  )
  {
    this.dimensionDescsList = dimensionDescsList;
    this.aggsManager = aggsManager;
    this.reportParseExceptions = reportParseExceptions;
    this.rowContainer = rowContainer;
  }

  @Override
  public void serialize(Row value, ByteBuffer byteBuffer)
  {
    aggsManager.initValue(byteBuffer, reportParseExceptions, (InputRow) value, rowContainer);
  }

  @Override
  public Row deserialize(ByteBuffer serializedValue)
  {
    throw new UnsupportedOperationException(); // cannot be deserialized without the IncrementalIndexRow
  }

  @Override
  public int calculateSize(Row row)
  {
    return aggsManager.aggsTotalSize;
  }

}
