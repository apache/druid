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

package org.apache.druid.segment.incremental;

import com.oath.oak.OakSerializer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;

import java.nio.ByteBuffer;


public class OakValueSerializer implements OakSerializer<Row>
{

  private final OakIncrementalIndex.AggsManager aggsManager;
  private final ThreadLocal<InputRow> rowContainer;

  public OakValueSerializer(OakIncrementalIndex.AggsManager aggsManager,
                            ThreadLocal<InputRow> rowContainer)
  {
    this.aggsManager = aggsManager;
    this.rowContainer = rowContainer;
  }

  @Override
  public void serialize(Row row, ByteBuffer byteBuffer)
  {
    //TODO YONIGO - why cast?
    aggsManager.initValue(byteBuffer, (InputRow) row, rowContainer);
  }

  @Override
  public Row deserialize(ByteBuffer byteBuffer)
  {
    // cannot be deserialized without the IncrementalIndexRow
    throw new UnsupportedOperationException();

  }

  @Override
  public int calculateSize(Row row)
  {
    return aggsManager.aggsTotalSize();
  }
}
