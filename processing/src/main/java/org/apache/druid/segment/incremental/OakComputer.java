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

import org.apache.druid.data.input.InputRow;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import com.oath.oak.OakWBuffer;

/**
 * For sending as input parameter to the Oak's putifAbsentComputeIfPresent method
 */
public class OakComputer implements Consumer<OakWBuffer>
{
  OffheapAggsManager aggsManager;
  boolean reportParseExceptions;
  InputRow row;
  ThreadLocal<InputRow> rowContainer;

  public OakComputer(
          OffheapAggsManager aggsManager,
          boolean reportParseExceptions,
          InputRow row,
          ThreadLocal<InputRow> rowContainer
  )
  {
    this.aggsManager = aggsManager;
    this.reportParseExceptions = reportParseExceptions;
    this.row = row;
    this.rowContainer = rowContainer;
  }

  @Override
  public void accept(OakWBuffer oakWBuffer)
  {
    ByteBuffer byteBuffer = oakWBuffer.getByteBuffer();
    aggsManager.aggregate(reportParseExceptions, row, rowContainer, byteBuffer);
  }
}
