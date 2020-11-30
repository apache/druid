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

package org.apache.druid.segment.incremental.oak;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.util.ArrayList;
import java.util.List;

/**
 * This class acts as a wrapper to bundle InputRow together with its row container.
 * It also collects the parse exceptions along the way.
 */
class OakInputRowContext
{
  final ThreadLocal<InputRow> rowContainer;
  final InputRow row;
  final List<String> parseExceptionMessages = new ArrayList<>();

  OakInputRowContext(ThreadLocal<InputRow> rowContainer, Row row)
  {
    this.rowContainer = rowContainer;
    this.row = (InputRow) row;
  }

  void setRow()
  {
    rowContainer.set(row);
  }

  void clearRow()
  {
    rowContainer.set(null);
  }

  void addException(ParseException e)
  {
    parseExceptionMessages.add(e.getMessage());
  }
}
