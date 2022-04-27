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

package org.apache.druid.java.util.common.parsers;

import java.util.List;

/**
 * This type of ParseException is meant to be used within ingestion to hold parse exception information for
 * rows that were partially parseable but had one or more unparseable columns, such as when passing a non-numeric
 * value to a numeric column. There may be several such exceptions in a given row, so this exception can hold
 * multiple column exception messages.
 */
public class UnparseableColumnsParseException extends ParseException
{
  private final List<String> columnExceptionMessages;

  public UnparseableColumnsParseException(
      String event,
      List<String> details,
      boolean fromPartiallyValidRow,
      String formatText,
      Object... arguments
  )
  {
    super(event, fromPartiallyValidRow, formatText, arguments);
    this.columnExceptionMessages = details;
  }

  public List<String> getColumnExceptionMessages()
  {
    return columnExceptionMessages;
  }
}
