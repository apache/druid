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

import org.apache.druid.java.util.common.StringUtils;

/**
 */
public class ParseException extends RuntimeException
{
  private boolean fromPartiallyValidRow = false;

  public ParseException(String formatText, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(formatText, arguments));
  }

  public ParseException(Throwable cause, String formatText, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(formatText, arguments), cause);
  }

  public boolean isFromPartiallyValidRow()
  {
    return fromPartiallyValidRow;
  }

  public void setFromPartiallyValidRow(boolean fromPartiallyValidRow)
  {
    this.fromPartiallyValidRow = fromPartiallyValidRow;
  }
}
