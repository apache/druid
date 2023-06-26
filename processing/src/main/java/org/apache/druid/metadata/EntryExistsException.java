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

package org.apache.druid.metadata;

import org.apache.druid.common.exception.DruidException;
import org.apache.druid.java.util.common.StringUtils;

/**
 * A non-transient Druid metadata exception thrown when trying to insert a
 * duplicate entry in the metadata.
 */
public class EntryExistsException extends DruidException
{

  private static final int HTTP_BAD_REQUEST = 400;

  public EntryExistsException(String entryType, String entryId)
  {
    this(entryType, entryId, null);
  }

  public EntryExistsException(String entryType, String entryId, Throwable t)
  {
    super(StringUtils.format("%s [%s] already exists.", entryType, entryId), HTTP_BAD_REQUEST, t, false);
  }

}
