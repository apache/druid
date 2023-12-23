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

package org.apache.druid.msq.util;

/**
 * Values that the query context flag 'arrayIngestMode' can take to specify the behaviour of ingestion of arrays via
 * MSQ's INSERT queries
 */
public enum ArrayIngestMode
{
  /**
   * Disables the ingestion of arrays via MSQ's INSERT queries.
   */
  NONE,

  /**
   * String arrays are ingested as MVDs. This is to preserve the legacy behaviour of Druid and will be removed in the
   * future, since MVDs are not true array types and the behaviour is incorrect.
   * This also disables the ingestion of numeric arrays
   */
  MVD,

  /**
   * Allows numeric and string arrays to be ingested as arrays. This should be the preferred method of ingestion,
   * unless bound by compatibility reasons to use 'mvd'
   */
  ARRAY
}
