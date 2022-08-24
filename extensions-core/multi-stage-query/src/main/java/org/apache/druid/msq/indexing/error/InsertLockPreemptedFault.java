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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(InsertLockPreemptedFault.CODE)
public class InsertLockPreemptedFault extends BaseMSQFault
{
  public static final InsertLockPreemptedFault INSTANCE = new InsertLockPreemptedFault();
  static final String CODE = "InsertLockPreempted";

  InsertLockPreemptedFault()
  {
    super(
        CODE,
        "Insert lock preempted while trying to ingest the data."
        + " This can occur if there are higher priority jobs like real-time ingestion running on same time chunks."
    );
  }

  @JsonCreator
  public static InsertLockPreemptedFault instance()
  {
    return INSTANCE;
  }
}
