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

package org.apache.druid.data.input;

import org.apache.druid.guice.annotations.ExtensionPoint;

import java.util.List;

/**
 * An InputRow is the interface definition of an event being input into the data ingestion layer.
 *
 * An InputRow is a Row with a self-describing list of the dimensions available.  This list is used to
 * implement "schema-less" data ingestion that allows the system to add new dimensions as they appear.
 *
 */
@ExtensionPoint
public interface InputRow extends Row
{
  /**
   * Returns the dimensions that exist in this row.
   *
   * @return the dimensions that exist in this row.
   */
  List<String> getDimensions();
}
