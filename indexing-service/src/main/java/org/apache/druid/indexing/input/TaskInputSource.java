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

package org.apache.druid.indexing.input;

import org.apache.druid.data.input.InputSource;
import org.apache.druid.indexing.common.TaskToolbox;

/**
 * An InputSource that allows the addition of a task toolbox to use it internally.
 */
public interface TaskInputSource extends InputSource
{
  /**
   * This method would ideally reside in the InputSource interface, but the TaskToolbox isn't accessible in that module.
   * This method provides a way for the DruidInputSource and perhaps others to set a provided TaskToolbox.
   *
   * @param toolbox to be added
   * @return InputSource which utilizes the task toolbox
   */
  InputSource withTaskToolbox(TaskToolbox toolbox);
}
