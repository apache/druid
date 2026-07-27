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

package org.apache.druid.io;

import java.io.File;
import java.io.IOException;

/**
 * Lambda interface for populating a file with downloaded content.
 */
@FunctionalInterface
public interface FilePopulator
{
  /**
   * Populate the given file with content.
   * The file's parent directory is guaranteed to exist.
   *
   * @param file The file to populate
   * @throws IOException if population fails
   */
  void populate(File file) throws IOException;
}
