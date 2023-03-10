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

package org.apache.druid.data;

import javax.annotation.Nullable;
import java.util.regex.Pattern;

/**
 * A class which implements SearchableVersionedDataFinder can find a DataDescriptor which describes the most up to
 * date version of data given a base descriptor and a matching pattern. "Version" is completely dependent on the
 * implementation but is commonly equal to the "last modified" timestamp.
 *
 * This is implemented explicitly for org.apache.druid.query.lookup.namespace.CacheGenerator
 * If you have a use case for this interface beyond CacheGenerator please bring it up in the dev list.
 *
 * @param <DataDescriptor> The containing type for the data. A simple example would be URI
 */
public interface SearchableVersionedDataFinder<DataDescriptor>
{
  /**
   * Get's a DataDescriptor to the latest "version" of a data quantum starting with descriptorBase and matching based on pattern
   *
   * @param descriptorBase The base unit of describing the data.
   * @param pattern        A pattern which must match in order for a DataDescriptor to be considered.
   *
   * @return A DataDescriptor which matches pattern, is a child of descriptorBase, and is of the most recent "version" at some point during the method execution.
   */
  DataDescriptor getLatestVersion(DataDescriptor descriptorBase, @Nullable Pattern pattern);
}
