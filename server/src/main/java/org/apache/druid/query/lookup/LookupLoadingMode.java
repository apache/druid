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

package org.apache.druid.query.lookup;

/**
 * This enum describes different modes of loading lookups
 * <p>Each task might define if it needs to load lookups or not {@link org.apache.druid.indexing.common.task.Task#loadLookups}
 * <p>Values:
 * <ul>
 * <li>ALL - represents regular mode of loading all lookups </li>
 * <li>NONE - represents mode when no lookups will be loaded </li>
 * <li>ON_DEMAND - currently not supported and will be used in the future </li>
 * </ul>
 */
public enum LookupLoadingMode
{
  NONE, ALL //, ON_DEMAND
}
