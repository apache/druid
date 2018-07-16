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

package io.druid.segment.data;

import java.io.Closeable;

/**
 * Resource that provides random access to a packed array of primitive int values. Typically backs up dictionary-encoded
 * string columns (see {@link io.druid.segment.column.DictionaryEncodedColumn}), either directly for single-valued
 * string columns, or indirectly as part of implementation of {@link ColumnarMultiInts}.
 */
public interface ColumnarInts extends IndexedInts, Closeable
{
}
