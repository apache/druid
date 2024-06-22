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

package org.apache.druid.segment.index.semantic;

import org.apache.druid.segment.column.DictionaryEncodedColumn;

/**
 * This exposes a 'raw' view into bitmap value indexes of a string {@link DictionaryEncodedColumn}. This allows callers
 * to directly retrieve bitmaps via dictionary ids, as well as access to lower level details of such a column like
 * value lookup and value cardinality.
 *
 * Most filter implementations should likely be using higher level index instead, such as {@link StringValueSetIndexes},
 * {@link LexicographicalRangeIndexes}, {@link NumericRangeIndexes}, or {@link DruidPredicateIndexes}
 */
public interface DictionaryEncodedStringValueIndex extends DictionaryEncodedValueIndex<String>
{

}
