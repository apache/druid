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

package org.apache.druid.segment;

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.segment.serde.Serializer;

import java.io.IOException;

/**
 * GenericColumnSerializer can be implemented in custom aggregator extensions that would like to take full control of column
 * serialization. That implementation would be returned by overriding {@link
 * org.apache.druid.segment.serde.ComplexMetricSerde#getSerializer}
 *
 * TODO rename to "ColumnSerializer" prior to Druid 1.0
 */
@ExtensionPoint
public interface GenericColumnSerializer<T> extends Serializer
{
  void open() throws IOException;

  void serialize(ColumnValueSelector<? extends T> selector) throws IOException;
}
