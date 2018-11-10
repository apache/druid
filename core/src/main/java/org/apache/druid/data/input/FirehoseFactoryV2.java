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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.IOException;

/**
 * Initialization method that connects up the FirehoseV2.  If this method returns successfully it should be safe to
 * call start() on the returned FirehoseV2 (which might subsequently block).
 *
 * In contrast to V1 version, FirehoseFactoryV2 is able to pass an additional json-serialized object to FirehoseV2,
 * which contains last commit metadata
 *
 * <p/>
 * If this method returns null, then any attempt to call start(), advance(), currRow(), makeCommitter() and close() on the return
 * value will throw a surprising NPE.   Throwing IOException on connection failure or runtime exception on
 * invalid configuration is preferred over returning null.
 */
@ExtensionPoint
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface FirehoseFactoryV2<T extends InputRowParser>
{
  /**
   * This method is declared to throw {@link IOException}, although it's not thrown in the implementations in Druid
   * code, for compatibility with third-party extensions.
   */
  @SuppressWarnings("RedundantThrows")
  FirehoseV2 connect(T parser, Object lastCommit) throws IOException, ParseException;
}
