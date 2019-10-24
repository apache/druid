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
import org.apache.druid.data.input.impl.prefetch.PrefetchableTextFilesFirehoseFactory;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.parsers.ParseException;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

/**
 * FirehoseFactory creates a {@link Firehose} which is an interface holding onto the stream of incoming data.
 * It currently provides two methods for creating a {@link Firehose} and their default implementations call each other
 * for the backward compatibility.  Implementations of this interface must implement one of these methods.
 */
@ExtensionPoint
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface FirehoseFactory<T extends InputRowParser>
{
  /**
   * Initialization method that connects up the fire hose.  If this method returns successfully it should be safe to
   * call hasMore() on the returned Firehose (which might subsequently block).
   * <p/>
   * If this method returns null, then any attempt to call hasMore(), nextRow() and close() on the return
   * value will throw a surprising NPE.   Throwing IOException on connection failure or runtime exception on
   * invalid configuration is preferred over returning null.
   *
   * @param parser             an input row parser
   */
  @Deprecated
  default Firehose connect(T parser) throws IOException, ParseException
  {
    return connect(parser, null);
  }

  /**
   * Initialization method that connects up the fire hose.  If this method returns successfully it should be safe to
   * call hasMore() on the returned Firehose (which might subsequently block).
   * <p/>
   * If this method returns null, then any attempt to call hasMore(), nextRow() and close() on the return
   * value will throw a surprising NPE.   Throwing IOException on connection failure or runtime exception on
   * invalid configuration is preferred over returning null.
   * <p/>
   * Some fire hoses like {@link PrefetchableTextFilesFirehoseFactory} may use a temporary
   * directory to cache data in it.
   *
   * @param parser             an input row parser
   * @param temporaryDirectory a directory where temporary files are stored
   */
  default Firehose connect(T parser, @Nullable File temporaryDirectory) throws IOException, ParseException
  {
    return connect(parser);
  }

  /**
   * Initialization method that connects up the firehose. This method is intended for use by the sampler, and allows
   * implementors to return a more efficient firehose, knowing that only a small number of rows will be read.
   *
   * @param parser             an input row parser
   * @param temporaryDirectory a directory where temporary files are stored
   */
  default Firehose connectForSampler(T parser, @Nullable File temporaryDirectory) throws IOException, ParseException
  {
    return connect(parser, temporaryDirectory);
  }

  default boolean isSplittable()
  {
    return false;
  }
}
