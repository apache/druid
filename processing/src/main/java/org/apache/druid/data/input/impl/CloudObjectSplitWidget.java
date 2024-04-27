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

package org.apache.druid.data.input.impl;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

/**
 * Helper used by {@link CloudObjectInputSource} to implement {@link SplittableInputSource#createSplits}.
 */
public interface CloudObjectSplitWidget
{
  long UNKNOWN_SIZE = -1;

  /**
   * Iterator of descriptors that match a list of prefixes. Used if {@link CloudObjectInputSource#getPrefixes()}
   * is set.
   *
   * Sizes in {@link LocationWithSize} are set if the information is naturally available as part of listing
   * the cloud object prefix. Otherwise, they are set to {@link #UNKNOWN_SIZE} and filled in later.
   */
  Iterator<LocationWithSize> getDescriptorIteratorForPrefixes(List<URI> prefixes);

  /**
   * Size of an object. May use a cached size, if available, or may issue a network call.
   */
  long getObjectSize(CloudObjectLocation descriptor) throws IOException;

  /**
   * Returned by {@link #getDescriptorIteratorForPrefixes(List)}. A pair of {@link CloudObjectLocation} and its size,
   * which may be {@link #UNKNOWN_SIZE} if not known.
   */
  class LocationWithSize
  {
    private final CloudObjectLocation location;
    private final long size;

    public LocationWithSize(CloudObjectLocation location, long size)
    {
      this.location = location;
      this.size = size;
    }

    public LocationWithSize(String bucket, String key, long size)
    {
      this(new CloudObjectLocation(bucket, key), size);
    }

    public CloudObjectLocation getLocation()
    {
      return location;
    }

    public long getSize()
    {
      return size;
    }
  }
}
