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

//CHECKSTYLE.OFF: PackageName - Must be in RoaringBitmap to reach ImmutableRoaringBitmap.highLowContainer

package org.roaringbitmap.buffer;

/**
 * Exposes {@link ImmutableRoaringBitmap#highLowContainer}, which is package-private. Every method on
 * {@link PointableRoaringArray} is public; only the field holding it is not.
 */
public final class DruidRoaringBufferAccess
{
  private DruidRoaringBufferAccess()
  {
  }

  public static PointableRoaringArray highLowContainer(final ImmutableRoaringBitmap bitmap)
  {
    return bitmap.highLowContainer;
  }
}
