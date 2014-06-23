/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.data;

import java.nio.ByteBuffer;
import java.util.Comparator;

public interface ObjectStrategy<T> extends Comparator<T>
{
  public Class<? extends T> getClazz();

  /**
   * Convert values from their underlying byte representation.
   *
   * Implementations of this method must not change the given buffer mark, or limit, but may modify its position.
   * Use buffer.asReadOnlyBuffer() or buffer.duplicate() if mark or limit need to be set.
   *
   * @param buffer buffer to read value from
   * @param numBytes number of bytes used to store the value, starting at buffer.position()
   * @return
   */
  public T fromByteBuffer(ByteBuffer buffer, int numBytes);
  public byte[] toBytes(T val);
}
