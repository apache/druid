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

/**
 * A ReadableOffset is an object that provides an integer offset, ostensibly as an index into an array.
 *
 * See the companion class Offset, for more context on how this could be useful.  A ReadableOffset should be
 * given to classes (e.g. FloatColumnSelector objects) by something which keeps a reference to the base Offset object
 * and increments it.
 */
public interface ReadableOffset
{
    int getOffset();
}

