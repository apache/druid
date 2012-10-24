/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.phonebook;

import java.util.Map;

/**
 * A ServiceLookup is an object that, when given a key, will return a metadata map for that key.  This was created
 * for use in doing things like consistent hashing, where the lookupKey represents the partition key and the
 * metadata map has stuff like host and port in it (basically, the information required to be able to contact the server)
 */
public interface ServiceLookup
{
  public Map<String, String> get(String lookupKey);
}
