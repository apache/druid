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

package com.metamx.druid.index.v1.serde;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This is a "factory" interface for registering handlers in the system.  It exists because I'm unaware of
 * another way to register the complex serdes in the MR jobs that run on Hadoop.  As such, instances of this interface
 * must be instantiatable via a no argument default constructor (the MR jobs on Hadoop use reflection to instantiate
 * instances).
 *
 * The name is not a typo, I felt that it needed an extra "er" to make the pronunciation that much more difficult.
 */
public interface Registererer
{
  public void register();

  public void registerSubType(ObjectMapper jsonMapper);
}
