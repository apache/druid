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

package io.druid.server.initialization;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.NoopEmitter;
import io.druid.guice.ManageLifecycle;

/**
 */
public class NoopEmitterModule implements Module
{
  public static final String EMITTER_TYPE = "noop";

  @Override
  public void configure(Binder binder)
  {
  }

  @Provides
  @ManageLifecycle
  @Named(EMITTER_TYPE)
  public Emitter makeEmitter()
  {
    return new NoopEmitter();
  }
}
