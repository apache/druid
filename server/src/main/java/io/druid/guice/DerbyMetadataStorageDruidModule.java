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

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import io.druid.metadata.DerbyConnector;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.SQLMetadataConnector;
import org.skife.jdbi.v2.IDBI;

public class DerbyMetadataStorageDruidModule extends SQLMetadataStorageDruidModule
{
  public DerbyMetadataStorageDruidModule()
  {
    super(TYPE);
  }

  public static final String TYPE = "derby";

  @Override
  public void configure(Binder binder)
  {
    createBindingChoices(binder);
    super.configure(binder);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageConnector.class))
            .addBinding(TYPE)
            .to(DerbyConnector.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(SQLMetadataConnector.class))
            .addBinding(TYPE)
            .to(DerbyConnector.class)
            .in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public IDBI getDbi(final DerbyConnector dbConnector)
  {
    return dbConnector.getDBI();
  }
}
