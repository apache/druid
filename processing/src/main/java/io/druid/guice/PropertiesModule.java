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

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.logger.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

/**
 */
public class PropertiesModule implements Module
{
  private static final Logger log = new Logger(PropertiesModule.class);

  private final String propertiesFile;

  public PropertiesModule(String propertiesFile)
  {
    this.propertiesFile = propertiesFile;
  }

  @Override
  public void configure(Binder binder)
  {
    final Properties fileProps = new Properties();
    Properties systemProps = System.getProperties();

    Properties props = new Properties(fileProps);
    props.putAll(systemProps);

    InputStream stream = ClassLoader.getSystemResourceAsStream(propertiesFile);
    try {
      if (stream == null) {
        File workingDirectoryFile = new File(systemProps.getProperty("druid.properties.file", propertiesFile));
        if (workingDirectoryFile.exists()) {
          stream = new BufferedInputStream(new FileInputStream(workingDirectoryFile));
        }
      }

      if (stream != null) {
        log.info("Loading properties from %s", propertiesFile);
        try(Reader reader = new InputStreamReader(stream, Charsets.UTF_8)) {
          fileProps.load(reader);
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    }
    catch (FileNotFoundException e) {
      log.wtf(e, "This can only happen if the .exists() call lied.  That's f'd up.");
    }
    finally {
      CloseQuietly.close(stream);
    }

    binder.bind(Properties.class).toInstance(props);
  }
}
