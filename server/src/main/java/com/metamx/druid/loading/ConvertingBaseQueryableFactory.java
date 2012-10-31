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

package com.metamx.druid.loading;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.metamx.common.logger.Logger;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.index.v1.IndexIO;

/**
 */
public abstract class ConvertingBaseQueryableFactory implements StorageAdapterFactory
{
  private static final Logger log = new Logger(ConvertingBaseQueryableFactory.class);

  @Override
  public StorageAdapter factorize(File parentDir) throws StorageAdapterLoadingException
  {
    File indexFile = new File(parentDir, "index.drd");
    if (!indexFile.exists()) {
      throw new StorageAdapterLoadingException("indexFile[%s] does not exist.", indexFile);
    }

    try {
      if (! IndexIO.canBeMapped(parentDir)) {
        File canBeMappedDir = new File(parentDir, "forTheMapping");
        if (canBeMappedDir.exists()) {
          FileUtils.deleteDirectory(canBeMappedDir);
        }
        canBeMappedDir.mkdirs();

        IndexIO.storeLatest(IndexIO.readIndex(parentDir), canBeMappedDir);
        if (! IndexIO.canBeMapped(canBeMappedDir)) {
          throw new StorageAdapterLoadingException("WTF!? newly written file[%s] cannot be mapped!?", canBeMappedDir);
        }
        for (File file : canBeMappedDir.listFiles()) {
          if (! file.renameTo(new File(parentDir, file.getName()))) {
            throw new StorageAdapterLoadingException("Couldn't rename[%s] to [%s]", canBeMappedDir, indexFile);
          }
        }
        FileUtils.deleteDirectory(canBeMappedDir);
      }

      return factorizeConverted(parentDir);
    }
    catch (IOException e) {
      log.warn(e, "Got exception, deleting index[%s]", indexFile);
      try {
        FileUtils.deleteDirectory(parentDir);
      }
      catch (IOException e2) {
        log.error(e, "Problem deleting parentDir[%s]", parentDir);
      }
      throw new StorageAdapterLoadingException(e, e.getMessage());
    }
  }

  protected abstract StorageAdapter factorizeConverted(File parentDir) throws IOException;
}
