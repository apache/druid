package com.metamx.druid.loading;

import com.metamx.common.logger.Logger;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.index.v1.IndexIO;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

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
