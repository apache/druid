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

package com.metamx.druid.common.s3;

import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 *
 */
public class S3Utils
{
  private static final Logger log = new Logger(S3Utils.class);

  public static void closeStreamsQuietly(S3Object s3Obj)
  {
    if (s3Obj == null) {
      return;
    }

    try {
      s3Obj.closeDataInputStream();
    }
    catch (IOException e) {

    }
  }

  /**
   * Retries S3 operations that fail due to io-related exceptions. Service-level exceptions (access denied, file not
   * found, etc) are not retried.
   */
  public static <T> T retryS3Operation(Callable<T> f) throws ServiceException, InterruptedException
  {
    int nTry = 0;
    final int maxTries = 3;
    while (true) {
      try {
        nTry++;
        return f.call();
      }
      catch (IOException e) {
        if (nTry <= maxTries) {
          awaitNextRetry(e, nTry);
        } else {
          throw Throwables.propagate(e);
        }
      }
      catch (ServiceException e) {
        if (nTry <= maxTries &&
            (e.getCause() instanceof IOException ||
             (e.getErrorCode() != null && e.getErrorCode().equals("RequestTimeout")))) {
          awaitNextRetry(e, nTry);
        } else {
          throw e;
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private static void awaitNextRetry(Exception e, int nTry) throws InterruptedException
  {
    final long baseSleepMillis = 1000;
    final double fuzziness = 0.2;
    final long sleepMillis = Math.max(
        baseSleepMillis,
        (long) (baseSleepMillis * Math.pow(2, nTry) *
                (1 + new Random().nextGaussian() * fuzziness))
    );
    log.info(e, "S3 fail on try %d, retrying in %,dms.", nTry, sleepMillis);
    Thread.sleep(sleepMillis);
  }
}
