/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.storage.cassandra;

import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ObjectMetadata;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.OutputStream;

/**
 * Cassandra Segment Puller
 *
 * @author boneill42
 */
public class CassandraDataSegmentPuller extends CassandraStorage implements DataSegmentPuller
{
	private static final Logger log = new Logger(CassandraDataSegmentPuller.class);
	private static final int CONCURRENCY = 10;
	private static final int BATCH_SIZE = 10;

  @Inject
	public CassandraDataSegmentPuller(CassandraDataSegmentConfig config)
	{
		super(config);
	}

	@Override
	public void getSegmentFiles(DataSegment segment, File outDir) throws SegmentLoadingException
	{
		String key = (String) segment.getLoadSpec().get("key");
		log.info("Pulling index from C* at path[%s] to outDir[%s]", key, outDir);

		if (!outDir.exists())
		{
			outDir.mkdirs();
		}

		if (!outDir.isDirectory())
		{
			throw new ISE("outDir[%s] must be a directory.", outDir);
		}

		long startTime = System.currentTimeMillis();
		ObjectMetadata meta = null;
		final File outFile = new File(outDir, "index.zip");
		try
		{
			try
			{
				log.info("Writing to [%s]", outFile.getAbsolutePath());
				OutputStream os = Files.newOutputStreamSupplier(outFile).getOutput();
				meta = ChunkedStorage
				    .newReader(indexStorage, key, os)
				    .withBatchSize(BATCH_SIZE)
				    .withConcurrencyLevel(CONCURRENCY)
				    .call();
				os.close();
				CompressionUtils.unzip(outFile, outDir);
			} catch (Exception e)
			{
				FileUtils.deleteDirectory(outDir);
			}
		} catch (Exception e)
		{
			throw new SegmentLoadingException(e, e.getMessage());
		}
		log.info("Pull of file[%s] completed in %,d millis (%s bytes)", key, System.currentTimeMillis() - startTime,
		    meta.getObjectSize());
	}
}
