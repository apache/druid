/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.common.utils.UUIDUtils;
import io.druid.java.util.common.StringUtils;
import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class HdfsClasspathSetupTest
{
  private static MiniDFSCluster miniCluster;
  private static File hdfsTmpDir;
  private static Configuration conf;
  private static String dummyJarString = "This is a test jar file.";
  private File dummyJarFile;
  private Path finalClasspath;
  private Path intermediatePath;
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void setupStatic() throws IOException, ClassNotFoundException
  {
    hdfsTmpDir = File.createTempFile("hdfsClasspathSetupTest", "dir");
    hdfsTmpDir.deleteOnExit();
    if (!hdfsTmpDir.delete()) {
      throw new IOException(String.format("Unable to delete hdfsTmpDir [%s]", hdfsTmpDir.getAbsolutePath()));
    }
    conf = new Configuration(true);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsTmpDir.getAbsolutePath());
    miniCluster = new MiniDFSCluster.Builder(conf).build();
  }

  @Before
  public void setUp() throws IOException
  {
    // intermedatePath and finalClasspath are relative to hdfsTmpDir directory.
    intermediatePath = new Path(String.format("/tmp/classpath/%s", UUIDUtils.generateUuid()));
    finalClasspath = new Path(String.format("/tmp/intermediate/%s", UUIDUtils.generateUuid()));
    dummyJarFile = tempFolder.newFile("dummy-test.jar");
    Files.copy(
        new ByteArrayInputStream(StringUtils.toUtf8(dummyJarString)),
        dummyJarFile.toPath(),
        StandardCopyOption.REPLACE_EXISTING
    );
  }

  @AfterClass
  public static void tearDownStatic() throws IOException
  {
    if (miniCluster != null) {
      miniCluster.shutdown(true);
    }
  }

  @After
  public void tearDown() throws IOException
  {
    dummyJarFile.delete();
    Assert.assertFalse(dummyJarFile.exists());
    miniCluster.getFileSystem().delete(finalClasspath, true);
    Assert.assertFalse(miniCluster.getFileSystem().exists(finalClasspath));
    miniCluster.getFileSystem().delete(intermediatePath, true);
    Assert.assertFalse(miniCluster.getFileSystem().exists(intermediatePath));
  }

  @Test
  public void testAddSnapshotJarToClasspath() throws IOException
  {
    Job job = Job.getInstance(conf, "test-job");
    DistributedFileSystem fs = miniCluster.getFileSystem();
    Path intermediatePath = new Path("/tmp/classpath");
    JobHelper.addSnapshotJarToClassPath(dummyJarFile, intermediatePath, fs, job);
    Path expectedJarPath = new Path(intermediatePath, dummyJarFile.getName());
    // check file gets uploaded to HDFS
    Assert.assertTrue(fs.exists(expectedJarPath));
    // check file gets added to the classpath
    Assert.assertEquals(expectedJarPath.toString(), job.getConfiguration().get(MRJobConfig.CLASSPATH_FILES));
    Assert.assertEquals(dummyJarString, StringUtils.fromUtf8(IOUtils.toByteArray(fs.open(expectedJarPath))));
  }

  @Test
  public void testAddNonSnapshotJarToClasspath() throws IOException
  {
    Job job = Job.getInstance(conf, "test-job");
    DistributedFileSystem fs = miniCluster.getFileSystem();
    JobHelper.addJarToClassPath(dummyJarFile, finalClasspath, intermediatePath, fs, job);
    Path expectedJarPath = new Path(finalClasspath, dummyJarFile.getName());
    // check file gets uploaded to final HDFS path
    Assert.assertTrue(fs.exists(expectedJarPath));
    // check that the intermediate file gets deleted
    Assert.assertFalse(fs.exists(new Path(intermediatePath, dummyJarFile.getName())));
    // check file gets added to the classpath
    Assert.assertEquals(expectedJarPath.toString(), job.getConfiguration().get(MRJobConfig.CLASSPATH_FILES));
    Assert.assertEquals(dummyJarString, StringUtils.fromUtf8(IOUtils.toByteArray(fs.open(expectedJarPath))));
  }

  @Test
  public void testIsSnapshot()
  {
    Assert.assertTrue(JobHelper.isSnapshot(new File("test-SNAPSHOT.jar")));
    Assert.assertTrue(JobHelper.isSnapshot(new File("test-SNAPSHOT-selfcontained.jar")));
    Assert.assertFalse(JobHelper.isSnapshot(new File("test.jar")));
    Assert.assertFalse(JobHelper.isSnapshot(new File("test-selfcontained.jar")));
    Assert.assertFalse(JobHelper.isSnapshot(new File("iAmNotSNAPSHOT.jar")));
    Assert.assertFalse(JobHelper.isSnapshot(new File("iAmNotSNAPSHOT-selfcontained.jar")));

  }

  @Test
  public void testConcurrentUpload() throws IOException, InterruptedException, ExecutionException, TimeoutException
  {
    final int concurrency = 10;
    ListeningExecutorService pool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(concurrency));
    // barrier ensures that all jobs try to add files to classpath at same time.
    final CyclicBarrier barrier = new CyclicBarrier(concurrency);
    final DistributedFileSystem fs = miniCluster.getFileSystem();
    final Path expectedJarPath = new Path(finalClasspath, dummyJarFile.getName());
    List<ListenableFuture<Boolean>> futures = new ArrayList<>();

    for (int i = 0; i < concurrency; i++) {
      futures.add(
          pool.submit(
              new Callable()
              {
                @Override
                public Boolean call() throws Exception
                {
                  int id = barrier.await();
                  Job job = Job.getInstance(conf, "test-job-" + id);
                  Path intermediatePathForJob = new Path(intermediatePath, "job-" + id);
                  JobHelper.addJarToClassPath(dummyJarFile, finalClasspath, intermediatePathForJob, fs, job);
                  // check file gets uploaded to final HDFS path
                  Assert.assertTrue(fs.exists(expectedJarPath));
                  // check that the intermediate file is not present
                  Assert.assertFalse(fs.exists(new Path(intermediatePathForJob, dummyJarFile.getName())));
                  // check file gets added to the classpath
                  Assert.assertEquals(
                      expectedJarPath.toString(),
                      job.getConfiguration().get(MRJobConfig.CLASSPATH_FILES)
                  );
                  return true;
                }
              }
          )
      );
    }

    Futures.allAsList(futures).get(30, TimeUnit.SECONDS);

    pool.shutdownNow();
  }

}
