package org.apache.druid.testing.utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;

public class s3TestUtil
{
  public static final Logger LOG = new Logger(TestQueryHelper.class);

  private final IntegrationTestingConfig config;

  private static AmazonS3 s3Client;

  public s3TestUtil(IntegrationTestingConfig config)
  {
    LOG.info("Inside s3TestUtil contructor");
    this.config = config;
    verifyEnvironment();
    s3Client = s3Client();
    LOG.info("Finished s3TestUtil contructor");
  }

  /**
   * Verify required environment variables are set for
   */
  public static void verifyEnvironment()
  {
    String[] envVars = {"DRUID_CLOUD_BUCKET", "DRUID_CLOUD_PATH", "AWS_ACCESS_KEY_ID",
                        "AWS_SECRET_ACCESS_KEY", "AWS_REGION"};
    for (String val : envVars){
      String envValue = System.getenv(val);
      if (envValue == null){
        LOG.error("%s was not set", val);
        LOG.error("All of %s MUST be set in the environment", Arrays.toString(envVars));
      }
    }
  }

  /**
   * Creates a s3Client which will be used for uploading and deleting files from s3
   */
  private AmazonS3 s3Client()
  {
    AWSCredentials credentials = new BasicAWSCredentials(config.getProperty("s3Accesskey"),
                                                         config.getProperty("s3SecretKey"));
    return AmazonS3ClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .withRegion(config.getProperty("s3Region"))
        .build();
  }

  /**
   * Uploads a list of files to s3 at the location set in the IT config
   *
   * @param  localFiles List of local path of files
   */
  public void uploadDataFilesToS3(List<String> localFiles)
  {
    List<String> s3ObjectPaths = new ArrayList<>();
    for (String file : localFiles) {
      String s3ObjectPath = config.getCloudPath() + "/" + file.substring(file.lastIndexOf('/')+1);
      s3ObjectPaths.add(s3ObjectPath);
      try {
        s3Client.putObject(
            config.getCloudBucket(),
            s3ObjectPath,
            new File(file)
        );
      }
      catch (Exception e) {
        LOG.error("Unable to upload file %s", file);
        LOG.error(e.toString());
        // Delete rest of the uploaded files
        deleteFilesFromS3(s3ObjectPaths);
        // Pass the exception forward for the test to handle
        throw e;
      }
    }
  }

  /**
   * Deletes a list of files to s3 at the location set in the IT config
   *
   * @param  fileList List of path of files inside a s3 bucket
   */
  public void deleteFilesFromS3(List<String> fileList)
  {
    try {
      String[] fileListArr = new String[fileList.size()];
      DeleteObjectsRequest delObjReq = new DeleteObjectsRequest(config.getCloudBucket())
          .withKeys(fileListArr);
      s3Client.deleteObjects(delObjReq);
    }
    catch (Exception e) {
      // Posting warn instead of error as not being able to delete files from s3 does not impact the test.
      LOG.warn("Unable to delete data files from s3");
      LOG.warn(e.toString());
    }
  }

  /**
   * Deletes all files in a folder in s3 bucket
   *
   * @param  datasource Path of folder inside a s3 bucket
   */
  public void deleteFilesFromS3(String datasource)
  {
    try {
      // Delete segments created by druid
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(config.getCloudBucket())
          .withPrefix(config.getCloudPath() + "/" + datasource + "/");

      ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);

      while (true) {
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          s3Client.deleteObject(config.getCloudBucket(), objectSummary.getKey());
        }
        if (objectListing.isTruncated()) {
          objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } else {
          break;
        }
      }
    }
    catch (Exception e){
      // Posting warn instead of error as not being able to delete files from s3 does not impact the test.
      LOG.warn("Unable to delete data files from s3");
      LOG.warn(e.toString());
    }
  }

  /**
   * Deletes a list of files provide in fileList and all files inside a folder (datasource) in a s3 bucket
   *
   * @param  fileList List of path of files inside a s3 bucket
   * @param  datasource Path of folder inside a s3 bucket
   */
  public void deleteFilesFromS3(List<String> fileList, String datasource)
  {
    try {
      deleteFilesFromS3(fileList);
      deleteFilesFromS3(datasource);
    }
    catch (Exception e){
      // Posting warn instead of error as not being able to delete files from s3 does not impact the test.
      LOG.warn("Unable to delete data files from s3");
      LOG.warn(e.toString());
    }
  }
}
