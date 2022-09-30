package org.apache.druid.cli;

import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.apache.druid.guice.GuiceInjectors;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CliPeonTest
{

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();


  @Test
  public void testCliPeonK8sMode() throws IOException
  {
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);
    GuiceRunnable runnable = new FakeCliPeon(file.getParent(), true);
    final Injector injector = GuiceInjectors.makeStartupInjector();
    injector.injectMembers(runnable);
    Assert.assertNotNull(runnable.makeInjector());
  }

  @Test
  public void testCliPeonNonK8sMode() throws IOException
  {
    File file = temporaryFolder.newFile("task.json");
    FileUtils.write(file, "{\"type\":\"noop\"}", StandardCharsets.UTF_8);
    GuiceRunnable runnable = new FakeCliPeon(file.getParent(), false);
    final Injector injector = GuiceInjectors.makeStartupInjector();
    injector.injectMembers(runnable);
    Assert.assertNotNull(runnable.makeInjector());
  }

  private static class FakeCliPeon extends CliPeon
  {
    List<String> taskAndStatusFile = new ArrayList<>();

    FakeCliPeon(String taskDirectory, boolean runningOnK8s)
    {
      try {
        taskAndStatusFile.add(taskDirectory);
        taskAndStatusFile.add("1");

        Field privateField = CliPeon.class
            .getDeclaredField("taskAndStatusFile");
        privateField.setAccessible(true);
        privateField.set(this, taskAndStatusFile);


        if (runningOnK8s) {
          System.setProperty("druid.indexer.runner.type", "k8s");
        }
      }
      catch (Exception ex) {
        // do nothing
      }

    }
  }
}
