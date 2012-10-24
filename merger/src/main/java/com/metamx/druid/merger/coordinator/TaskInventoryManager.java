package com.metamx.druid.merger.coordinator;

import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.InventoryManagementStrategy;
import com.metamx.druid.client.InventoryManager;
import com.metamx.druid.client.InventoryManagerConfig;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.config.IndexerZkConfig;
import com.metamx.druid.merger.worker.Worker;
import com.metamx.phonebook.PhoneBook;
import com.metamx.phonebook.PhoneBookPeon;

import java.util.Map;

/**
 * A simple {@link InventoryManager} that monitors ZK for the creation and deletion of new Workers and the
 * tasks each worker is assigned.
 */
public class TaskInventoryManager extends InventoryManager<Worker>
{
  public TaskInventoryManager(
      IndexerZkConfig config,
      PhoneBook yp
  )
  {
    super(
        new Logger(TaskInventoryManager.class.getName() + "." + config.getStatusPath()),
        new InventoryManagerConfig(
            config.getAnnouncementPath(),
            config.getStatusPath()
        ),
        yp,
        new WorkerInventoryManagementStrategy(
            new Logger(
                TaskInventoryManager.class.getName() + "." + config.getStatusPath()
            )
        )
    );
  }

  private static class WorkerInventoryManagementStrategy implements InventoryManagementStrategy<Worker>
  {
    private final Logger log;

    public WorkerInventoryManagementStrategy(
        Logger log
    )
    {
      this.log = log;
    }

    @Override
    public Class<Worker> getContainerClass()
    {
      return Worker.class;
    }

    @Override
    public Pair<String, PhoneBookPeon<?>> makeSubListener(final Worker worker)
    {
      return new Pair<String, PhoneBookPeon<?>>(
          worker.getHost(),
          new PhoneBookPeon<TaskStatus>()
          {
            @Override
            public Class<TaskStatus> getObjectClazz()
            {
              return TaskStatus.class;
            }

            @Override
            public void newEntry(String name, TaskStatus taskStatus)
            {
              worker.addTask(taskStatus);
              log.info("Worker[%s] has new task[%s] in ZK", worker.getHost(), taskStatus.getId());
            }

            @Override
            public void entryRemoved(String taskId)
            {
              worker.removeTask(taskId);
              log.info("Worker[%s] removed task[%s] in ZK", worker.getHost(), taskId);
            }
          }
      );
    }

    @Override
    public void objectRemoved(Worker baseObject)
    {
    }

    @Override
    public boolean doesSerde()
    {
      return false;
    }

    @Override
    public Worker deserialize(String name, Map<String, String> properties)
    {
      throw new UnsupportedOperationException();
    }
  }
}
