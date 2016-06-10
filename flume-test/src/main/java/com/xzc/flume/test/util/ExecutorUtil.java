package com.xzc.flume.test.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class ExecutorUtil
{
  private ExecutorService executor;

  private ExecutorUtil()
  {
    this.executor = Executors.newFixedThreadPool(10);
  }

  public static ExecutorUtil getInstance()
  {
    return HandlerExecutorsHolder.handlerExecutor;
  }

  public void execute(Runnable command)
  {
    this.executor.execute(command);
  }

  private static class HandlerExecutorsHolder
  {
    private static ExecutorUtil handlerExecutor = new ExecutorUtil();
  }
}