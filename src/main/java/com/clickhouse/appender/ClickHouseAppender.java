package com.clickhouse.appender;

import com.clickhouse.appender.manager.LogBufferManager;
import com.clickhouse.appender.util.ConnectionSettings;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.*;
import org.apache.logging.log4j.core.layout.PatternLayout;

@Plugin(
    name = "ClickHouseAppender",
    category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE,
    printObject = true)
public class ClickHouseAppender extends AbstractAppender {

  private static final int DEFAULT_BUFFER_SIZE = 8192;
  private static final int DEFAULT_FLUSH_TIMEOUT_SEC = 30;
  private static final String DEFAULT_TABLE_NAME = "logs";
  private static final int DEFAULT_FLUSH_RETRY_COUNT = 3;
  private static final int DEFAULT_SLEEP_ON_FLUSH_RETRY_SEC = 3;

  private LogBufferManager logBufferManager;

  private ClickHouseAppender(
      String name,
      Filter filter,
      Layout<String> layout,
      boolean ignoreExceptions,
      int bufferSize,
      int bufferFlushTimeoutSec,
      String tableName,
      int flushRetryCount,
      int sleepOnRetrySec,
      ConnectionSettings connectionSettings) {
    super(name, filter, layout, false, null);

    this.logBufferManager =
        new LogBufferManager(
            bufferSize,
            bufferFlushTimeoutSec,
            tableName,
            flushRetryCount,
            sleepOnRetrySec,
            connectionSettings);
  }

  public void setLogBufferManager(LogBufferManager logBufferManager) {
    this.logBufferManager = logBufferManager;
  }

  public ClickHouseAppender()
  {
    super("test_name", null, PatternLayout.createDefaultLayout(), true, null);
  }

  @PluginFactory
  public static ClickHouseAppender createAppender(
      @PluginAttribute("name") String name,
      @PluginElement("Filters") Filter filter,
      @PluginElement("layout") Layout<String> layout,
      @PluginAttribute("ignoreExceptions") boolean ignoreExceptions,
      @PluginAttribute("bufferSize") int bufferSize,
      @PluginAttribute("timeoutSec") int timeoutSec,
      @PluginAttribute("tableName") String tableName,
      @PluginAttribute("flushRetryCount") int flushRetryCount,
      @PluginAttribute("sleepOnRetrySec") int sleepOnRetrySec,
      @PluginElement("ConnectionSettings") ConnectionSettings connectionSettings) {

    if (name == null) {
      LOGGER.info("No name provided for ClickHouseAppender, default name is set");
      name = "ClickHouseAppender";
    }

    if (layout == null) {
      LOGGER.error("No layout provided for ClickHouseAppender, exit...");
      throw new RuntimeException();
    }

    if (bufferSize == 0) {
      LOGGER.info("No buffer size provided, default value is set - {}", DEFAULT_BUFFER_SIZE);
      bufferSize = DEFAULT_BUFFER_SIZE;
    }

    if (timeoutSec == 0) {
      LOGGER.info("No timeout for flush provided, default value is set - {}", DEFAULT_FLUSH_TIMEOUT_SEC);
      timeoutSec = DEFAULT_FLUSH_TIMEOUT_SEC;
    }

    if (tableName == null) {
      LOGGER.info("No table provided, default table is set - {}", DEFAULT_TABLE_NAME);
      tableName = DEFAULT_TABLE_NAME;
    }

    if (flushRetryCount == 0) {
      LOGGER.info("No flush retry count provided, default value is set - {}", DEFAULT_FLUSH_RETRY_COUNT);
      flushRetryCount = DEFAULT_FLUSH_RETRY_COUNT;
    }

    if(sleepOnRetrySec == 0)
    {
      LOGGER.info("No sleep retry count provided, default value is set - {}", DEFAULT_SLEEP_ON_FLUSH_RETRY_SEC);
      sleepOnRetrySec = DEFAULT_SLEEP_ON_FLUSH_RETRY_SEC;
    }

    return new ClickHouseAppender(
        name,
        filter,
        layout,
        ignoreExceptions,
        bufferSize,
        timeoutSec,
        tableName,
        flushRetryCount,
        sleepOnRetrySec,
        connectionSettings);
  }

  @Override
  public void append(LogEvent event) {
    String serializedEvent = (String) getLayout().toSerializable(event);

    // System delimiter is replaced with empty string to prevent
    // an error related to default delimiter:
    // \r\n causes ClickHouse to return an error
    if (serializedEvent.endsWith(System.lineSeparator())) {
      serializedEvent =
          serializedEvent.substring(0, serializedEvent.length() - System.lineSeparator().length());
    }

    logBufferManager.insertLogMsg(event.getTimeMillis(), serializedEvent);
  }
}
