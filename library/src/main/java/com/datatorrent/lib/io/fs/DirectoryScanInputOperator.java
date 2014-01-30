/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.fs;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;

import java.io.File;

import java.util.concurrent.LinkedBlockingQueue;

import javax.validation.constraints.NotNull;
import org.apache.commons.io.FilenameUtils;
import org.python.antlr.PythonParser.and_expr_return;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;

/**
 *
 */
/**
 * Input Adapter for scanning a specified directory
 * <p>
 * <br>
 * 
 * <br>
 * 
 * @param <STREAM>
 * @since 0.9.3
 */
public class DirectoryScanInputOperator extends BaseOperator implements InputOperator, ActivationListener<OperatorContext>
{
  private File baseDirectory = null;

  @NotNull
  private static final Logger LOG = LoggerFactory.getLogger(DirectoryScanInputOperator.class);
  private String directoryPath;//the directory that is scanned
  private int scanIntervalInMilliSeconds = 1000;// the time interval for periodically scanning, Default is 1 sec = 1000ms
  private int fileCountPerEmit = 200; // the maximum number of file info records that will be output per emit

  public final transient DefaultOutputPort<FileInfoRecord> outport = new DefaultOutputPort<FileInfoRecord>();
  private transient FileAlterationObserver observer;
  private transient FAListener listner;

  private LinkedBlockingQueue<FileInfoRecord> fileRecordsQueue = new LinkedBlockingQueue<FileInfoRecord>();

  public class FAListener implements FileAlterationListener
  {

    // Is triggered when a file is created in the monitored folder
    @Override
    public void onFileCreate(File file)
    {
      LOG.info("onFileCreate : " + file);
      FileInfoRecord fileRecord = new FileInfoRecord(file.getAbsolutePath(), FilenameUtils.getExtension(file.getAbsolutePath()), file.length());
      // fileRecordsQueue.add(fileRecord);
      try {
        fileRecordsQueue.put(fileRecord);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onFileDelete(File file)
    {
      LOG.info("onFileDelete : " + file);
      FileInfoRecord fileToDelete = new FileInfoRecord(file.getAbsolutePath(), FilenameUtils.getExtension(file.getAbsolutePath()), file.length());
      fileRecordsQueue.remove(fileToDelete);
    }

    @Override
    public void onDirectoryCreate(File directory)
    {
      //files within the directory will in turn generate onFileCreate events
      LOG.info("onDirectoryCreate : " + directory);

    }

    @Override
    public void onDirectoryDelete(File directory)
    {
     //files within the directory will in turn generate onFileDelete events
      LOG.info("onDirectoryDelete : " + directory);
    }

    @Override
    public void onDirectoryChange(File directory)
    {
      LOG.info("onDirectoryChange : " + directory);
    }

    @Override
    public void onFileChange(File file)
    {
      LOG.info("onFileChange : " + file);
    }

    @Override
    public void onStart(FileAlterationObserver arg0)
    {
      LOG.info("onStart of FileAlterationListener!!!!!");

    }

    @Override
    public void onStop(FileAlterationObserver arg0)
    {
      LOG.info("onStop of FileAlterationListener!!!!!");
    }
  }

  public class DirectoryScanner implements Runnable
  {
    public void run()
    {
      try {
        while (true) {
          observer.checkAndNotify();
          // Sleep for Scan interval
          Thread.sleep(scanIntervalInMilliSeconds);
        }
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  // FAListener listener = new FAListener();
  Thread directoryScanThread = new Thread(new DirectoryScanner());

  /**
   * Sets the path of the directory to be scanned. The separators in path are replaced with File.separatorChar and the
   * directory instance is created
   * 
   * @param dirPath
   *          the path of the directory to be scanned
   */
  public void setDirectoryPath(String dirPath)
  {
    this.directoryPath = dirPath;

    baseDirectory = new File(directoryPath.replace('/', File.separatorChar).replace('\\', File.separatorChar));

    if (!baseDirectory.exists()) {
      // Check if monitored folder exists
      throw new RuntimeException("Directory not found: " + baseDirectory);
    }
  }

  /**
   * Returns the path of the directory being scanned.
   * 
   * @return directoryPath the path of directory being scanned.
   */
  public String getDirectoryPath()
  {
    return directoryPath;
  }

  /**
   * Sets the time interval at which the directory is to be scanned
   * 
   * @param scanIntervalInMilliSeconds
   *          the time interval for scanning the directory
   */
  public void setScanIntervalInMilliSeconds(int scanIntervalInMilliSeconds)
  {
    this.scanIntervalInMilliSeconds = scanIntervalInMilliSeconds;
  }

  /**
   * Returns the interval at which the directory is being scanned.
   * 
   * @return scanIntervalInMilliSeconds the interval at which the directory is being scanned
   */
  public int getScanIntervalInMilliSeconds()
  {
    return scanIntervalInMilliSeconds;
  }

  /**
   * Sets the number of file records to output in one emit cycle
   * 
   * @param fileCount
   *          the number of file records to output per emit
   */
  public void setFileCountPerEmit(int fileCount)
  {
    this.fileCountPerEmit = fileCount;
  }

  /**
   * Returns the number of file records that are output per emit.
   * 
   * @return fileCountPerEmit the number of file records that are output per emit
   */
  public int getFileCountPerEmit()
  {
    return fileCountPerEmit;
  }

  @Override
  public void setup(OperatorContext context)
  {

  }

  /**
   * Initializes the directory scan monitoring mechanism. and starts the thread that periodically scans the directory
   */
  @Override
  final public void activate(OperatorContext ctx)
  {
    // Start the directory monitor
    observer = new FileAlterationObserver(baseDirectory);

    listner = new FAListener();
    observer.addListener(listner);

    directoryScanThread.start();
  }

  /**
   * Stops the thread that periodically scans the directory
   */
  @Override
  final public void deactivate()
  {
    // Interrupt the directoryScanThread and wait for it to join
    directoryScanThread.interrupt();
    try {
      directoryScanThread.join();
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
    }
  }

  @Override
  public void teardown()
  {

  }

  /**
   * Emits the file info records up-to a maximum of the specified number of file records per emit. Each file info record
   * has the file name, file type {@link and_expr_return} file size.
   */
  @Override
  public void emitTuples()
  {
    int count = fileCountPerEmit;
    try {
      while (!fileRecordsQueue.isEmpty() && count > 0) {
        outport.emit(fileRecordsQueue.take());
        count--;
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {

  }

  @Override
  public void endWindow()
  {
  }

}
