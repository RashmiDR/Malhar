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
package com.datatorrent.contrib.kafka;

import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import junit.framework.Assert;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Utils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KafkaOutputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaOutputOperatorTest.class);
  private static int tupleCount = 0;
  private static final int maxTuple = 20;
  private KafkaServer kserver;
  private NIOServerCnxnFactory standaloneServerFactory;
  private final String zklogdir = "/tmp/zookeeper-server-data";
  private final String kafkalogdir = "/tmp/kafka-server-data";
  private static boolean useZookeeper = true;  // standard consumer use zookeeper, whereas simpleConsumer don't

  /**
   * Concrete class of KafkaStringSinglePortOutputOperator for testing.
   */
  public static class KafkaStringSinglePortOutputOperator extends KafkaSinglePortOutputOperator<Integer, String>
  {
    /**
     * Implementation of Abstract Method.
     *
     * @return
     */
    @Override
    public ProducerConfig createKafkaProducerConfig()
    {
      Properties props = new Properties();
      props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
      if (useZookeeper)
      {
        props.setProperty("zk.connect", "localhost:2182");
      }
      else {
        props.setProperty("broker.list", "1:localhost:2182");
        props.setProperty("producer.type", "async");
        props.setProperty("queue.time", "2000");
        props.setProperty("queue.size", "100");
        props.setProperty("batch.size", "10");
      }

      return new ProducerConfig(props);
    }
  } // End of KafkaStringSinglePortOutputOperator


  public void startZookeeper()
  {
    if (!useZookeeper) { // Do not use zookeeper for simpleconsumer
      return;
    }

    try {
      int clientPort = 2182;
      int numConnections = 5000;
      int tickTime = 2000;
      File dir = new File(zklogdir);

      ZooKeeperServer zserver = new ZooKeeperServer(dir, dir, tickTime);
      standaloneServerFactory = new NIOServerCnxnFactory();
      standaloneServerFactory.configure(new InetSocketAddress(clientPort), numConnections);
      standaloneServerFactory.startup(zserver); // start the zookeeper server.
    }
    catch (InterruptedException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
    catch (IOException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }

  public void stopZookeeper()
  {
    if (!useZookeeper) {
      return;
    }

    standaloneServerFactory.shutdown();
    Utils.rm(zklogdir);
  }

  public void startKafkaServer()
  {
    Properties props = new Properties();
    if (useZookeeper) {
      props.setProperty("enable.zookeeper", "true");
      props.setProperty("zk.connect", "localhost:2182");
      props.setProperty("topic", "topic1");
      props.setProperty("log.flush.interval", "10"); // Controls the number of messages accumulated in each topic (partition) before the data is flushed to disk and made available to consumers.
      //   props.setProperty("log.default.flush.scheduler.interval.ms", "100");  // optional if we have the flush.interval
    }
    else {
      props.setProperty("enable.zookeeper", "false");
      props.setProperty("hostname", "localhost");
      props.setProperty("port", "2182");
    }
    props.setProperty("brokerid", "1");
    props.setProperty("log.dir", kafkalogdir);

    kserver = new KafkaServer(new KafkaConfig(props));
    kserver.startup();
  }

  public void stopKafkaServer()
  {
    kserver.shutdown();
    kserver.awaitShutdown();
    Utils.rm(kafkalogdir);
  }

  @Before
  public void beforeTest()
  {
    try {
      startZookeeper();
      startKafkaServer();
    }
    catch (java.nio.channels.CancelledKeyException ex) {
      logger.debug("LSHIL {}", ex.getLocalizedMessage());
    }
  }

  @After
  public void afterTest()
  {
    try {
      stopKafkaServer();
      stopZookeeper();
    }
    catch (java.nio.channels.CancelledKeyException ex) {
      logger.debug("LSHIL {}", ex.getLocalizedMessage());
    }
  }

   /**
   * Tuple generator for testing.
   */
  public static class StringGeneratorInputOperator implements InputOperator, ActivationListener<OperatorContext>
  {
    public final transient DefaultOutputPort<String> outputPort = new DefaultOutputPort<String>();
    private final transient ArrayBlockingQueue<String> stringBuffer = new ArrayBlockingQueue<String>(1024);
    private volatile Thread dataGeneratorThread;

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {
    }

    @Override
    public void activate(OperatorContext ctx)
    {
      dataGeneratorThread = new Thread("String Generator")
      {
        @Override
        @SuppressWarnings("SleepWhileInLoop")
        public void run()
        {
          try {
            int i = 0;
            while (dataGeneratorThread != null && i < maxTuple) {
              stringBuffer.put("testString " + (++i));
              tupleCount++;
              Thread.sleep(20);
            }
          }
          catch (InterruptedException ie) {
          }
        }
      };
      dataGeneratorThread.start();
    }

    @Override
    public void deactivate()
    {
      dataGeneratorThread = null;
    }

    @Override
    public void emitTuples()
    {
      for (int i = stringBuffer.size(); i-- > 0;) {
        outputPort.emit(stringBuffer.poll());
      }
    }
  } // End of StringGeneratorInputOperator

  /**
   * Test KafkaOutputOperator (i.e. an output adapter for Kafka, aka producer).
   * This module sends data into an ActiveMQ message bus.
   *
   * [Generate tuple] ==> [send tuple through Kafka output adapter(i.e. producer) into Kafka message bus]
   * ==> [receive data in outside Kaka listener (i.e consumer)]
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings({"SleepWhileInLoop", "empty-statement"})
  public void testKafkaOutputOperator() throws Exception
  {
    // Setup a message listener to receive the message
    KafkaConsumer listener = new KafkaConsumer("topic1");
    new Thread(listener).start();

    // Malhar module to send message
    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    // Create ActiveMQStringSinglePortOutputOperator
    StringGeneratorInputOperator generator = dag.addOperator("TestStringGenerator", StringGeneratorInputOperator.class);
    KafkaStringSinglePortOutputOperator node = dag.addOperator("Kafka message producer", KafkaStringSinglePortOutputOperator.class);
    // Set configuration parameters for Kafka
    node.setTopic("topic1");

    // Connect ports
    dag.addStream("Kafka message", generator.outputPort, node.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    Thread.sleep(2000);
    lc.shutdown();

    // Check values send vs received
    Assert.assertEquals("Number of emitted tuples", tupleCount, listener.holdingBuffer.size());
    logger.debug(String.format("Number of emitted tuples: %d", listener.holdingBuffer.size()));
    Assert.assertEquals("First tuple", "testString 1", listener.getMessage(listener.holdingBuffer.peek()));

    listener.close();
  }

  //@Test
  public void test1() throws Exception
  {
    testKafkaOutputOperator();

  }

}
