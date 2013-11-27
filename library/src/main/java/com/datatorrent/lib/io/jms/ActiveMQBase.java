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
package com.datatorrent.lib.io.jms;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.validation.constraints.NotNull;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.annotation.ShipContainingJars;

/**
 * Base class for any ActiveMQ input or output adapter operator. <p><br>
 * Operators should not be derived from this,
 * rather from AbstractActiveMQInputOperator or AbstractActiveMQSinglePortInputOperator or AbstractActiveMQOutputOperator
 * or AbstractActiveMQSinglePortOutputOperator. This creates connection with active MQ broker.<br>
 *
 * <br>
 * Ports:<br>
 * None<br>
 * <br>
 * Properties:<br>
 * <b>usr</b>: userid for connecting to active MQ message bus<br>
 * <b>password</b>: password for connecting to active MQ message bus<br>
 * <b>url</b>: URL for connecting to active MQ message bus<br>
 * <b>ackMode</b>: message acknowledgment mode<br>
 * <b>clientId</b>: client id<br>
 * <b>subject</b>: name of destination<br>
 * <b>durable</b>: flag to indicate durable consumer<br>
 * <b>topic</b>: flag to indicate if the destination is a topic or queue<br>
 * <b>transacted</b>: flag whether the messages should be transacted or not<br>
 * <br>
 * Compile time checks:<br>
 * usr should not be null<br>
 * password should not be null<br>
 * url should not be null<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * NA<br>
 * <br>
 *
 * @since 0.3.2
 */
@ShipContainingJars(classes = {javax.jms.Message.class, org.apache.activemq.ActiveMQConnectionFactory.class, javax.management.j2ee.statistics.Stats.class})
public class ActiveMQBase
{
  private static final Logger logger = LoggerFactory.getLogger(ActiveMQBase.class);
  private transient Connection connection;
  private transient Session session;
  private transient Destination destination;

  @NotNull
  private String user = "";
  @NotNull
  private String password = "";
  @NotNull
  private String url;
  private String ackMode = "CLIENT_ACKNOWLEDGE";
  private String clientId = "TestClient";
  private String subject = "TEST.FOO";
  private int batch = 10;
  private int messageSize = 255;
  private boolean durable = false;
  private boolean topic = false;
  private boolean transacted = false;
  private boolean verbose = false;

  /**
   * @return the connection 
   */
  public Connection getConnection()
  {
    return connection;
  }

  /**
   * @return the session 
   */
  public Session getSession()
  {
    return session;
  }

  /**
   * @return the destination 
   */
  public Destination getDestination()
  {
    return destination;
  }

  /**
   * @return the username 
   */
  public String getUser()
  {
    return user;
  }

  /**
   * @param user- the username to set for connecting to active MQ message bus
   */
  public void setUser(String user)
  {
    this.user = user;
  }

  /**
   * @return the password 
   */
  public String getPassword()
  {
    return password;
  }

  /**
   * @param password- the password to set for connecting to active MQ message bus
   */
  public void setPassword(String password)
  {
    this.password = password;
  }

  /**
   * @return the url 
   */
  public String getUrl()
  {
    return url;
  }

  /**
   * @param url- the url to set 
   */
  public void setUrl(String url)
  {
    this.url = url;
  }

  /**
   * @return the message acknowledgment mode 
   */
  public String getAckMode()
  {
    return ackMode;
  }

  /**
   * @param ackMode- the message acknowledgment mode to set
   */
  public void setAckMode(String ackMode)
  {
    this.ackMode = ackMode;
  }

  /**
   * @return the clientId
   */
  public String getClientId()
  {
    return clientId;
  }

  /**
   * @param clientId- the clientId to set
   */
  public void setClientId(String clientId)
  {
    this.clientId = clientId;
  }

  /**
   * @return the name of the destination
   */
  public String getSubject()
  {
    return subject;
  }
  
  /**
   * @param subject- the name of the destination to set
   */
  public void setSubject(String subject)
  {
    this.subject = subject;
  }

  /**
   * @return the batch
   */
  public int getBatch()
  {
    return batch;
  }

  /**
   * @param batch- the batch to set
   */
  public void setBatch(int batch)
  {
    this.batch = batch;
  }

  /**
   * @return the message size
   */
  public int getMessageSize()
  {
    return messageSize;
  }

  /**
   * @param messageSize- the size of the message to set
   */
  public void setMessageSize(int messageSize)
  {
    this.messageSize = messageSize;
  }

  /**
   * @return the durability of the consumer
   */
  public boolean isDurable()
  {
    return durable;
  }

  /**
   * @param durable- the durable flag to set to indicate durable consumer
   */
  public void setDurable(boolean durable)
  {
    this.durable = durable;
  }

  /**
   * @return the topic 
   */
  public boolean isTopic()
  {
    return topic;
  }

  /**
   * @param topic- the topic to set to indicate if the destination is a topic or queue
   */
  public void setTopic(boolean topic)
  {
    this.topic = topic;
  }

  /**
   * @return the transacted 
   */
  public boolean isTransacted()
  {
    return transacted;
  }

  /**
   * @param transacted- the flag to set whether the messages should be transacted or not
   */
  public void setTransacted(boolean transacted)
  {
    this.transacted = transacted;
  }

  public boolean isVerbose()
  {
    return verbose;
  }

  /**
   * @param verbose- the flag to set to enable verbose option 
   */
  public void setVerbose(boolean verbose)
  {
    this.verbose = verbose;
  }

  /**
   *  Get session acknowledge.
   *  Converts acknowledge string into JMS Session variable.
   */
  public int getSessionAckMode(String ackMode)
  {
    if ("CLIENT_ACKNOWLEDGE".equals(ackMode)) {
      return Session.CLIENT_ACKNOWLEDGE;
    }
    else if ("AUTO_ACKNOWLEDGE".equals(ackMode)) {
      return Session.AUTO_ACKNOWLEDGE;
    }
    else if ("DUPS_OK_ACKNOWLEDGE".equals(ackMode)) {
      return Session.DUPS_OK_ACKNOWLEDGE;
    }
    else if ("SESSION_TRANSACTED".equals(ackMode)) {
      return Session.SESSION_TRANSACTED;
    }
    else {
      return Session.CLIENT_ACKNOWLEDGE; // default
    }
  }

  /**
   *  Connection specific setup for ActiveMQ.
   *
   *  @throws JMSException
   */
  public void createConnection() throws JMSException
  {
    // Create connection
    ActiveMQConnectionFactory connectionFactory;
    connectionFactory = new ActiveMQConnectionFactory(user, password, url);

    connection = connectionFactory.createConnection();
    if (durable && clientId != null) {
      connection.setClientID(clientId);
    }
    connection.start();

    // Create session
    session = connection.createSession(transacted, getSessionAckMode(ackMode));

    // Create destination
    destination = topic
                  ? session.createTopic(subject)
                  : session.createQueue(subject);
  }

  /**
   *  cleanup connection resources.
   */
  public void cleanup()
  {
    try {
      session.close();
      connection.close();
      session = null;
      connection = null;
    }
    catch (JMSException ex) {
      logger.debug(ex.getLocalizedMessage());
    }
  }
}
