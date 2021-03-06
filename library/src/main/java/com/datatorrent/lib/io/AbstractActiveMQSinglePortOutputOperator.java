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
package com.datatorrent.lib.io;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import javax.jms.JMSException;
import javax.jms.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ActiveMQ output adapter operator with only one input port, which produce data into ActiveMQ message bus.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Have only one input port<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method createMessage() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 *
 * @since 0.3.2
 */
public abstract class AbstractActiveMQSinglePortOutputOperator<T> extends AbstractActiveMQOutputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractActiveMQSinglePortOutputOperator.class);
  long countMessages = 0;  // Number of messages produced so far

  /**
   * Convert tuple into JMS message. Tuple can be any Java Object.
   * @param tuple
   * @return Message
   */
  protected abstract Message createMessage(T tuple);

  /**
   * The single input port.
   */
  @InputPortFieldAnnotation(name = "ActiveMQInputPort")
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      countMessages++;

      if (countMessages > maxSendMessage && maxSendMessage != 0) {
        if (countMessages == maxSendMessage + 1) {
          logger.warn("Reached maximum send messages of {}", maxSendMessage);
        }
        return; // Stop sending messages after max limit.
      }

      try {
        Message msg = createMessage(tuple);
        getProducer().send(msg);
        //logger.debug("process message {}", tuple.toString());
      }
      catch (JMSException ex) {
        logger.debug(ex.getLocalizedMessage());
      }
    }
  };
}
