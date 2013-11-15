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
package com.datatorrent.demos.performance;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;
import javax.validation.constraints.Min;

/**
 * <p>RandomWordInputModule class.</p>
 *
 * @since 0.3.2
 */
public class RandomWordInputModule implements InputOperator
{
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();
  private transient int count;
  private boolean firstTime;
  
  @Min(1)
  private int tupleSize = 64;
 
  /**
   * Sets the size of the tuple to the specified size
   * 
   * @param size - the tupleSize to set
   */
  public void setTupleSize(int size) 
  {
	  tupleSize = size;
  }
  
  /**
   * @return the tupleSize
   */
  public int getTupleSize()
  {
    return tupleSize;
  }

  @Override
  public void emitTuples()
  {
    if (firstTime) {
      for (int i = count--; i-- > 0;) {
        output.emit(new byte[tupleSize]);
      }
      firstTime = false;
    }
    else {
      output.emit(new byte[tupleSize]);
      count++;
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    firstTime = true;
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

}
