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
package com.datatorrent.lib.stream;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseKeyOperator;

/**
 * Duplicates an input stream as is into two output streams; needed to allow separation of listeners into two streams with different properties (for example
 * inline vs in-rack)<p>
 * This is a pass through operator<br>
 * <br>
 * <b>Port Interface</b><br>
 * <b>data</b>: expects &lt;K&gt;<br>
 * <b>out1</b>: emits &lt;K&gt;<br>
 * <b>out2</b>: emits &lt;K&gt;<br>
 * <br>
 *
 * @since 0.3.2
 */
public class StreamDuplicater<K> extends BaseKeyOperator<K>
{
	/**
	 * Input port.
	 */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>()
  {
    /**
     * Emits tuple on both streams
     */
    @Override
    public void process(K tuple)
    {
      out1.emit(cloneKey(tuple));
      out2.emit(cloneKey(tuple));
    }
  };

  /**
   * Output port 1.
   */
  @OutputPortFieldAnnotation(name = "out1")
  public final transient DefaultOutputPort<K> out1 = new DefaultOutputPort<K>();
  
  /**
   * Output port 2.
   */
  @OutputPortFieldAnnotation(name = "out2")
  public final transient DefaultOutputPort<K> out2 = new DefaultOutputPort<K>();
}
