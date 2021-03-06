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
package com.datatorrent.lib.math;

/**
 *
 * Emits the result of square of the input tuple (Number).<br>
 * Emits the result as Long on port longResult, as Integer on port integerResult,
 * as Double on port doubleResult, and as Float on port floatResult. This is a pass through operator<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>input</b>: expects Number<br>
 * <b>longResult</b>: emits Long<br>
 * <b>integerResult</b>: emits Integer<br>
 * <b>doubleResult</b>: emits Double<br>
 * <b>floatResult</b>: emits Float<br>
 * <br>
 *
 * @since 0.3.3
 */
public class SquareCalculus extends SingleVariableAbstractCalculus
{
  @Override
  public double function(double dval)
  {
    return dval * dval;
  }

  @Override
  public long function(long lval)
  {
    return lval * lval;
  }

}
