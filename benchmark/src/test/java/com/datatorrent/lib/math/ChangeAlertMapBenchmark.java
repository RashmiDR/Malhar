/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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

import com.datatorrent.lib.math.ChangeAlertMap;
import com.datatorrent.lib.testbench.CountTestSink;

import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Performance tests for {@link com.datatorrent.lib.math.ChangeAlertMap}. <p>
 *
 */
public class ChangeAlertMapBenchmark
{
  private static Logger log = LoggerFactory.getLogger(ChangeAlertMapBenchmark.class);

  /**
   * Test node logic emits correct results.
   */
  @Test
  @Category(com.datatorrent.lib.annotation.PerformanceTestCategory.class)
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new ChangeAlertMap<String, Integer>());
    testNodeProcessingSchema(new ChangeAlertMap<String, Double>());
    testNodeProcessingSchema(new ChangeAlertMap<String, Float>());
    testNodeProcessingSchema(new ChangeAlertMap<String, Short>());
    testNodeProcessingSchema(new ChangeAlertMap<String, Long>());
  }

  public <V extends Number> void testNodeProcessingSchema(ChangeAlertMap<String, V> oper)
  {
    CountTestSink alertSink = new CountTestSink<HashMap<String, HashMap<V, Double>>>();

    oper.alert.setSink(alertSink);
    oper.setPercentThreshold(5);

    oper.beginWindow(0);
    HashMap<String, V> input = new HashMap<String, V>();

    int numTuples = 10000000;
    for (int i = 0; i < numTuples; i++) {
      input.clear();
      input.put("a", oper.getValue(i));
      input.put("b", oper.getValue(i + 2));
      oper.data.process(input);

      input.clear();
      input.put("a", oper.getValue(i + 1));
      input.put("b", oper.getValue(i + 3));

      oper.data.process(input);
      if (i % 100000 == 0) {
        input.clear();
        input.put("a", oper.getValue(10));
        input.put("b", oper.getValue(33));
        oper.data.process(input);
      }
    }
    oper.endWindow();
    // One for each key
    log.debug(String.format("\nBenchmarked %d tuples, emitted %d", numTuples * 4, alertSink.getCount()));
  }
}