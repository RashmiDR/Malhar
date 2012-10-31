/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.OperatorConfiguration;
import com.malhartech.dag.TestCountAndLastTupleSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.FirstMatch}<p>
 *
 */
public class FirstMatchTest
{
  private static Logger log = LoggerFactory.getLogger(FirstMatchTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new FirstMatch<String, Integer>());
    testNodeProcessingSchema(new FirstMatch<String, Double>());
    testNodeProcessingSchema(new FirstMatch<String, Float>());
    testNodeProcessingSchema(new FirstMatch<String, Short>());
    testNodeProcessingSchema(new FirstMatch<String, Long>());
  }

  public void testNodeProcessingSchema(FirstMatch oper)
  {
    TestCountAndLastTupleSink matchSink = new TestCountAndLastTupleSink();
    oper.first.setSink(matchSink);
    oper.setup(new OperatorConfiguration());
    oper.setKey("a");
    oper.setValue(3);
    oper.setTypeEQ();

    oper.beginWindow();
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 4);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.put("a", 3);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 2);
    oper.data.process(input);
    input.clear();
    input.put("a", 4);
    input.put("b", 21);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 4);
    input.put("b", 20);
    input.put("c", 5);
    oper.data.process(input);
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, matchSink.count);
    HashMap<String, Number> tuple = (HashMap<String, Number>)matchSink.tuple;
    Number aval = tuple.get("a");
    Assert.assertEquals("Value of a was ", 3, aval.intValue());
    matchSink.clear();

    oper.beginWindow();
    input.clear();
    input.put("a", 2);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 5);
    oper.data.process(input);
    oper.endWindow();
    // There should be no emit as all tuples do not match
    Assert.assertEquals("number emitted tuples", 0, matchSink.count);
    matchSink.clear();
  }
}