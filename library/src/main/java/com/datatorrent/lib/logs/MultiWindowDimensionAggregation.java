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
package com.datatorrent.lib.logs;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.mutable.MutableLong;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * <p>MultiWindowDimensionAggregation class.</p>
 * This class aggregates the value of given dimension across windows
 *
 * @since 0.3.4
 */
public class MultiWindowDimensionAggregation implements Operator {

  private int windowSize = 2;
  private int currentWindow = 0;
  private int[] dimensionArray;
  private String timeBucket;
  private Pattern pattern;
  private String dimensionKeyVal = "0";
  private String dimensionArrayString;

  private Map<Integer, Map<String, Number>> cacheOject;
  private Map<String, Number> countMap;

  public final transient DefaultOutputPort<Map<String, DimensionObject<String>>> output = new DefaultOutputPort<Map<String, DimensionObject<String>>>();

  public final transient DefaultInputPort<Map<String, Map<String, Number>>> data = new DefaultInputPort<Map<String, Map<String, Number>>>() {
    @Override
      public void process(Map<String, Map<String, Number>> tuple) {
        Collection<String> tupleKeySet = tuple.keySet();
        Iterator<String> tupleKeySetItr = tupleKeySet.iterator();
        String tupleKey = tupleKeySetItr.next().trim();
        String matchString = match(tupleKey);
        if (matchString == null)
          return;

        Map<String, Number> cacheMap = cacheOject.get(currentWindow);
        if (cacheMap == null) {
          cacheMap = new HashMap<String, Number>();
          cacheOject.put(currentWindow, cacheMap);
        }
        Map<String, Number> tupleValue = tuple.get(tupleKey);
        Number n = cacheMap.get(matchString);
        if (n == null) {
          cacheMap.put(matchString, tupleValue.get(dimensionKeyVal));
        } else {
          ((MutableLong) n).add(tupleValue.get(dimensionKeyVal));
        }
      }
  };

  public String getDimensionKeyVal() {
    return dimensionKeyVal;
  }

  public void setDimensionKeyVal(String dimensionKeyVal) {
    this.dimensionKeyVal = dimensionKeyVal;
  }

  private String match(String s) {
    Matcher matcher = pattern.matcher(s);
    if (matcher.matches()) {
      StringBuilder builder = new StringBuilder(matcher.group(2));
      for (int i = 1; i < dimensionArray.length; i++) {
        builder.append("," + matcher.group(i + 2));
      }

      return builder.toString();
    }
    return null;
  }

  public String getTimeBucket() {
    return timeBucket;
  }

  public void setTimeBucket(String timeBucket) {
    this.timeBucket = timeBucket;
  }

  public int[] getDimensionArray() {
    return dimensionArray;
  }

  public void setDimensionArray(int[] dimensionArray) {
    this.dimensionArray = dimensionArray;
    StringBuilder builder = new StringBuilder("" + dimensionArray[0]);
    for (int i = 1; i < dimensionArray.length; i++) {
      builder.append("," + dimensionArray[i]);
    }
    dimensionArrayString = builder.toString();
  }

  public int getWindowSize() {
    return windowSize;
  }

  public void setWindowSize(int windowSize) {
    this.windowSize = windowSize;
  }

  @Override
    public void setup(OperatorContext arg0) {
      cacheOject = new HashMap<Integer, Map<String, Number>>(windowSize);
      countMap = new HashMap<String, Number>();
    }

  @Override
    public void teardown() {

    }

  @Override
    public void beginWindow(long arg0) {
      Map<String, Number> cacheMap = cacheOject.get(currentWindow);
      if (cacheMap == null)
        cacheMap = new HashMap<String, Number>();
      cacheMap.clear();
      countMap.clear();
      if (pattern == null) {
        StringBuilder builder = new StringBuilder(timeBucket + "\\|(\\d+)");
        for (int i = 0; i < dimensionArray.length; i++) {
          builder.append("\\|" + dimensionArray[i] + ":(\\w+)");
        }
        pattern = Pattern.compile(builder.toString());
      }

    }

  @Override
    public void endWindow() {
      Collection<Map<String, Number>> coll = cacheOject.values();
      Iterator<Map<String, Number>> itr = coll.iterator();
      while (itr.hasNext()) {
        Map<String, Number> map = itr.next();
        for (Map.Entry<String, Number> e : map.entrySet()) {
          Number n = countMap.get(e.getKey());
          if (n == null) {
            countMap.put(e.getKey(), new MutableLong(e.getValue()));
          } else {
            ((MutableLong) n).add(e.getValue());
          }
        }
      }

      for (Map.Entry<String, Number> e : countMap.entrySet()) {
        HashMap<String, DimensionObject<String>> outputData = new HashMap<String, DimensionObject<String>>();
        outputData.put(dimensionArrayString, new DimensionObject<String>(e
                                                                         .getValue().longValue(), e.getKey()));
        output.emit(outputData);
      }

      currentWindow = (currentWindow + 1) % windowSize;

    }

}
