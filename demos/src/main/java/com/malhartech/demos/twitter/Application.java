/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.api.DAG;
import com.malhartech.dag.ApplicationFactory;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Application implements ApplicationFactory
{
  @Override
  public DAG getApplication(Configuration conf)
  {
    return new TwitterTopCounter(conf, new Random().nextBoolean());
  }
}