/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.mongodb;

import com.malhartech.bufferserver.util.Codec;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import java.util.Date;
import java.util.HashMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class MongoDBOutputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(MongoDBOutputOperatorTest.class);
  public String[] hashMapping1 = new String[columnNum];
  public String[] arrayMapping1 = new String[columnNum];
  public final static int maxTuple = 20;
  public final static int columnNum = 5;

  public void buildDataset()
  {
//    hashMapping1[0] = "prop1:t1.col1:STRING";
//    hashMapping1[1] = "prop2:t1.col2:STRING";
//    hashMapping1[2] = "prop5:t1.col5:STRING";
//    hashMapping1[3] = "prop6:t1.col4:STRING";
//    hashMapping1[4] = "prop7:t1.col7:STRING";

    hashMapping1[0] = "prop1:t1.col1:INT";
    hashMapping1[1] = "prop3:t1.col3:STRING";
    hashMapping1[2] = "prop2:t1.col2:DATE";
    hashMapping1[3] = "prop1:t2.col1:STRING";
    hashMapping1[4] = "prop2:t2.col2:INT";

    arrayMapping1[0] = "t1.col1:INT";
    arrayMapping1[1] = "t1.col3:STRING";
    arrayMapping1[2] = "t1.col2:DATE";
    arrayMapping1[3] = "t2.col2:STRING";
    arrayMapping1[4] = "t2.col1:INT";

  }

  public HashMap<String, Object> generateHashMapData(int j, MongoDBHashMapOutputOperator oper)
  {
    HashMap<String, Object> hm = new HashMap<String, Object>();
    for (int i = 0; i < columnNum; i++) {
      String[] tokens = hashMapping1[i].split("[:]");
      String[] subtok = tokens[1].split("[.]");
      String table = subtok[0];
      String column = subtok[1];
      String prop = tokens[0];
      String type = tokens[2];
      if (type.contains("INT")) {
        hm.put(prop, j * columnNum + i);
      }
      else if (type.equals("STRING")) {
        hm.put(prop, String.valueOf(j * columnNum + i));
      }
      else if (type.equals("DATE")) {
        hm.put(prop, new Date());
      }
      oper.propTable.put(prop, table);
    }
    return hm;
  }

  public void readDB(MongoDBHashMapOutputOperator oper)
  {
    for (Object o : oper.getTableNames()) {
      String table = (String)o;
      DBCursor cursor = oper.db.getCollection(table).find();
      while (cursor.hasNext()) {
        System.out.println(cursor.next());
      }
    }
  }

  @Test
  public void MongoDBHashMapOutputOperatorTest()
  {
    buildDataset();

    MongoDBHashMapOutputOperator oper = new MongoDBHashMapOutputOperator();

    oper.addTable("t1");
    oper.addTable("t2");
    oper.setBatchSize(100);
    oper.setDbUrl("localhost");
    oper.setDataBase("test");
    oper.setUserName("test");
    oper.setPassWord("123");
    oper.setWindowIdName("winid");
    oper.setOperatorIdName("operatorid");
    oper.setMaxWindowTable("maxWindowTable");
    oper.setQueryFunction(1);

    oper.setup(new com.malhartech.engine.OperatorContext("1", null, null));
    for (Object o : oper.getTableNames()) {
      String table = (String)o;
      oper.db.getCollection(table).drop();
    }

//    oper.beginWindow(oper.getLastWindowId());
    oper.beginWindow(oper.getLastWindowId() + 1);
    logger.debug("beginwindow {}", Codec.getStringWindowId(oper.getLastWindowId() + 1));

    for (int i = 0; i < maxTuple; ++i) {
      HashMap<String, Object> hm = generateHashMapData(i, oper);
//      logger.debug(hm.toString());
      oper.inputPort.process(hm);

    }
    oper.endWindow();
    readDB(oper);

    oper.teardown();
  }

  @Test
  public void MongoDBArrayListOutputOperatorTest() {
    buildDataset();
    MongoDBArrayListOutputOperator oper = new MongoDBArrayListOutputOperator();

  }
}