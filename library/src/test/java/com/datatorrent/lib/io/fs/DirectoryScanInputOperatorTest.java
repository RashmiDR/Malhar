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
package com.datatorrent.lib.io.fs;


import java.io.File;
import java.io.IOException;
import com.datatorrent.lib.testbench.CollectorTestSink;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

/**
 * Units tests for DirectorySanInputOperator
 */
public class DirectoryScanInputOperatorTest
{
	// Sample text file path.
	protected String dirName = "../library/src/test/resources/directoryScanTestDirectory";


	@SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
	public void testDirectoryScan() throws InterruptedException, IOException
	{
		DirectoryScanInputOperator oper = new DirectoryScanInputOperator();
		oper.setDirectoryPath(dirName);
		oper.setScanIntervalInMilliSeconds(1000);
		oper.activate(null);

		CollectorTestSink sink = new CollectorTestSink();
		oper.outport.setSink(sink);
		
		//Test if the existing files in the directory are emitted.
		Thread.sleep(1000);
		oper.emitTuples();
    
    Assert.assertTrue("tuple emmitted: " + sink.collectedTuples.size(), sink.collectedTuples.size() > 0);
		Assert.assertEquals(sink.collectedTuples.size(), 10);
		
		
		//Test if the new file added is detected
		sink.collectedTuples.clear();
		
		File oldFile = new File("../library/src/test/resources/directoryScanTestDirectory/testFile1.txt");
		File newFile = new File("../library/src/test/resources/directoryScanTestDirectory/newFile.txt");
		FileUtils.copyFile(oldFile, newFile);

		int timeoutMillis = 2000;
    while (sink.collectedTuples.isEmpty() && timeoutMillis > 0) {
      oper.emitTuples();
      timeoutMillis -= 20;
      Thread.sleep(20);
    }
    		
    Assert.assertTrue("tuple emmitted: " + sink.collectedTuples.size(), sink.collectedTuples.size() > 0);
    Assert.assertEquals(sink.collectedTuples.size(), 1);
    
    //clean up the directory to initial state
    newFile.delete();    
		
    oper.deactivate();
		oper.teardown();
	}

}
