/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.groovyx;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockProcessorInitializationContext;

import org.apache.commons.io.FileUtils;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.processor.exception.ProcessException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;

import java.util.List;
import java.util.HashMap;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;

import static org.junit.Assert.assertNotNull;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;

public class ExecuteGroovyScriptTest {
    private final static String DB_LOCATION = "target/db";

    protected TestRunner runner;
    protected ExecuteGroovyScript proc;
    public final String TEST_RESOURCE_LOCATION = "target/test/resources/groovy/";
    private final String TEST_CSV_DATA = "gender,title,first,last\n"
            + "female,miss,marlene,shaw\n"
            + "male,mr,todd,graham";



    @AfterClass
    public static void cleanUpAfterClass() throws Exception {
        try {
            DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";shutdown=true");
        } catch (Exception e) {
        }
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        FileUtils.deleteQuietly(dbLocation);
    }

    /**
     * Copies all scripts to the target directory because when they are compiled they can leave unwanted .class files.
     *
     * @throws Exception Any error encountered while testing
     */
    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        FileUtils.copyDirectory(new File("src/test/resources"), new File("target/test/resources"));
        //prepare database connection
        System.setProperty("derby.stream.error.file", "target/derby.log");

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        FileUtils.deleteQuietly(dbLocation);
    }
    
    @Before
    public void setup() throws Exception {
        try{
        proc = new ExecuteGroovyScript();
        MockProcessContext context = new MockProcessContext(proc);
        MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(proc, context);
        proc.initialize(initContext);
        
        assertNotNull(proc.getSupportedPropertyDescriptors());
        runner = TestRunners.newTestRunner(proc);
        //insert come test data
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final HashMap<String, String> dbcpProperties = new HashMap<>();
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        Connection con = ((DBCPService) runner.getControllerService("dbcp"))
            .getConnection();
        Statement stmt = con.createStatement();
        try{
            stmt.execute("drop table mytable");
        }catch(Exception e){}
        stmt.execute("create table mytable (id integer not null, name varchar(100), scale float, created timestamp, data blob)");
        stmt.execute("insert into mytable (id, name, scale, created, data) VALUES (0, 'Joe Smith', 1.0, '1962-09-23 03:23:34.234', null)");
        stmt.execute("insert into mytable (id, name, scale, created, data) VALUES (1, 'Carrie Jones', 5.1, '2000-01-01 03:23:34.234', null)");
        stmt.close();
        con.commit();
        con.close();
        }catch(Exception e){e.printStackTrace(); throw e;}
    }
    /**
     * Tests a script that reads content of the flowfile content and stores the value in an attribute of the outgoing flowfile.
     * @throws Exception Any error encountered while testing
     */
    @Test
    public void testReadFlowFileContentAndStoreInFlowFileAttribute() throws Exception {
        runner.setProperty(proc.SCRIPT_BODY, "flowFile.testAttr = flowFile.read().getText('UTF-8'); REL_SUCCESS << flowFile;");
        runner.setProperty(proc.REQUIRE_FLOW, "true");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        
        runner.assertValid();
        runner.enqueue("test content".getBytes("UTF-8"));
        runner.run();

        runner.assertAllFlowFilesTransferred(proc.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(proc.REL_SUCCESS.getName());
        result.get(0).assertAttributeEquals("testAttr", "test content");
    }
    
    @Test
    public void test_onTrigger_groovy() throws Exception {
        runner.setProperty(proc.SCRIPT_FILE, TEST_RESOURCE_LOCATION+"test_onTrigger.groovy");
        runner.setProperty(proc.REQUIRE_FLOW, "false");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();
        
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(proc.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(proc.REL_SUCCESS.getName());
        result.get(0).assertAttributeEquals("from-content", "test content");
    }
    
    @Test
    public void test_onTriggerX_groovy() throws Exception {
        runner.setProperty(proc.SCRIPT_FILE, TEST_RESOURCE_LOCATION+"test_onTriggerX.groovy");
        runner.setProperty(proc.REQUIRE_FLOW, "true");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();
        
        runner.enqueue("test content".getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertAllFlowFilesTransferred(proc.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(proc.REL_SUCCESS.getName());
        result.get(0).assertAttributeEquals("from-content", "test content");
    }
    
    @Test
    public void test_onTrigger_changeContent_groovy() throws Exception {
        runner.setProperty(proc.SCRIPT_FILE, TEST_RESOURCE_LOCATION+"test_onTrigger_changeContent.groovy");
        runner.setProperty(proc.REQUIRE_FLOW, "false");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();
        
        runner.enqueue(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(proc.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(proc.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.get(0);
        resultFile.assertAttributeEquals("selected.columns", "first,last");
        resultFile.assertContentEquals("Marlene Shaw\nTodd Graham\n");
    }
    
    @Test
    public void test_onTrigger_changeContentX_groovy() throws Exception {
        runner.setProperty(proc.SCRIPT_FILE, TEST_RESOURCE_LOCATION+"test_onTrigger_changeContentX.groovy");
        runner.setProperty(proc.REQUIRE_FLOW, "true");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();
        
        runner.enqueue(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(proc.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(proc.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.get(0);
        resultFile.assertAttributeEquals("selected.columns", "first,last");
        resultFile.assertContentEquals("Marlene Shaw\nTodd Graham\n");
    }
    
    @Test
    public void test_no_input_groovy() throws Exception {
        runner.setProperty(proc.SCRIPT_FILE, TEST_RESOURCE_LOCATION+"test_no_input.groovy");
        runner.setProperty(proc.REQUIRE_FLOW, "false");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();
        runner.run();
        runner.assertAllFlowFilesTransferred(proc.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(proc.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.get(0);
        resultFile.assertAttributeEquals("filename", "test.txt");
        resultFile.assertContentEquals("Test");
    }
    
    //@Test
    public void test_sql_in_script_groovy() throws Exception {
        runner.setProperty(proc.SCRIPT_FILE, TEST_RESOURCE_LOCATION+"test_sql_in_script.groovy");
        runner.setProperty(proc.REQUIRE_FLOW, "false");
        //runner.setProperty(proc.FAIL_STRATEGY, "rollback");
        runner.assertValid();
        
        //runner.enqueue(TEST_CSV_DATA.getBytes(StandardCharsets.UTF_8));
        runner.run();

        runner.assertAllFlowFilesTransferred(proc.REL_SUCCESS.getName(), 1);
        final List<MockFlowFile> result = runner.getFlowFilesForRelationship(proc.REL_SUCCESS.getName());
        MockFlowFile resultFile = result.get(0);
        //resultFile.assertAttributeEquals("selected.columns", "first,last");
        resultFile.assertContentEquals("Test");
    }
    
    
    
    private class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                return DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }
}
