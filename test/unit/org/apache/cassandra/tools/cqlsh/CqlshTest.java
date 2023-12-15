/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools.cqlsh;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CqlshTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
    }

    @Test
    public void testKeyspaceRequired()
    {
        ToolResult tool = ToolRunner.invokeCqlsh("SELECT * FROM test");
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("No keyspace has been specified"));
        assertEquals(2, tool.getExitCode());
    }

    @Test
    public void testCopyFloatVectorFromFile() throws IOException
    {
        validateCopyVectorLiteralsFromFile("float", 6, new Object[][] {
                                           row(1, vector(0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f)),
                                           row(2, vector(-0.1f, -0.2f, -0.3f, -0.4f, -0.5f, -0.6f)),
                                           row(3, vector(0.9f, 0.8f, 0.7f, 0.6f, 0.5f, 0.4f))
                                           });

        validateCopyVectorLiteralsFromFile("float", 3, new Object[][] {
                                           row(1, vector(0.1f, 0.2f, 0.3f)),
                                           row(2, vector(-0.4f, -0.5f, -0.6f)),
                                           row(3, vector(0.7f, 0.8f, 0.9f))
                                           });
    }

    @Test
    public void testCopyIntVectorFromFile() throws IOException
    {
        validateCopyVectorLiteralsFromFile("int", 6, new Object[][] {
                                           row(1, vector(1, 2, 3, 4, 5, 6)),
                                           row(2, vector(-1, -2, -3, -4, -5, -6)),
                                           row(3, vector(9, 8, 7, 6, 5, 4))
                                           });

        validateCopyVectorLiteralsFromFile("int", 3, new Object[][] {
                                           row(1, vector(1, 2, 3)),
                                           row(2, vector(-4, -5, -6)),
                                           row(3, vector(7, 8, 9))
                                           });
    }

    @Test
    public void testCopyOnlyThoseRowsThatMatchVectorTypeSize() throws IOException
    {
        // given a table with a vector column and a file containing vector literals
        createTable(KEYSPACE, "CREATE TABLE %s (id int PRIMARY KEY, embedding_vector vector<int, 6>)");
        assertTrue("table should be initially empty", execute("SELECT * FROM %s").isEmpty());

        Object[][] rows = {
            row(1, vector(1, 2, 3, 4, 5, 6)),
            row(2, vector(1, 2, 3, 4, 5)),
            row(3, vector(1, 2, 3, 4, 6, 7))
        };

        Path csv = prepareCSVFile(rows);

        // when running COPY via cqlsh
        ToolRunner.ToolResult result = ToolRunner.invokeCqlsh(format("COPY %s.%s FROM '%s'", KEYSPACE, currentTable(), csv.toAbsolutePath()));
        UntypedResultSet importedRows = execute("SELECT * FROM %s");

        // then only rows that match type size should be imported
        result.asserts().failure();
        result.asserts().errorContains("The length of given vector value '5' is not equal to the vector size from the type definition '6'");
        assertRowsIgnoringOrder(importedRows, row(1, vector(1, 2, 3, 4, 5, 6)),
                                row(3, vector(1, 2, 3, 4, 6, 7)));
    }

    @Test
    public void testCopyIntVectorToFile() throws IOException
    {
        validateCopyVectorLiteralsToFile("int", 6, new Object[][] {
                                         row(1, vector(1, 2, 3, 4, 5, 6)),
                                         row(2, vector(-1, -2, -3, -4, -5, -6)),
                                         row(3, vector(9, 8, 7, 6, 5, 4))
                                         });
        validateCopyVectorLiteralsToFile("int", 3, new Object[][] {
                                         row(1, vector(1, 2, 3)),
                                         row(2, vector(-4, -5, -6)),
                                         row(3, vector(7, 8, 9))
                                         });
    }

    private void validateCopyVectorLiteralsFromFile(String vectorType, int vectorSize, Object[][] rows) throws IOException
    {
        // given a table with a vector column and a file containing vector literals
        createTable(KEYSPACE, format("CREATE TABLE %%s (id int PRIMARY KEY, embedding_vector vector<%s, %d>)", vectorType, vectorSize));
        assertTrue("table should be initially empty", execute("SELECT * FROM %s").isEmpty());

        Path csv = prepareCSVFile(rows);

        // when running COPY via cqlsh
        ToolRunner.ToolResult result = ToolRunner.invokeCqlsh(format("COPY %s.%s FROM '%s'", KEYSPACE, currentTable(), csv.toAbsolutePath()));
        result.asserts().success();

        UntypedResultSet importedRows = execute("SELECT * FROM %s");
        assertRowsIgnoringOrder(importedRows, rows);
    }

    private void validateCopyVectorLiteralsToFile(String vectorType, int vectorSize, Object[][] rows) throws IOException
    {
        // given a table with a vector column and a file containing vector literals
        createTable(KEYSPACE, format("CREATE TABLE %%s (id int PRIMARY KEY, embedding_vector vector<%s, %d>)", vectorType, vectorSize));
        assertTrue("table should be initially empty", execute("SELECT * FROM %s").isEmpty());

        // write the rows into the table
        for (Object[] row : rows)
            execute("INSERT INTO %s (id, embedding_vector) VALUES (?, ?)", row);
        // export the rows to CSV
        Path csv = Files.createTempFile("test_copy_to_vector", ".csv");
        csv.toFile().deleteOnExit();
        ToolRunner.ToolResult result = ToolRunner.invokeCqlsh(format("COPY %s.%s TO '%s'", KEYSPACE, currentTable(), csv.toAbsolutePath()));
        result.asserts().success();
        // verify that the exported CSV contains the expected rows
        Assertions.assertThat(csv).hasSameTextualContentAs(prepareCSVFile(rows));
        // truncate the table
        execute("TRUNCATE %s");
        assertTrue("table should be empty", execute("SELECT * FROM %s").isEmpty());
    }

    private static Path prepareCSVFile(Object[][] rows) throws IOException
    {
        Path csv = Files.createTempFile("test_copy_vector", ".csv");
        csv.toFile().deleteOnExit();

        try (Writer out = Files.newBufferedWriter(csv, StandardCharsets.UTF_8))
        {
            for (Object[] row : rows)
            {
                out.write(String.format("%s,\"%s\"\n", row[0], row[1]));
            }
        }

        return csv;
    }
}
