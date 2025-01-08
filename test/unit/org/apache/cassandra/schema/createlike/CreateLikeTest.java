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

package org.apache.cassandra.schema.createlike;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.validation.operations.CreateTest;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CreateLikeTest extends CQLTester
{
    @Parameterized.Parameter
    public boolean differentKs;

    @Parameterized.Parameters(name = "{index}: differentKs={0}")
    public static Collection<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        result.add(new Object[]{false});
        result.add(new Object[]{true});
        return result;
    }

    private UUID uuid1 = UUID.fromString("62c3e96f-55cd-493b-8c8e-5a18883a1698");
    private UUID uuid2 = UUID.fromString("52c3e96f-55cd-493b-8c8e-5a18883a1698");
    private TimeUUID timeUuid1 = TimeUUID.fromString("00346642-2d2f-11ed-a261-0242ac120002");
    private TimeUUID timeUuid2 = TimeUUID.fromString("10346642-2d2f-11ed-a261-0242ac120002");
    private Duration duration1 = Duration.newInstance(1, 2, 3);
    private Duration duration2 = Duration.newInstance(1, 2, 4);
    private Date date1 = new Date();
    private Double d1 = Double.valueOf("1.1");
    private Double d2 = Double.valueOf("2.2");
    private Float f1 = Float.valueOf("3.33");
    private Float f2 = Float.valueOf("4.44");
    private BigDecimal decimal1 =  BigDecimal.valueOf(1.1);
    private BigDecimal decimal2 =  BigDecimal.valueOf(2.2);
    private Vector<Integer> vector1  = vector(1, 2);
    private Vector<Integer> vector2 = vector(3, 4);
    private String keyspace1 = "keyspace1";
    private String keyspace2 = "keyspace2";
    private String sourceKs;
    private String targetKs;

    @Before
    public void before()
    {
        createKeyspace("CREATE KEYSPACE " + keyspace1 + " WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        createKeyspace("CREATE KEYSPACE " + keyspace2 + " WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        sourceKs = keyspace1;
        targetKs = differentKs ? keyspace2 : keyspace1;
    }

    @Test
    public void testTableSchemaCopy()
    {
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c text);");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c) VALUES (?, ?, ?)", 1, duration1, "1");
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, b, c) VALUES (?, ?, ?)", 2, duration2, "2");
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1, duration1, "1"));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2, duration2, "2"));

        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY);");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a) VALUES (1)");
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a) VALUES (2)");
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2));

        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a frozen<map<text, text>> PRIMARY KEY);");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a) VALUES (?)", map("k", "v"));
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a) VALUES (?)", map("nk", "nv"));
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(map("k", "v")));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(map("nk", "nv")));

        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b set<frozen<list<text>>>, c map<text, int>, d smallint, e duration, f tinyint);");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)",
                1, set(list("1", "2"), list("3", "4")), map("k", 1), (short)2, duration1, (byte)4);
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)",
                2, set(list("5", "6"), list("7", "8")), map("nk", 2), (short)3, duration2, (byte)5);
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1, set(list("1", "2"), list("3", "4")), map("k", 1), (short)2, duration1, (byte)4));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2, set(list("5", "6"), list("7", "8")), map("nk", 2), (short)3, duration2, (byte)5));

        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int , b double, c tinyint, d float, e list<text>, f map<text, int>, g duration, PRIMARY KEY((a, b, c), d));");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c, d, e, f, g) VALUES (?, ?, ?, ?, ?, ?, ?) ",
                1, d1, (byte)4, f1, list("a", "b"), map("k", 1), duration1);
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, d, e, f, g) VALUES (?, ?, ?, ?, ?, ?, ?) ",
                2, d2, (byte)5, f2, list("c", "d"), map("nk", 2), duration2);
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1, d1, (byte)4, f1, list("a", "b"), map("k", 1), duration1));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2, d2, (byte)5, f2, list("c", "d"), map("nk", 2), duration2));

        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int , " +
                                                                "b text, " +
                                                                "c bigint, " +
                                                                "d decimal, " +
                                                                "e set<text>, " +
                                                                "f uuid, " +
                                                                "g vector<int, 2>, " +
                                                                "h list<float>, " +
                                                                "i timeuuid, " +
                                                                "j map<text, frozen<set<int>>>, " +
                                                                "PRIMARY KEY((a, b), c, d)) " +
                                                                "WITH CLUSTERING ORDER BY (c DESC, d ASC);");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c, d, e, f, g, h, i, j)  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                1, "b", 100L, decimal1, set("1", "2"), uuid1, vector1, list(1.1F, 2.2F), timeUuid1, map("k", set(1, 2)));
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, d, e, f, g, h, i, j) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                2, "nb", 200L, decimal2, set("3", "4"), uuid2, vector2, list(3.3F, 4.4F), timeUuid2, map("nk", set(3, 4)));
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1, "b", 100L, decimal1, set("1", "2"), uuid1, vector1, list(1.1F, 2.2F), timeUuid1, map("k", set(1, 2))));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2, "nb", 200L, decimal2, set("3", "4"), uuid2, vector2, list(3.3F, 4.4F), timeUuid2, map("nk", set(3, 4))));

        // test that can create a copy of a copied table
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c text);");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        createTableLike("CREATE TABLE %s LIKE %s", targetTb, targetKs, "newtargettb", sourceKs);
    }

    @Test
    public void testIfNotExists() throws Throwable
    {
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int, b text, c duration, d float, PRIMARY KEY(a, b));");
        String targetTb = createTableLike("CREATE TABLE IF NOT EXISTS %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        createTableLike("CREATE TABLE IF NOT EXISTS %s LIKE %s", sourceTb, sourceKs, targetTb, targetKs);
        assertInvalidThrowMessage("Cannot add already existing table \"" + targetTb + "\" to keyspace \"" + targetKs + "\"", AlreadyExistsException.class,
                                  "CREATE TABLE " + targetKs + "." + targetTb + " LIKE " + sourceKs + "." + sourceTb);
    }

    @Test
    public void testCopyAfterAlterTable()
    {
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int, b text, c duration, d float, PRIMARY KEY(a, b));");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        alterTable("ALTER TABLE  " + sourceKs + " ." + sourceTb + " DROP d");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        alterTable("ALTER TABLE " + sourceKs + " ." + sourceTb + " ADD e uuid");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        alterTable("ALTER TABLE " + sourceKs + " ." + sourceTb + " ADD f float");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a, b, c, e, f) VALUES (?, ?, ?, ?, ?)", 1, "1", duration1, uuid1, f1);
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, e, f) VALUES (?, ?, ?, ?, ?)", 2, "2", duration2, uuid2, f2);
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1, "1", duration1, uuid1, f1));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2, "2", duration2, uuid2, f2));

        alterTable("ALTER TABLE " + sourceKs + " ." + sourceTb + " DROP f USING TIMESTAMP 20000");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        alterTable("ALTER TABLE " + sourceKs + " ." + sourceTb + " RENAME b TO bb ");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        alterTable("ALTER TABLE " + sourceKs + " ." + sourceTb + " WITH compaction = {'class':'LeveledCompactionStrategy', 'sstable_size_in_mb':10, 'fanout_size':16} ");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        execute("INSERT INTO " + sourceKs + "." + sourceTb + " (a, bb, c, e) VALUES (?, ?, ?, ?)", 1, "1", duration1, uuid1);
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, bb, c, e) VALUES (?, ?, ?, ?)", 2, "2", duration2, uuid2);
        assertRows(execute("SELECT * FROM " + sourceKs + "." + sourceTb),
                   row(1, "1", duration1, uuid1));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb),
                   row(2, "2", duration2, uuid2));

    }

    @Test
    public void testTableOptionsCopy() throws Throwable
    {
        // compression
        String tbCompressionDefault1 = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))");
        String tbCompressionDefault2 =createTable(sourceKs,"CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                           " WITH compression = { 'enabled' : 'false'};");
        String tbCompressionSnappy1 = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                           " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 };");
        String tbCompressionSnappy2 =createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                           " WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32, 'enabled' : true };");
        String tbCompressionSnappy3 = createTable(sourceKs,"CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                           " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : 2 };");
        String tbCompressionSnappy4 = createTable(sourceKs,"CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                           " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : 1 };");
        String tbCompressionSnappy5 = createTable(sourceKs,"CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                           " WITH compression = { 'class' : 'SnappyCompressor', 'min_compress_ratio' : 0 };");

        // memtable
        String tableMemtableSkipList = createTable(sourceKs,"CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                            " WITH memtable = 'skiplist';");
        String tableMemtableTrie = createTable(sourceKs,"CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                        " WITH memtable = 'trie';");
        String tableMemtableDefault = createTable(sourceKs,"CREATE TABLE %s (a text, b int, c int, primary key (a, b))" +
                                                           " WITH memtable = 'default';");

        // compaction
        String tableCompactionStcs = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':false};");
        String tableCompactionLcs =createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH compaction = {'class':'LeveledCompactionStrategy', 'sstable_size_in_mb':1, 'fanout_size':5};");
        String tableCompactionTwcs = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH compaction = {'class':'TimeWindowCompactionStrategy', 'min_threshold':2};");
        String tableCompactionUcs =createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH compaction = {'class':'UnifiedCompactionStrategy'};");

        // other options are all different from default
        String tableOtherOptions = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH" +
                                                        " additional_write_policy = '95p' " +
                                                        " AND bloom_filter_fp_chance = 0.1 " +
                                                        " AND caching = {'keys': 'ALL', 'rows_per_partition': '100'}" +
                                                        " AND cdc = true " +
                                                        " AND comment = 'test for create like'" +
                                                        " AND crc_check_chance = 0.1" +
                                                        " AND default_time_to_live = 10" +
                                                        " AND compaction = {'class':'UnifiedCompactionStrategy'} " +
                                                        " AND compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 32 }" +
                                                        " AND gc_grace_seconds = 100" +
                                                        " AND incremental_backups = false" +
                                                        " AND max_index_interval = 1024" +
                                                        " AND min_index_interval = 64" +
                                                        " AND speculative_retry = '95p'" +
                                                        " AND read_repair = 'NONE'" +
                                                        " AND memtable_flush_period_in_ms = 360000" +
                                                        " AND memtable = 'default';" );

        String tbLikeCompressionDefault1 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionDefault1, sourceKs, targetKs);
        String tbLikeCompressionDefault2 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionDefault2, sourceKs, targetKs);
        String tbLikeCompressionSp1 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionSnappy1, sourceKs, targetKs);
        String tbLikeCompressionSp2 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionSnappy2, sourceKs, targetKs);
        String tbLikeCompressionSp3 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionSnappy3, sourceKs, targetKs);
        String tbLikeCompressionSp4 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionSnappy4, sourceKs, targetKs);
        String tbLikeCompressionSp5 = createTableLike("CREATE TABLE %s LIKE %s", tbCompressionSnappy5, sourceKs, targetKs);
        String tbLikeMemtableSkipList = createTableLike("CREATE TABLE %s LIKE %s", tableMemtableSkipList, sourceKs, targetKs);
        String tbLikeMemtableTrie = createTableLike("CREATE TABLE %s LIKE %s", tableMemtableTrie, sourceKs, targetKs);
        String tbLikeMemtableDefault = createTableLike("CREATE TABLE %s LIKE %s", tableMemtableDefault, sourceKs, targetKs);
        String tbLikeCompactionStcs = createTableLike("CREATE TABLE %s LIKE %s", tableCompactionStcs, sourceKs, targetKs);
        String tbLikeCompactionLcs = createTableLike("CREATE TABLE %s LIKE %s", tableCompactionLcs, sourceKs, targetKs);
        String tbLikeCompactionTwcs = createTableLike("CREATE TABLE %s LIKE %s", tableCompactionTwcs, sourceKs, targetKs);
        String tbLikeCompactionUcs = createTableLike("CREATE TABLE %s LIKE %s", tableCompactionUcs, sourceKs, targetKs);
        String tbLikeCompactionOthers= createTableLike("CREATE TABLE %s LIKE %s", tableOtherOptions, sourceKs, targetKs);

        assertTableMetaEquals(sourceKs, targetKs, tbCompressionDefault1, tbLikeCompressionDefault1);
        assertTableMetaEquals(sourceKs, targetKs, tbCompressionDefault2, tbLikeCompressionDefault2);
        assertTableMetaEquals(sourceKs, targetKs, tbCompressionSnappy1, tbLikeCompressionSp1);
        assertTableMetaEquals(sourceKs, targetKs, tbCompressionSnappy2, tbLikeCompressionSp2);
        assertTableMetaEquals(sourceKs, targetKs, tbCompressionSnappy3, tbLikeCompressionSp3);
        assertTableMetaEquals(sourceKs, targetKs, tbCompressionSnappy4, tbLikeCompressionSp4);
        assertTableMetaEquals(sourceKs, targetKs, tbCompressionSnappy5, tbLikeCompressionSp5);
        assertTableMetaEquals(sourceKs, targetKs, tableMemtableSkipList, tbLikeMemtableSkipList);
        assertTableMetaEquals(sourceKs, targetKs, tableMemtableTrie, tbLikeMemtableTrie);
        assertTableMetaEquals(sourceKs, targetKs, tableMemtableDefault, tbLikeMemtableDefault);
        assertTableMetaEquals(sourceKs, targetKs, tableCompactionStcs, tbLikeCompactionStcs);
        assertTableMetaEquals(sourceKs, targetKs, tableCompactionLcs, tbLikeCompactionLcs);
        assertTableMetaEquals(sourceKs, targetKs, tableCompactionTwcs, tbLikeCompactionTwcs);
        assertTableMetaEquals(sourceKs, targetKs, tableCompactionUcs, tbLikeCompactionUcs);
        assertTableMetaEquals(sourceKs, targetKs, tableOtherOptions, tbLikeCompactionOthers);

        // a copy of the table with the table parameters set
        String tableCopyAndSetCompression = createTableLike("CREATE TABLE %s LIKE %s WITH compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 64 };",
                                                            tbCompressionSnappy1, sourceKs, targetKs);
        String tableCopyAndSetLCSCompaction = createTableLike("CREATE TABLE %s LIKE %s WITH compaction = {'class':'LeveledCompactionStrategy', 'sstable_size_in_mb':10, 'fanout_size':16};",
                                                           tableCompactionLcs, sourceKs, targetKs);
        String tableCopyAndSetAllParams = createTableLike("CREATE TABLE %s (a text, b int, c int, primary key (a, b)) WITH" +
                                                           " bloom_filter_fp_chance = 0.75 " +
                                                           " AND caching = {'keys': 'NONE', 'rows_per_partition': '10'}" +
                                                           " AND cdc = true " +
                                                           " AND comment = 'test for create like and set params'" +
                                                           " AND crc_check_chance = 0.8" +
                                                           " AND default_time_to_live = 100" +
                                                           " AND compaction = {'class':'SizeTieredCompactionStrategy'} " +
                                                           " AND compression = { 'class' : 'SnappyCompressor', 'chunk_length_in_kb' : 64 }" +
                                                           " AND gc_grace_seconds = 1000" +
                                                           " AND incremental_backups = true" +
                                                           " AND max_index_interval = 128" +
                                                           " AND min_index_interval = 16" +
                                                           " AND speculative_retry = '96p'" +
                                                           " AND read_repair = 'NONE'" +
                                                           " AND memtable_flush_period_in_ms = 3600;",
                                                           tableOtherOptions, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, tbCompressionDefault1, tableCopyAndSetCompression, false, false, false);
        assertTableMetaEquals(sourceKs, targetKs, tableCompactionLcs, tableCopyAndSetLCSCompaction, false, false, false);
        assertTableMetaEquals(sourceKs, targetKs, tableOtherOptions, tableCopyAndSetAllParams, false, false, false);
        TableParams paramsSetCompression = getTableMetadata(targetKs, tableCopyAndSetCompression).params;
        TableParams paramsSetLCSCompaction = getTableMetadata(targetKs, tableCopyAndSetLCSCompaction).params;
        TableParams paramsSetAllParams = getTableMetadata(targetKs, tableCopyAndSetAllParams).params;

        assertEquals(paramsSetCompression, TableParams.builder().compression(CompressionParams.snappy(64 * 1024, 0.0)).build());
        assertEquals(paramsSetLCSCompaction, TableParams.builder().compaction(CompactionParams.create(LeveledCompactionStrategy.class,
                                                                                                      Map.of("sstable_size_in_mb", "10",
                                                                                                             "fanout_size", "16")))
                                                                  .build());
        assertEquals(paramsSetAllParams, TableParams.builder().bloomFilterFpChance(0.75)
                                                              .caching(new CachingParams(false, 10))
                                                              .cdc(true)
                                                              .comment("test for create like and set params")
                                                              .crcCheckChance(0.8)
                                                              .defaultTimeToLive(100)
                                                              .compaction(CompactionParams.stcs(Collections.emptyMap()))
                                                              .compression(CompressionParams.snappy(64 * 1024, 0.0))
                                                              .gcGraceSeconds(1000)
                                                              .incrementalBackups(true)
                                                              .maxIndexInterval(128)
                                                              .minIndexInterval(16)
                                                              .speculativeRetry(SpeculativeRetryPolicy.fromString("96PERCENTILE"))
                                                              .readRepair(ReadRepairStrategy.NONE)
                                                              .memtableFlushPeriodInMs(3600)
                                                              .build());

        // table id
        TableId id = TableId.generate();
        String tbNormal = createTable(sourceKs, "CREATE TABLE %s (a text, b int, c int, primary key (a, b))");
        assertInvalidThrowMessage("Cannot alter table id.", ConfigurationException.class,
                                  "CREATE TABLE " + targetKs + ".targetnormal LIKE " +  sourceKs + "." + tbNormal + " WITH ID = " + id);
    }

    @Test
    public void testStaticColumnCopy()
    {
        // create with static column
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int , b int , c int static, d int, e list<text>, PRIMARY KEY(a, b));", "tb1");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);
        execute("INSERT INTO " + targetKs + "." + targetTb + " (a, b, c, d, e) VALUES (0, 1, 2, 3, ?)", list("1", "2", "3", "4"));
        assertRows(execute("SELECT * FROM " + targetKs + "." + targetTb), row(0, 1, 2, 3, list("1", "2", "3", "4")));

        // add static column
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int, b int, c text, PRIMARY KEY (a, b))");
        alterTable("ALTER TABLE " + sourceKs + "." + sourceTb + " ADD d  int static");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);
    }

    @Test
    public void testColumnMaskTableCopy()
    {
        DatabaseDescriptor.setDynamicDataMaskingEnabled(true);
        // masked partition key
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int MASKED WITH mask_default() PRIMARY KEY, r int)");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        // masked partition key component
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k1 int, k2 text MASKED WITH DEFAULT, r int, PRIMARY KEY(k1, k2))");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        // masked clustering key
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int, c int MASKED WITH mask_default(), r int, PRIMARY KEY (k, c))");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        // masked clustering key with reverse order
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int, c text MASKED WITH mask_default(), r int, PRIMARY KEY (k, c)) " +
                                         "WITH CLUSTERING ORDER BY (c DESC)");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        // masked clustering key component
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int, c1 int, c2 text MASKED WITH DEFAULT, r int, PRIMARY KEY (k, c1, c2))");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        // masked regular column
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int PRIMARY KEY, r1 text MASKED WITH DEFAULT, r2 int)");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        // masked static column
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int, c int, r int, s int STATIC MASKED WITH DEFAULT, PRIMARY KEY (k, c))");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        // multiple masked columns
        sourceTb = createTable(sourceKs, "CREATE TABLE %s (" +
                                         "k1 int, k2 int MASKED WITH DEFAULT, " +
                                         "c1 int, c2 text MASKED WITH DEFAULT, " +
                                         "r1 int, r2 int MASKED WITH DEFAULT, " +
                                         "s1 int static, s2 int static MASKED WITH DEFAULT, " +
                                         "PRIMARY KEY((k1, k2), c1, c2))");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);

        sourceTb = createTable(sourceKs, "CREATE TABLE %s (k int PRIMARY KEY, " +
                                         "s set<int> MASKED WITH DEFAULT, " +
                                         "l list<int> MASKED WITH DEFAULT, " +
                                         "m map<int, int> MASKED WITH DEFAULT, " +
                                         "fs frozen<set<int>> MASKED WITH DEFAULT, " +
                                         "fl frozen<list<int>> MASKED WITH DEFAULT, " +
                                         "fm frozen<map<int, int>> MASKED WITH DEFAULT)");
        targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb);
    }

    @Test
    public void testUDTTableCopy() throws Throwable
    {
        // normal udt
        String udt = createType(sourceKs, "CREATE TYPE %s (a int, b uuid, c text)");
        // collection udt
        String udtSet = createType(sourceKs, "CREATE TYPE %s (a int, c frozen <set<text>>)");
        // frozen udt
        String udtFrozen = createType(sourceKs, "CREATE TYPE %s (a int, c frozen<" + udt + ">)");

        String sourceTbUdt = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c " + udt + ");");
        String sourceTbUdtSet = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c " + udtSet + ");");
        String sourceTbUdtFrozen = createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b duration, c " + udtFrozen + ");");

        if (differentKs)
        {
            assertInvalidThrowMessage("Cannot use CREATE TABLE LIKE across different keyspace when source table have UDT",
                                      InvalidRequestException.class, "CREATE TABLE " + targetKs + ".tbudt LIKE " + sourceKs + "." + sourceTbUdt);
            assertInvalidThrowMessage("Cannot use CREATE TABLE LIKE across different keyspace when source table have UDT",
                                      InvalidRequestException.class, "CREATE TABLE " + targetKs + ".tbdtset LIKE " + sourceKs + "." + sourceTbUdt);
            assertInvalidThrowMessage("Cannot use CREATE TABLE LIKE across different keyspace when source table have UDT", InvalidRequestException.class,
                                      "CREATE TABLE " + targetKs + ".tbudtfrozen LIKE " + sourceKs + "." + sourceTbUdt);
        }
        else
        {
            String targetTbUdt = createTableLike("CREATE TABLE %s LIKE %s", sourceTbUdt, sourceKs, "tbudt", targetKs);
            String targetTbUdtSet = createTableLike("CREATE TABLE %s LIKE %s", sourceTbUdtSet, sourceKs, "tbdtset", targetKs);
            String targetTbUdtFrozen = createTableLike("CREATE TABLE %s LIKE %s", sourceTbUdtFrozen, sourceKs, "tbudtfrozen", targetKs);
            assertTableMetaEquals(sourceKs, targetKs, sourceTbUdt, targetTbUdt);
            assertTableMetaEquals(sourceKs, targetKs, sourceTbUdtSet, targetTbUdtSet);
            assertTableMetaEquals(sourceKs, targetKs, sourceTbUdtFrozen, targetTbUdtFrozen);
        }
    }

    @Test
    public void testIndexOperationOnCopiedTable()
    {
        // copied table can do index creation
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (id text PRIMARY KEY, val text, num int);");
        String targetTb = createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        String saiIndex = createIndex(targetKs, "CREATE INDEX ON %s(val) USING 'sai'");
        execute("INSERT INTO " + targetKs + "." + targetTb + " (id, val, num) VALUES ('1', 'value', 1)");
        assertEquals(1, execute("SELECT id FROM " + targetKs + "." + targetTb + " WHERE val = 'value'").size());
        String normalIndex = createIndex(targetKs, "CREATE INDEX ON %s(num)");
        Indexes targetIndexes = getTableMetadata(targetKs, targetTb).indexes;
        assertEquals(targetIndexes.size(), 2);
        assertNotNull(targetIndexes.get(saiIndex));
        assertNotNull(targetIndexes.get(normalIndex));
    }

    @Test
    public void testTriggerOperationOnCopiedTable()
    {
        String triggerName = "trigger_1";
        String sourceTb = createTable(sourceKs, "CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a))");
        String targetTb =  createTableLike("CREATE TABLE %s LIKE %s", sourceTb, sourceKs, targetKs);
        execute("CREATE TRIGGER " + triggerName + " ON " + targetKs + "." + targetTb + " USING '" + CreateTest.TestTrigger.class.getName() + "'");
        assertNotNull(getTableMetadata(targetKs, targetTb).triggers.get(triggerName));
    }

    @Test
    public void testUnSupportedSchema() throws Throwable
    {
        createTable(sourceKs, "CREATE TABLE %s (a int PRIMARY KEY, b int, c text)", "tb");
        String index = createIndex( "CREATE INDEX ON " + sourceKs + ".tb (c)");
        assertInvalidThrowMessage("Souce Table '" + targetKs + "." + index + "' doesn't exist", InvalidRequestException.class,
                            "CREATE TABLE " + sourceKs + ".newtb LIKE  " + targetKs + "." + index + ";");

        assertInvalidThrowMessage("System keyspace 'system' is not user-modifiable", InvalidRequestException.class,
                                  "CREATE TABLE system.local_clone LIKE system.local ;");
        assertInvalidThrowMessage("System keyspace 'system_views' is not user-modifiable", InvalidRequestException.class,
                                  "CREATE TABLE system_views.newtb LIKE system_views.snapshots ;");
    }

    private void assertTableMetaEquals(String sourceKs, String targetKs, String sourceTb, String targetTb)
    {
        assertTableMetaEquals(sourceKs, targetKs, sourceTb, targetTb, true, true, true);
    }

    private void assertTableMetaEquals(String sourceKs, String targetKs, String sourceTb, String targetTb, boolean compareParams, boolean compareIndexes, boolean compareTrigger)
    {
        TableMetadata left = getTableMetadata(sourceKs, sourceTb);
        TableMetadata right = getTableMetadata(targetKs, targetTb);
        assertNotNull(left);
        assertNotNull(right);
        assertTrue(equalsWithoutTableNameAndDropCns(left, right, compareParams, compareIndexes, compareTrigger));
        assertNotEquals(left.id, right.id);
        assertNotEquals(left.name, right.name);
    }
}
