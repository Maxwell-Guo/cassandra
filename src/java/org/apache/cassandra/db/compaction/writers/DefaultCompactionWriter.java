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
package org.apache.cassandra.db.compaction.writers;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * The default compaction writer - creates one output file in L0
 */
public class DefaultCompactionWriter extends CompactionAwareWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(DefaultCompactionWriter.class);
    private final int sstableLevel;

    public DefaultCompactionWriter(ColumnFamilyStore cfs, Directories directories, ILifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables)
    {
        this(cfs, directories, txn, nonExpiredSSTables, false, 0);
    }

    public DefaultCompactionWriter(ColumnFamilyStore cfs, Directories directories, ILifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables, boolean keepOriginals, int sstableLevel)
    {
        super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);
        this.sstableLevel = sstableLevel;
    }

    @Override
    protected boolean shouldSwitchWriterInCurrentLocation(DecoratedKey key)
    {
        return false;
    }

    protected int sstableLevel()
    {
        return sstableLevel;
    }

    protected long sstableKeyCount()
    {
        return estimatedTotalKeys;
    }
}
