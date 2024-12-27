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

package org.apache.cassandra.cql3.statements.schema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.TriggerMetadata;
import org.apache.cassandra.schema.Triggers;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Event.SchemaChange;

import static com.google.common.collect.Iterables.transform;
import static org.apache.cassandra.cql3.statements.schema.CopyTableStatement.CreateLikeOptiion.ALL;
import static org.apache.cassandra.cql3.statements.schema.CopyTableStatement.CreateLikeOptiion.INDEXES;
import static org.apache.cassandra.cql3.statements.schema.CopyTableStatement.CreateLikeOptiion.TRIGGERS;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * {@code CREATE TABLE [IF NOT EXISTS] <newtable> LIKE <oldtable> WITH <property> = <value>}
 */
public final class CopyTableStatement extends AlterSchemaStatement
{
    private final String sourceKeyspace;
    private final String sourceTableName;
    private final String targetKeyspace;
    private final String targetTableName;
    private final boolean ifNotExists;
    private final TableAttributes attrs;
    private final Set<CreateLikeOptiion> optiions;

    public CopyTableStatement(String sourceKeyspace,
                              String targetKeyspace,
                              String sourceTableName,
                              String targetTableName,
                              boolean ifNotExists,
                              TableAttributes attrs,
                              Set<CreateLikeOptiion> optiions)
    {
        super(targetKeyspace);
        this.sourceKeyspace = sourceKeyspace;
        this.targetKeyspace = targetKeyspace;
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.ifNotExists = ifNotExists;
        this.attrs = attrs;
        this.optiions = optiions;
    }

    @Override
    SchemaChange schemaChangeEvent(Keyspaces.KeyspacesDiff diff)
    {
        return new SchemaChange(SchemaChange.Change.CREATED, SchemaChange.Target.TABLE, targetKeyspace, targetTableName);
    }

    @Override
    public void authorize(ClientState client)
    {
        client.ensureTablePermission(sourceKeyspace, sourceTableName, Permission.SELECT);
        client.ensureAllTablesPermission(targetKeyspace, Permission.CREATE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_TABLE_LIKE, targetKeyspace, targetTableName);
    }

    @Override
    public Keyspaces apply(ClusterMetadata metadata)
    {
        Keyspaces schema = metadata.schema.getKeyspaces();
        KeyspaceMetadata sourceKeyspaceMeta = schema.getNullable(sourceKeyspace);

        if (null == sourceKeyspaceMeta)
            throw ire("Source Keyspace '%s' doesn't exist", sourceKeyspace);

        TableMetadata sourceTableMeta = sourceKeyspaceMeta.getTableNullable(sourceTableName);

        if (null == sourceTableMeta)
            throw ire("Souce Table '%s.%s' doesn't exist", sourceKeyspace, sourceTableName);

        if (sourceTableMeta.isIndex())
            throw ire("Cannot use CREATE TABLE LIKE on an index table '%s.%s'.", sourceKeyspace, sourceTableName);

        if (sourceTableMeta.isView())
            throw ire("Cannot use CREATE TABLE LIKE on a materialized view '%s.%s'.", sourceKeyspace, sourceTableName);

        KeyspaceMetadata targetKeyspaceMeta = schema.getNullable(targetKeyspace);
        if (null == targetKeyspaceMeta)
            throw ire("Target Keyspace '%s' doesn't exist", targetKeyspace);

        if (targetKeyspaceMeta.hasTable(targetTableName))
        {
            if(ifNotExists)
                return schema;

            throw new AlreadyExistsException(targetKeyspace, targetTableName);
        }
        // todo support udt for differenet ks latter
        if (!sourceKeyspace.equalsIgnoreCase(targetKeyspace) && !sourceTableMeta.getReferencedUserTypes().isEmpty())
            throw ire("Cannot use CREATE TABLE LIKE across different keyspace when source table have UDTs.");

        // withInternals can be set to false as it is only used for souce table id, which is not need for target table and the table
        // id can be set through create table like cql using WITH ID
        String sourceCQLString = sourceTableMeta.toCqlString(false, false, false, false);

        TableMetadata.Builder targetBuilder = CreateTableStatement.parse(sourceCQLString,
                                                                         targetKeyspace,
                                                                         targetTableName,
                                                                         sourceKeyspaceMeta.types,
                                                                         UserFunctions.none());
        buildIndexesAndTriggers(targetBuilder, sourceTableMeta, targetKeyspace, targetTableName, true);
        TableParams originalParams = targetBuilder.build().params;
        TableParams newTableParams = attrs.asAlteredTableParams(originalParams);

        TableMetadata table = targetBuilder.params(newTableParams)
                                           .id(TableId.get(metadata))
                                           .build();
        table.validate();

        if (targetKeyspaceMeta.replicationStrategy.hasTransientReplicas()
            && table.params.readRepair != ReadRepairStrategy.NONE)
        {
            throw ire("read_repair must be set to 'NONE' for transiently replicated keyspaces");
        }

        if (!table.params.compression.isEnabled())
            Guardrails.uncompressedTablesEnabled.ensureEnabled(state);

        return schema.withAddedOrUpdated(targetKeyspaceMeta.withSwapped(targetKeyspaceMeta.tables.with(table)));
    }

    private void buildIndexesAndTriggers(TableMetadata.Builder builder, TableMetadata metadata, String keyspace, String table, boolean randomName)
    {
        assert builder != null;
        if (optiions.contains(INDEXES) ||
            optiions.contains(ALL))
        {
            Indexes.Builder idxBuilder = Indexes.builder();
            for (IndexMetadata indexMetadata : metadata.indexes)
            {
                String indexCqlString = indexMetadata.toCqlString(metadata, false);
                String index = randomName ? generateRandomName(keyspace, table, indexMetadata.name) : indexMetadata.name;
                idxBuilder.add(parseIndex(metadata, indexCqlString, keyspace, table, index));
            }
            builder.indexes(idxBuilder.build());
        }

        if (optiions.contains(TRIGGERS) ||
            optiions.contains(ALL))
        {
            Triggers.Builder triggersBuilder = Triggers.builder();
            for (TriggerMetadata triggerMetadata : metadata.triggers)
            {
                String indexCqlString = triggerMetadata.toCqlString(metadata, false);
                String trigger = randomName ? generateRandomName(keyspace, table, triggerMetadata.name) : triggerMetadata.name;
                triggersBuilder.add(CreateTriggerStatement.parse(indexCqlString, keyspace, table, trigger));
            }
            builder.triggers(triggersBuilder.build());
        }
    }

    // The code is ugly here, should move to CreateIndexStatement, but we need the source table meta to
    // raw index target's prepare
    private IndexMetadata parseIndex(TableMetadata tableMetadata, String cql, String keyspace, String table, String index)
    {
        CreateIndexStatement.Raw createIndex = CQLFragmentParser.parseAny(CqlParser::createIndexStatement, cql, "CREATE INDEX")
                                                                .keyspace(keyspace);
        if (table != null)
            createIndex.table(table);

        if (index != null)
            createIndex.index(index);

        CreateIndexStatement statement = createIndex.prepare(null);

        List<IndexTarget> indexTargets = Lists.newArrayList(transform(statement.getRawIndexTargets(), t -> t.prepare(tableMetadata)));
        IndexMetadata.Kind kind = statement.getAttrs().isCustom ? IndexMetadata.Kind.CUSTOM : IndexMetadata.Kind.COMPOSITES;

        Map<String, String> options = statement.getAttrs().isCustom ? statement.getAttrs().getOptions() : Collections.emptyMap();

        return IndexMetadata.fromIndexTargets(indexTargets, index, kind, options);
    }

    private String generateRandomName(String keyspace, String table, String name)
    {
        return keyspace + "_" + table + "_" + name + nextTimeUUID().asUUID();
    }

    public final static class Raw extends CQLStatement.Raw
    {
        private final QualifiedName oldName;
        private final QualifiedName newName;
        private final boolean ifNotExists;
        public final TableAttributes attrs = new TableAttributes();
        public final Set<CreateLikeOptiion> optiions = Sets.newHashSet();

        public Raw(QualifiedName newName, QualifiedName oldName, boolean ifNotExists)
        {
            this.newName = newName;
            this.oldName = oldName;
            this.ifNotExists = ifNotExists;
        }

        @Override
        public CQLStatement prepare(ClientState state)
        {
            String oldKeyspace = oldName.hasKeyspace() ? oldName.getKeyspace() : state.getKeyspace();
            String newKeyspace = newName.hasKeyspace() ? newName.getKeyspace() : state.getKeyspace();
            return new CopyTableStatement(oldKeyspace, newKeyspace, oldName.getName(), newName.getName(), ifNotExists, attrs, optiions);
        }

        public void extendWithLikeOptions(String option)
        {
            assert option != null && !option.isEmpty() : "CREATE TABLE LIKE WITH null or empty option value.";
            this.optiions.add(CreateLikeOptiion.valueOf(option));
        }
    }

    public enum CreateLikeOptiion
    {
        ALL("ALL"),
        INDEXES("INDEXES"),
        TRIGGERS("TRIGGERS");

        private final String value;
        CreateLikeOptiion(String value)
        {
            this.value = value;
        }
    }

}
