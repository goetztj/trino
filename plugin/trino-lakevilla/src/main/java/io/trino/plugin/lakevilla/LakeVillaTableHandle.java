/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.lakevilla;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import static java.util.Objects.requireNonNull;

/**
 * A LakeVilla table.
 *
 * @param path location of the table in the bucket (with a trailing slash)
 * @param keyConstraint enforced constraint on the key column (the first column): a set of keys
 * the scan returns and a DELETE removes with LakeVilla's key-based operations; all() for no
 * constraint
 */
public record LakeVillaTableHandle(String schemaName, String tableName, String path, TupleDomain<ColumnHandle> keyConstraint)
        implements ConnectorTableHandle
{
    public LakeVillaTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(path, "path is null");
        requireNonNull(keyConstraint, "keyConstraint is null");
    }

    public LakeVillaTableHandle(String schemaName, String tableName, String path)
    {
        this(schemaName, tableName, path, TupleDomain.all());
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    public LakeVillaTableHandle withKeyConstraint(TupleDomain<ColumnHandle> keyConstraint)
    {
        return new LakeVillaTableHandle(schemaName, tableName, path, keyConstraint);
    }

    @Override
    public String toString()
    {
        return schemaName + "." + tableName + (keyConstraint.isAll() ? "" : " " + keyConstraint);
    }
}
