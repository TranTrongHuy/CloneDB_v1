using Microsoft.Data.SqlClient;
using System.Data;

namespace CloneDB
{
    public enum TableCloneMode
    {
        SchemaOnly,
        DataOnly,
        SchemaAndData
    }

    public class DbObject
    {
        public string Name { get; set; } = "";
        public string ObjectType { get; set; } = "TABLE"; // TABLE, VIEW, PROCEDURE, FUNCTION
    }

    public class TableCloneProgress
    {
        public string TableName { get; set; } = "";
        public double Percentage { get; set; }
        public string Status { get; set; } = "Pending"; // Pending, Preparing, Waiting, Cloning, Success, Error, Skipped
        public string Message { get; set; } = "";
        public long RowsCopied { get; set; }
        public long TotalRows { get; set; }
        public string ItemType { get; set; } = "TABLE"; // TABLE, VIEW, PROCEDURE, FUNCTION
        public TableCloneMode CloneMode { get; set; } = TableCloneMode.SchemaAndData;
    }

    public class CloneService
    {
        private static readonly HashSet<string> _systemTableExclusions = new(StringComparer.OrdinalIgnoreCase)
        {
            "sysdiagrams", "__MigrationHistory", "__EFMigrationsHistory"
        };

        /// <summary>Mô tả 1 khóa ngoại (hỗ trợ nhiều cột) để dựng lại sau khi drop/create table.</summary>
        private sealed class ForeignKeyDef
        {
            public string Name { get; set; } = "";
            public string ParentTable { get; set; } = "";
            public string RefTable { get; set; } = "";
            public List<string> Columns { get; } = new();
            public List<string> RefColumns { get; } = new();
            public string OnDelete { get; set; } = "NO_ACTION";
            public string OnUpdate { get; set; } = "NO_ACTION";
        }

        /// <summary>Test kết nối tới 1 server (mở connection + query nhẹ), throw nếu không connect được.</summary>
        public async Task TestConnectionAsync(string connStr)
        {
            using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand("select 1", conn);
            await cmd.ExecuteScalarAsync();
        }

        /// <summary>Lấy toàn bộ Tables từ server source.</summary>
        public async Task<List<string>> GetTablesAsync(string serverStr)
        {
            var tables = new List<string>();
            using var conn = new SqlConnection(serverStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(
                "SELECT name FROM sys.tables WHERE is_ms_shipped = 0 ORDER BY name", conn);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var name = reader.GetString(0);
                if (!_systemTableExclusions.Contains(name))
                    tables.Add(name);
            }
            return tables;
        }

        /// <summary>Lấy row count (approximate từ sys.partitions) cho toàn bộ tables trong 1 query.</summary>
        public async Task<Dictionary<string, long>> GetTableRowCountsAsync(string serverStr)
        {
            var result = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            using var conn = new SqlConnection(serverStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(@"
                SELECT t.name, ISNULL(SUM(p.rows), 0)
                FROM sys.tables t
                LEFT JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
                WHERE t.is_ms_shipped = 0
                GROUP BY t.name", conn);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                result[reader.GetString(0)] = reader.GetInt64(1);
            return result;
        }

        /// <summary>Lấy Views, Procedures, Functions từ server source.</summary>
        public async Task<List<DbObject>> GetObjectsAsync(string serverStr)
        {
            var result = new List<DbObject>();
            using var conn = new SqlConnection(serverStr);
            await conn.OpenAsync();

            const string sql = @"
                SELECT o.name, RTRIM(o.type) AS type
                FROM sys.sql_modules m
                JOIN sys.objects o ON o.object_id = m.object_id
                WHERE o.type IN ('V', 'P', 'FN', 'IF', 'TF')
                  AND o.is_ms_shipped = 0
                ORDER BY CASE o.type
                    WHEN 'V'  THEN 1
                    WHEN 'FN' THEN 2 WHEN 'IF' THEN 2 WHEN 'TF' THEN 2
                    ELSE 3 END, o.name";

            using var cmd = new SqlCommand(sql, conn);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                string rawType = reader.GetString(1).Trim();
                string objectType = rawType switch
                {
                    "V" => "VIEW",
                    "P" => "PROCEDURE",
                    "FN" or "IF" or "TF" => "FUNCTION",
                    _ => rawType
                };
                result.Add(new DbObject { Name = reader.GetString(0), ObjectType = objectType });
            }
            return result;
        }

        /// <summary>
        /// Clone từ source sang local.<br/>
        /// - Tables: theo <paramref name="tableModes"/> (SchemaOnly / DataOnly / SchemaAndData)<br/>
        /// - Objects (Views/Procedures/Functions): theo <paramref name="selectedObjects"/>
        /// </summary>
        public async Task StartClone(
            string serverStr,
            string localStr,
            Dictionary<string, TableCloneMode> tableModes,   // tableName → mode
            List<DbObject> selectedObjects,                   // view/proc/func được chọn
            int maxParallel,
            Action<TableCloneProgress> onProgress,
            Dictionary<string, CancellationTokenSource>? itemCts = null)
        {
            var tablesNeedingSchema = tableModes
                .Where(kv => kv.Value is TableCloneMode.SchemaOnly or TableCloneMode.SchemaAndData)
                .Select(kv => kv.Key)
                .ToList();

            var tablesNeedingData = tableModes
                .Where(kv => kv.Value is TableCloneMode.DataOnly or TableCloneMode.SchemaAndData)
                .Select(kv => kv.Key)
                .ToList();

            // === PHASE 1: Rebuild schema (sequential) ===
            if (tablesNeedingSchema.Count > 0 || tablesNeedingData.Count > 0)
            {
                using var localConn = new SqlConnection(localStr);
                await localConn.OpenAsync();
                await ExecuteNonQuery(localConn, "EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'");

                // Các khóa ngoại bị gỡ tạm để drop được bảng, sẽ dựng lại ở cuối phase 1.
                var pendingFks = new List<ForeignKeyDef>();

                foreach (var tbl in tablesNeedingSchema)
                {
                    var ct = itemCts != null && itemCts.TryGetValue(tbl, out var cts) ? cts.Token : CancellationToken.None;
                    if (ct.IsCancellationRequested)
                    {
                        onProgress(new TableCloneProgress { TableName = tbl, Status = "Skipped", Percentage = 100, Message = "Cancelled by user", ItemType = "TABLE", CloneMode = tableModes[tbl] });
                        continue;
                    }
                    onProgress(new TableCloneProgress
                    {
                        TableName = tbl,
                        Status = "Preparing...",
                        ItemType = "TABLE",
                        CloneMode = tableModes[tbl]
                    });
                    using var srvConn = new SqlConnection(serverStr);
                    await srvConn.OpenAsync();
                    await RebuildTableSchema(srvConn, localConn, tbl, pendingFks);
                }

                // DataOnly: chỉ TRUNCATE, không rebuild schema
                foreach (var tbl in tablesNeedingData.Except(tablesNeedingSchema))
                {
                    onProgress(new TableCloneProgress
                    {
                        TableName = tbl,
                        Status = "Preparing...",
                        ItemType = "TABLE",
                        CloneMode = tableModes[tbl]
                    });
                    await ClearTableData(localConn, tbl);
                }

                // Dựng lại các khóa ngoại còn thiếu (bị gỡ để drop bảng, hoặc lúc tạo bảng thì bảng đích chưa có).
                if (pendingFks.Count > 0)
                {
                    foreach (var fk in pendingFks)
                    {
                        try { await ExecuteNonQuery(localConn, BuildAddForeignKeySql(fk)); }
                        catch { /* bo qua neu bang cha/bang dich khong con khop */ }
                    }

                    // Tắt lại constraint để phase bulk copy không bị chặn bởi các khóa ngoại vừa dựng.
                    await ExecuteNonQuery(localConn, "EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'");
                }
            }

            // === PHASE 2: BulkCopy parallel ===
            if (tablesNeedingData.Count > 0)
            {
                var semaphore = new SemaphoreSlim(maxParallel);
                var tasks = tablesNeedingData.Select(tableName => Task.Run(async () =>
                {
                    var mode = tableModes[tableName];
                    var ct = itemCts != null && itemCts.TryGetValue(tableName, out var cts) ? cts.Token : CancellationToken.None;
                    var p = new TableCloneProgress { TableName = tableName, Status = "Waiting...", ItemType = "TABLE", CloneMode = mode };

                    if (ct.IsCancellationRequested)
                    {
                        p.Status = "Skipped"; p.Message = "Cancelled by user"; p.Percentage = 100;
                        onProgress(p); return;
                    }

                    onProgress(p);
                    bool acquired = false;
                    try
                    {
                        await semaphore.WaitAsync(ct);
                        acquired = true;

                        if (ct.IsCancellationRequested)
                        {
                            p.Status = "Skipped"; p.Message = "Cancelled by user"; p.Percentage = 100;
                            onProgress(p); return;
                        }

                        p.Status = "Cloning..."; onProgress(p);

                        using var srvConn = new SqlConnection(serverStr);
                        using var locConn = new SqlConnection(localStr);
                        await srvConn.OpenAsync();
                        await locConn.OpenAsync();

                        var columns = await GetSafeColumns(locConn, tableName);
                        if (columns.Count == 0)
                        {
                            p.Status = "Success"; p.Percentage = 100; onProgress(p); return;
                        }

                        string colList = string.Join(",", columns.Select(c => $"[{c}]"));
                        long totalRows = await GetRowCount(srvConn, tableName);

                        bool hasId = await CheckIdentity(locConn, tableName);
                        if (hasId) await ExecuteNonQuery(locConn, $"SET IDENTITY_INSERT [{tableName}] ON");

                        using var selectCmd = new SqlCommand($"SELECT {colList} FROM [{tableName}]", srvConn);
                        selectCmd.CommandTimeout = 0;
                        using var reader = await selectCmd.ExecuteReaderAsync();

                        using var bulk = new SqlBulkCopy(locConn, SqlBulkCopyOptions.KeepIdentity, null)
                        {
                            DestinationTableName = $"[{tableName}]",
                            BatchSize = 5000,
                            BulkCopyTimeout = 0,
                            NotifyAfter = 500
                        };
                        foreach (var col in columns) bulk.ColumnMappings.Add(col, col);
                        bulk.SqlRowsCopied += (_, e) =>
                        {
                            p.RowsCopied = e.RowsCopied;
                            p.TotalRows = totalRows;
                            p.Percentage = totalRows > 0
                                ? Math.Min(Math.Round((double)e.RowsCopied / totalRows * 100, 1), 99)
                                : 50;
                            onProgress(p);
                        };

                        await bulk.WriteToServerAsync(reader, ct);
                        if (hasId) await ExecuteNonQuery(locConn, $"SET IDENTITY_INSERT [{tableName}] OFF");

                        p.Status = "Success"; p.Percentage = 100;
                        p.RowsCopied = totalRows; p.TotalRows = totalRows;
                    }
                    catch (OperationCanceledException)
                    {
                        p.Status = "Skipped"; p.Message = "Cancelled by user";
                    }
                    catch (Exception ex) { p.Status = "Error"; p.Message = ex.Message; }
                    finally { if (acquired) semaphore.Release(); }
                    onProgress(p);
                }));

                await Task.WhenAll(tasks);
            }
            else if (tablesNeedingSchema.Count > 0)
            {
                // SchemaOnly: mark success ngay sau phase 1
                foreach (var tbl in tablesNeedingSchema)
                    onProgress(new TableCloneProgress
                    {
                        TableName = tbl,
                        Status = "Success",
                        Percentage = 100,
                        ItemType = "TABLE",
                        CloneMode = tableModes[tbl]
                    });
            }

            // === PHASE 3: Re-enable FK ===
            if (tableModes.Count > 0)
            {
                using var localConn = new SqlConnection(localStr);
                await localConn.OpenAsync();
                await ExecuteNonQuery(localConn,
                    "EXEC sp_MSforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL'");
            }

            // === PHASE 4: Clone selected objects (Views / Procedures / Functions) ===
            if (selectedObjects.Count > 0)
                await CloneObjectsAsync(serverStr, localStr, selectedObjects, onProgress, itemCts);
        }

        private async Task CloneObjectsAsync(
            string serverStr,
            string localStr,
            List<DbObject> objects,
            Action<TableCloneProgress> onProgress,
            Dictionary<string, CancellationTokenSource>? itemCts = null)
        {
            // Load definitions from source
            var names = objects.Select(o => $"'{o.Name}'");
            string inList = string.Join(",", names);

            var definitions = new Dictionary<string, (string Type, string Definition)>(StringComparer.OrdinalIgnoreCase);

            using (var srvConn = new SqlConnection(serverStr))
            {
                await srvConn.OpenAsync();
                string sql = $@"
                    SELECT o.name, RTRIM(o.type) AS type, m.definition
                    FROM sys.sql_modules m
                    JOIN sys.objects o ON o.object_id = m.object_id
                    WHERE o.name IN ({inList}) AND o.is_ms_shipped = 0
                    ORDER BY CASE o.type
                        WHEN 'V'  THEN 1
                        WHEN 'FN' THEN 2 WHEN 'IF' THEN 2 WHEN 'TF' THEN 2
                        ELSE 3 END, o.name";
                using var cmd = new SqlCommand(sql, srvConn);
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                    definitions[reader.GetString(0)] = (reader.GetString(1).Trim(), reader.GetString(2));
            }

            using var locConn = new SqlConnection(localStr);
            await locConn.OpenAsync();

            foreach (var obj in objects)
            {
                if (!definitions.TryGetValue(obj.Name, out var def)) continue;
                var (rawType, definition) = def;

                string itemType = rawType switch
                {
                    "V" => "VIEW",
                    "P" => "PROCEDURE",
                    "FN" or "IF" or "TF" => "FUNCTION",
                    _ => rawType
                };

                string itemKey = $"{itemType}::{obj.Name}";
                var ct = itemCts != null && itemCts.TryGetValue(itemKey, out var cts) ? cts.Token : CancellationToken.None;

                var p = new TableCloneProgress { TableName = obj.Name, Status = "Cloning...", ItemType = itemType };

                if (ct.IsCancellationRequested)
                {
                    p.Status = "Skipped"; p.Message = "Cancelled by user"; p.Percentage = 100;
                    onProgress(p); continue;
                }

                onProgress(p);

                try
                {
                    string dropSql = rawType switch
                    {
                        "V" => $"IF OBJECT_ID(N'[{obj.Name}]','V')  IS NOT NULL DROP VIEW      [{obj.Name}]",
                        "P" => $"IF OBJECT_ID(N'[{obj.Name}]','P')  IS NOT NULL DROP PROCEDURE [{obj.Name}]",
                        "FN" => $"IF OBJECT_ID(N'[{obj.Name}]','FN') IS NOT NULL DROP FUNCTION  [{obj.Name}]",
                        "IF" => $"IF OBJECT_ID(N'[{obj.Name}]','IF') IS NOT NULL DROP FUNCTION  [{obj.Name}]",
                        "TF" => $"IF OBJECT_ID(N'[{obj.Name}]','TF') IS NOT NULL DROP FUNCTION  [{obj.Name}]",
                        _ => ""
                    };
                    if (!string.IsNullOrEmpty(dropSql)) await ExecuteNonQuery(locConn, dropSql);
                    await ExecuteNonQuery(locConn, definition);
                    p.Status = "Success"; p.Percentage = 100;
                }
                catch (Exception ex)
                {
                    bool isLinked = ex.Message.Contains("sys.servers", StringComparison.OrdinalIgnoreCase)
                                 || ex.Message.Contains("linked server", StringComparison.OrdinalIgnoreCase);
                    p.Status = isLinked ? "Skipped" : "Error";
                    p.Message = isLinked ? "Linked server not available on local (expected)" : ex.Message;
                }
                onProgress(p);
            }
        }

        // ── helpers ─────────────────────────────────────────────────────────────

        private async Task<long> GetRowCount(SqlConnection conn, string tbl)
        {
            using var cmd = new SqlCommand(
                "SELECT SUM(p.rows) FROM sys.partitions p " +
                $"WHERE p.object_id = OBJECT_ID('{tbl}') AND p.index_id IN (0,1)", conn);
            var result = await cmd.ExecuteScalarAsync();
            return (result != null && result != DBNull.Value) ? Convert.ToInt64(result) : 0;
        }

        private async Task RebuildTableSchema(SqlConnection srv, SqlConnection loc, string tbl, List<ForeignKeyDef> pendingFks)
        {
            // 1. Schema cot
            using var cmd = new SqlCommand($"SELECT TOP 0 * FROM [{tbl}]", srv);
            using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
            var schema = reader.GetSchemaTable();
            reader.Close();
            if (schema == null) return;

            // 2. DEFAULT CONSTRAINT
            var defaults = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            using (var defCmd = new SqlCommand(@"
                SELECT c.name, dc.definition
                FROM sys.default_constraints dc
                JOIN sys.columns c ON dc.parent_object_id = c.object_id
                                   AND dc.parent_column_id = c.column_id
                WHERE dc.parent_object_id = OBJECT_ID(@tbl)", srv))
            {
                defCmd.Parameters.AddWithValue("@tbl", tbl);
                using var defReader = await defCmd.ExecuteReaderAsync();
                while (await defReader.ReadAsync())
                    defaults[defReader.GetString(0)] = defReader.GetString(1);
            }

            // 3. PRIMARY KEY
            var pkColumns = new List<string>();
            string pkName = $"PK_{tbl}";
            using (var pkCmd = new SqlCommand(@"
                SELECT c.name, kc.name AS pk_name
                FROM sys.index_columns ic
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id  = i.index_id
                LEFT JOIN sys.key_constraints kc ON kc.parent_object_id = i.object_id
                                                 AND kc.unique_index_id = i.index_id
                WHERE i.object_id = OBJECT_ID(@tbl) AND i.is_primary_key = 1
                ORDER BY ic.key_ordinal", srv))
            {
                pkCmd.Parameters.AddWithValue("@tbl", tbl);
                using var pkReader = await pkCmd.ExecuteReaderAsync();
                while (await pkReader.ReadAsync())
                {
                    pkColumns.Add(pkReader.GetString(0));
                    if (!pkReader.IsDBNull(1)) pkName = pkReader.GetString(1);
                }
            }

            // 4. UNIQUE CONSTRAINT (khac voi unique index — phai clone rieng de EF nhan dien)
            var uniqueConstraints = new Dictionary<string, List<string>>(); // constraintName → cols
            using (var ucCmd = new SqlCommand(@"
                SELECT kc.name, c.name AS col_name, ic.key_ordinal
                FROM sys.key_constraints kc
                JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id
                                          AND kc.unique_index_id = ic.index_id
                JOIN sys.columns c        ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE kc.parent_object_id = OBJECT_ID(@tbl)
                  AND kc.type = 'UQ'
                ORDER BY kc.name, ic.key_ordinal", srv))
            {
                ucCmd.Parameters.AddWithValue("@tbl", tbl);
                using var ucReader = await ucCmd.ExecuteReaderAsync();
                while (await ucReader.ReadAsync())
                {
                    string ucName = ucReader.GetString(0);
                    if (!uniqueConstraints.ContainsKey(ucName))
                        uniqueConstraints[ucName] = new List<string>();
                    uniqueConstraints[ucName].Add(ucReader.GetString(1));
                }
            }

            // 5. Non-PK INDEXES (CLUSTERED / NONCLUSTERED, ASC / DESC)
            var indexes = new Dictionary<string, (bool IsUnique, bool IsClustered, List<string> Columns)>();
            using (var idxCmd = new SqlCommand(@"
                SELECT i.name, i.is_unique, i.type, c.name AS col_name, ic.is_descending_key
                FROM sys.indexes i
                JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c        ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE i.object_id       = OBJECT_ID(@tbl)
                  AND i.is_primary_key  = 0
                  AND i.is_unique_constraint = 0
                  AND i.type            IN (1, 2)
                  AND i.is_hypothetical = 0
                ORDER BY i.name, ic.key_ordinal", srv))
            {
                idxCmd.Parameters.AddWithValue("@tbl", tbl);
                using var idxReader = await idxCmd.ExecuteReaderAsync();
                while (await idxReader.ReadAsync())
                {
                    string idxName = idxReader.GetString(0);
                    bool isUnique = idxReader.GetBoolean(1);
                    bool isClustered = idxReader.GetByte(2) == 1;
                    string colName = idxReader.GetString(3);
                    bool isDesc = idxReader.GetBoolean(4);
                    string colEntry = isDesc ? $"[{colName}] DESC" : $"[{colName}]";
                    if (!indexes.ContainsKey(idxName))
                        indexes[idxName] = (isUnique, isClustered, new List<string>());
                    indexes[idxName].Columns.Add(colEntry);
                }
            }

            // 6. CHECK CONSTRAINT
            var checkConstraints = new Dictionary<string, string>(); // name → definition
            using (var ckCmd = new SqlCommand(@"
                SELECT cc.name, cc.definition
                FROM sys.check_constraints cc
                WHERE cc.parent_object_id = OBJECT_ID(@tbl)
                  AND cc.is_ms_shipped    = 0", srv))
            {
                ckCmd.Parameters.AddWithValue("@tbl", tbl);
                using var ckReader = await ckCmd.ExecuteReaderAsync();
                while (await ckReader.ReadAsync())
                    checkConstraints[ckReader.GetString(0)] = ckReader.GetString(1);
            }

            // 7. FOREIGN KEY (gom theo tên constraint để giữ đúng khóa ngoại nhiều cột)
            var foreignKeys = await ReadForeignKeys(srv, @"
                select fk.name                              as fk_name,
                       object_name(fk.parent_object_id)     as parent_table,
                       c_src.name                           as fk_col,
                       object_name(fk.referenced_object_id) as ref_table,
                       c_ref.name                           as ref_col,
                       fk.delete_referential_action_desc,
                       fk.update_referential_action_desc
                from sys.foreign_keys fk
                join sys.foreign_key_columns fkc on fk.object_id = fkc.constraint_object_id
                join sys.columns c_src on fkc.parent_object_id     = c_src.object_id and fkc.parent_column_id     = c_src.column_id
                join sys.columns c_ref on fkc.referenced_object_id = c_ref.object_id and fkc.referenced_column_id = c_ref.column_id
                where fk.parent_object_id = object_id(@tbl)
                order by fk.name, fkc.constraint_column_id", tbl);

            // 8. TRIGGER definitions
            var triggers = new Dictionary<string, string>();
            using (var trgCmd = new SqlCommand(@"
                SELECT t.name, m.definition
                FROM sys.triggers t
                JOIN sys.sql_modules m ON t.object_id = m.object_id
                WHERE t.parent_id   = OBJECT_ID(@tbl)
                  AND t.is_ms_shipped = 0", srv))
            {
                trgCmd.Parameters.AddWithValue("@tbl", tbl);
                using var trgReader = await trgCmd.ExecuteReaderAsync();
                while (await trgReader.ReadAsync())
                    triggers[trgReader.GetString(0)] = trgReader.GetString(1);
            }

            // 9. MS_Description (comment) cua table va tung cot
            var comments = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            string? tableComment = null;
            using (var cmtCmd = new SqlCommand(@"
                SELECT
                    CASE ep.minor_id WHEN 0 THEN NULL ELSE c.name END AS col_name,
                    CAST(ep.value AS NVARCHAR(MAX)) AS description
                FROM sys.extended_properties ep
                LEFT JOIN sys.columns c ON ep.major_id = c.object_id AND ep.minor_id = c.column_id
                WHERE ep.class    = 1
                  AND ep.name     = 'MS_Description'
                  AND ep.major_id = OBJECT_ID(@tbl)", srv))
            {
                cmtCmd.Parameters.AddWithValue("@tbl", tbl);
                using var cmtReader = await cmtCmd.ExecuteReaderAsync();
                while (await cmtReader.ReadAsync())
                {
                    string desc = cmtReader.IsDBNull(1) ? "" : cmtReader.GetString(1);
                    if (cmtReader.IsDBNull(0)) tableComment = desc;
                    else comments[cmtReader.GetString(0)] = desc;
                }
            }

            // ── BUILD CREATE TABLE ─────────────────────────────────────────────

            var colDefs = schema.Rows.Cast<DataRow>().Select(r =>
            {
                string colName = r["ColumnName"].ToString()!;
                string typeDef = GetSqlType(r);
                string identity = (bool)r["IsIdentity"] ? "IDENTITY(1,1)" : "";
                string defVal = defaults.TryGetValue(colName, out var dv) ? $"DEFAULT {dv}" : "";
                string nullable = (bool)r["AllowDBNull"] ? "NULL" : "NOT NULL";
                return $"[{colName}] {typeDef} {identity} {defVal} {nullable}".Trim();
            }).ToList();

            if (pkColumns.Count > 0)
            {
                string pkCols = string.Join(", ", pkColumns.Select(c => $"[{c}]"));
                colDefs.Add($"CONSTRAINT [{pkName}] PRIMARY KEY ({pkCols})");
            }

            foreach (var (ucName, ucCols) in uniqueConstraints)
            {
                string cols = string.Join(", ", ucCols.Select(c => $"[{c}]"));
                colDefs.Add($"CONSTRAINT [{ucName}] UNIQUE ({cols})");
            }

            foreach (var (ckName, ckDef) in checkConstraints)
                colDefs.Add($"CONSTRAINT [{ckName}] CHECK {ckDef}");

            // Gỡ các khóa ngoại từ bảng khác đang trỏ tới bảng này, nếu không SQL Server chặn drop table
            // (tắt constraint bằng NOCHECK vẫn bị chặn). Chúng được dựng lại ở cuối phase rebuild schema.
            await DropIncomingForeignKeys(loc, tbl, pendingFks);

            await ExecuteNonQuery(loc, $"IF OBJECT_ID(N'[{tbl}]','U') IS NOT NULL DROP TABLE [{tbl}]");
            await ExecuteNonQuery(loc, $"CREATE TABLE [{tbl}] ({string.Join(", ", colDefs)})");

            // ── POST CREATE TABLE ──────────────────────────────────────────────

            // Non-PK indexes
            foreach (var (idxName, info) in indexes)
            {
                string unique = info.IsUnique ? "UNIQUE " : "";
                string cluster = info.IsClustered ? "CLUSTERED" : "NONCLUSTERED";
                string cols = string.Join(", ", info.Columns);
                try { await ExecuteNonQuery(loc, $"CREATE {unique}{cluster} INDEX [{idxName}] ON [{tbl}] ({cols})"); }
                catch { /* bo qua filtered / computed index */ }
            }

            // Foreign Keys — bảng đích có thể chưa được tạo, khi đó để dành dựng lại ở cuối phase
            foreach (var fk in foreignKeys)
            {
                if (!await TableExists(loc, fk.RefTable))
                {
                    AddPendingFk(pendingFks, fk);
                    continue;
                }
                try { await ExecuteNonQuery(loc, BuildAddForeignKeySql(fk)); }
                catch { AddPendingFk(pendingFks, fk); }
            }

            // Triggers
            foreach (var (trgName, definition) in triggers)
            {
                try
                {
                    await ExecuteNonQuery(loc, $"IF OBJECT_ID(N'{trgName}','TR') IS NOT NULL DROP TRIGGER [{trgName}]");
                    await ExecuteNonQuery(loc, definition);
                }
                catch { /* bo qua trigger loi linked server / syntax */ }
            }

            // Comment cua table
            if (!string.IsNullOrEmpty(tableComment))
            {
                using var ct = new SqlCommand(@"
                    IF NOT EXISTS (SELECT 1 FROM sys.extended_properties
                                   WHERE class=1 AND name=N'MS_Description'
                                     AND major_id=OBJECT_ID(@tbl) AND minor_id=0)
                        EXEC sys.sp_addextendedproperty
                            @name=N'MS_Description', @value=@val,
                            @level0type=N'SCHEMA', @level0name=N'dbo',
                            @level1type=N'TABLE',  @level1name=@tbl
                    ELSE
                        EXEC sys.sp_updateextendedproperty
                            @name=N'MS_Description', @value=@val,
                            @level0type=N'SCHEMA', @level0name=N'dbo',
                            @level1type=N'TABLE',  @level1name=@tbl", loc);
                ct.Parameters.AddWithValue("@tbl", tbl);
                ct.Parameters.AddWithValue("@val", tableComment);
                try { await ct.ExecuteNonQueryAsync(); } catch { }
            }

            // Comment cua tung cot
            foreach (var (colName, desc) in comments)
            {
                if (string.IsNullOrEmpty(desc)) continue;
                using var cc = new SqlCommand(@"
                    IF NOT EXISTS (SELECT 1 FROM sys.extended_properties ep
                                   JOIN sys.columns col ON ep.major_id=col.object_id AND ep.minor_id=col.column_id
                                   WHERE ep.class=1 AND ep.name=N'MS_Description'
                                     AND ep.major_id=OBJECT_ID(@tbl) AND col.name=@col)
                        EXEC sys.sp_addextendedproperty
                            @name=N'MS_Description', @value=@val,
                            @level0type=N'SCHEMA', @level0name=N'dbo',
                            @level1type=N'TABLE',  @level1name=@tbl,
                            @level2type=N'COLUMN', @level2name=@col
                    ELSE
                        EXEC sys.sp_updateextendedproperty
                            @name=N'MS_Description', @value=@val,
                            @level0type=N'SCHEMA', @level0name=N'dbo',
                            @level1type=N'TABLE',  @level1name=@tbl,
                            @level2type=N'COLUMN', @level2name=@col", loc);
                cc.Parameters.AddWithValue("@tbl", tbl);
                cc.Parameters.AddWithValue("@col", colName);
                cc.Parameters.AddWithValue("@val", desc);
                try { await cc.ExecuteNonQueryAsync(); } catch { }
            }
        }

        private string GetSqlType(DataRow r)
        {
            string type = r["DataTypeName"].ToString()!;
            string size = r["ColumnSize"].ToString() ?? "";
            bool isMax = size == "-1" || size == "2147483647";

            if (type is "nvarchar" or "varchar" or "nchar" or "char")
                return $"{type}({(isMax ? "max" : size)})";
            if (type is "varbinary" or "binary")
                return $"{type}({(isMax ? "max" : size)})";
            if (type is "decimal" or "numeric")
                return $"{type}({r["NumericPrecision"]},{r["NumericScale"]})";
            if (type is "timestamp" or "rowversion")
                return "binary(8)";
            return type;
        }

        private async Task<List<string>> GetSafeColumns(SqlConnection conn, string tbl)
        {
            var list = new List<string>();
            using var cmd = new SqlCommand(
                "SELECT name FROM sys.columns " +
                $"WHERE object_id = OBJECT_ID('{tbl}') AND system_type_id <> 189 AND is_computed = 0", conn);
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) list.Add(r.GetString(0));
            return list;
        }

        /// <summary>Đọc khóa ngoại theo 1 câu query, gom nhiều dòng cùng tên constraint thành 1 khóa ngoại nhiều cột.</summary>
        private async Task<List<ForeignKeyDef>> ReadForeignKeys(SqlConnection conn, string sql, string tbl)
        {
            var byName = new Dictionary<string, ForeignKeyDef>(StringComparer.OrdinalIgnoreCase);
            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@tbl", tbl);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                string fkName = reader.GetString(0);
                if (!byName.TryGetValue(fkName, out var fk))
                {
                    fk = new ForeignKeyDef
                    {
                        Name = fkName,
                        ParentTable = reader.GetString(1),
                        RefTable = reader.GetString(3),
                        OnDelete = reader.GetString(5),
                        OnUpdate = reader.GetString(6)
                    };
                    byName[fkName] = fk;
                }
                fk.Columns.Add(reader.GetString(2));
                fk.RefColumns.Add(reader.GetString(4));
            }
            return byName.Values.ToList();
        }

        /// <summary>
        /// Gỡ mọi khóa ngoại từ bảng khác đang trỏ tới <paramref name="tbl"/> trên local và ghi lại vào
        /// <paramref name="pendingFks"/> để dựng lại sau. Khóa ngoại tự trỏ vào chính nó không cần gỡ vì drop table xóa luôn.
        /// </summary>
        private async Task DropIncomingForeignKeys(SqlConnection loc, string tbl, List<ForeignKeyDef> pendingFks)
        {
            var incoming = await ReadForeignKeys(loc, @"
                select fk.name                              as fk_name,
                       object_name(fk.parent_object_id)     as parent_table,
                       c_src.name                           as fk_col,
                       object_name(fk.referenced_object_id) as ref_table,
                       c_ref.name                           as ref_col,
                       fk.delete_referential_action_desc,
                       fk.update_referential_action_desc
                from sys.foreign_keys fk
                join sys.foreign_key_columns fkc on fk.object_id = fkc.constraint_object_id
                join sys.columns c_src on fkc.parent_object_id     = c_src.object_id and fkc.parent_column_id     = c_src.column_id
                join sys.columns c_ref on fkc.referenced_object_id = c_ref.object_id and fkc.referenced_column_id = c_ref.column_id
                where fk.referenced_object_id = object_id(@tbl)
                  and fk.parent_object_id    <> object_id(@tbl)
                order by fk.name, fkc.constraint_column_id", tbl);

            foreach (var fk in incoming)
            {
                await ExecuteNonQuery(loc,
                    $"if object_id(N'[{fk.Name}]','F') is not null alter table [{fk.ParentTable}] drop constraint [{fk.Name}]");
                AddPendingFk(pendingFks, fk);
            }
        }

        private static void AddPendingFk(List<ForeignKeyDef> pendingFks, ForeignKeyDef fk)
        {
            if (!pendingFks.Any(p => string.Equals(p.Name, fk.Name, StringComparison.OrdinalIgnoreCase)))
                pendingFks.Add(fk);
        }

        /// <summary>Sinh câu lệnh thêm khóa ngoại, tự bỏ qua nếu constraint đã có hoặc 2 bảng chưa sẵn sàng.</summary>
        private static string BuildAddForeignKeySql(ForeignKeyDef fk)
        {
            string cols = string.Join(", ", fk.Columns.Select(c => $"[{c}]"));
            string refCols = string.Join(", ", fk.RefColumns.Select(c => $"[{c}]"));
            string del = fk.OnDelete == "NO_ACTION" ? "" : $" on delete {fk.OnDelete.Replace("_", " ")}";
            string upd = fk.OnUpdate == "NO_ACTION" ? "" : $" on update {fk.OnUpdate.Replace("_", " ")}";
            return $@"if object_id(N'[{fk.Name}]','F') is null
                      and object_id(N'[{fk.ParentTable}]','U') is not null
                      and object_id(N'[{fk.RefTable}]','U')    is not null
                        alter table [{fk.ParentTable}] with nocheck add constraint [{fk.Name}]
                            foreign key ({cols}) references [{fk.RefTable}] ({refCols}){del}{upd}";
        }

        private async Task<bool> TableExists(SqlConnection conn, string tbl)
        {
            using var cmd = new SqlCommand("select object_id(@tbl, 'U')", conn);
            cmd.Parameters.AddWithValue("@tbl", tbl);
            var result = await cmd.ExecuteScalarAsync();
            return result != null && result != DBNull.Value;
        }

        /// <summary>Xóa sạch dữ liệu 1 bảng: ưu tiên TRUNCATE, nếu bị khóa ngoại chặn thì fallback DELETE.</summary>
        private async Task ClearTableData(SqlConnection conn, string tbl)
        {
            try { await ExecuteNonQuery(conn, $"if object_id(N'[{tbl}]','U') is not null truncate table [{tbl}]"); }
            catch { await ExecuteNonQuery(conn, $"if object_id(N'[{tbl}]','U') is not null delete from [{tbl}]"); }
        }

        private async Task<bool> CheckIdentity(SqlConnection conn, string tbl)
        {
            using var cmd = new SqlCommand(
                $"SELECT 1 FROM sys.identity_columns WHERE object_id = OBJECT_ID('{tbl}')", conn);
            return await cmd.ExecuteScalarAsync() != null;
        }

        private async Task ExecuteNonQuery(SqlConnection conn, string sql)
        {
            using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 0 };
            await cmd.ExecuteNonQueryAsync();
        }
    }
}