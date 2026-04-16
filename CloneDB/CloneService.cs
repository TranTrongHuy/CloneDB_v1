using Microsoft.Data.SqlClient;
using System.Data;

namespace CloneDB
{
    public class TableCloneProgress
    {
        public string TableName { get; set; } = "";
        public double Percentage { get; set; }
        public string Status { get; set; } = "Pending"; // Pending, Preparing, Waiting, Cloning, Success, Error
        public string Message { get; set; } = "";
        public long RowsCopied { get; set; }
        public long TotalRows { get; set; }
        public string ItemType { get; set; } = "TABLE"; // TABLE, VIEW, PROCEDURE, FUNCTION
    }

    public class CloneService
    {
        // Bảng hệ thống cần loại trừ dù is_ms_shipped = 0
        private static readonly HashSet<string> _systemTableExclusions = new(StringComparer.OrdinalIgnoreCase)
        {
            "sysdiagrams", "__MigrationHistory", "__EFMigrationsHistory"
        };

        // Lấy danh sách bảng từ server source
        public async Task<List<string>> GetTablesAsync(string serverStr)
        {
            var tables = new List<string>();
            using var conn = new SqlConnection(serverStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand("SELECT name FROM sys.tables WHERE is_ms_shipped = 0 ORDER BY name", conn);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var name = reader.GetString(0);
                if (!_systemTableExclusions.Contains(name))
                    tables.Add(name);
            }
            return tables;
        }
        /// <summary>
        /// Clone dữ liệu từ server source về local theo 4 phase:<br/>
        /// 1. Tắt FK → Rebuild schema từng bảng (DROP + CREATE)<br/>
        /// 2. BulkCopy song song theo <paramref name="maxParallel"/><br/>
        /// 3. Bật lại FK<br/>
        /// 4. Clone Views / Functions / Procedures
        /// </summary>
        /// <param name="serverStr">Connection string của server source (remote).</param>
        /// <param name="localStr">Connection string của server đích (local).</param>
        /// <param name="selectedTables">Danh sách bảng cần clone.</param>
        /// <param name="maxParallel">
        /// Số bảng được clone đồng thời (giới hạn qua SemaphoreSlim).<br/>
        /// Khuyến nghị:<br/>
        /// • 2–3 : Server yếu, mạng chậm<br/>
        /// • 4   : Bình thường (mặc định)<br/>
        /// • 6–8 : Server mạnh, LAN nhanh<br/>
        /// • 8–10: Các bảng nhỏ (&lt; 1K rows)
        /// </param>
        /// <param name="onProgress">Callback cập nhật tiến trình cho từng bảng/object.</param>
        public async Task StartClone(string serverStr, string localStr, List<string> selectedTables, int maxParallel, Action<TableCloneProgress> onProgress)
        {
            // === PHASE 1: Chuẩn bị tuần tự (1 connection dùng chung) ===
            using (var localConn = new SqlConnection(localStr))
            {
                await localConn.OpenAsync();

                // Tắt toàn bộ FK constraints
                await ExecuteNonQuery(localConn, "EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'");

                foreach (var tbl in selectedTables)
                {
                    onProgress(new TableCloneProgress { TableName = tbl, Status = "Preparing..." });

                    // Luôn DROP + recreate từ source để đảm bảo schema đồng bộ
                    // Giải quyết: Invalid column name, invalid column length
                    using var srvConn = new SqlConnection(serverStr);
                    await srvConn.OpenAsync();
                    await RebuildTableSchema(srvConn, localConn, tbl);
                }
            }

            // === PHASE 2: BulkCopy song song, mỗi task có connection riêng ===
            var semaphore = new SemaphoreSlim(maxParallel);

            var tasks = selectedTables.Select(tableName => Task.Run(async () =>
            {
                var p = new TableCloneProgress { TableName = tableName, Status = "Waiting..." };
                onProgress(p);

                await semaphore.WaitAsync();
                try
                {
                    p.Status = "Cloning...";
                    onProgress(p);

                    // Mỗi task tạo connection RIÊNG (SqlConnection không thread-safe)
                    using var srvConn = new SqlConnection(serverStr);
                    using var locConn = new SqlConnection(localStr);
                    await srvConn.OpenAsync();
                    await locConn.OpenAsync();

                    // Lấy cột an toàn: bỏ timestamp (189) và computed columns
                    var columns = await GetSafeColumns(locConn, tableName);
                    if (columns.Count == 0)
                    {
                        p.Status = "Success";
                        p.Percentage = 100;
                        onProgress(p);
                        return;
                    }

                    string colList = string.Join(",", columns.Select(c => $"[{c}]"));
                    long totalRows = await GetRowCount(srvConn, tableName);

                    // IDENTITY_INSERT mỗi task có session riêng nên không conflict
                    bool hasId = await CheckIdentity(locConn, tableName);
                    if (hasId)
                        await ExecuteNonQuery(locConn, $"SET IDENTITY_INSERT [{tableName}] ON");

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

                    // Map theo tên cột để tránh lỗi thứ tự cột
                    foreach (var col in columns)
                        bulk.ColumnMappings.Add(col, col);

                    bulk.SqlRowsCopied += (_, e) =>
                    {
                        p.RowsCopied = e.RowsCopied;
                        p.TotalRows = totalRows;
                        p.Percentage = totalRows > 0
                            ? Math.Min(Math.Round((double)e.RowsCopied / totalRows * 100, 1), 99)
                            : 50;
                        onProgress(p);
                    };

                    await bulk.WriteToServerAsync(reader);

                    if (hasId)
                        await ExecuteNonQuery(locConn, $"SET IDENTITY_INSERT [{tableName}] OFF");

                    p.Status = "Success";
                    p.Percentage = 100;
                    p.RowsCopied = totalRows;
                    p.TotalRows = totalRows;
                }
                catch (Exception ex)
                {
                    p.Status = "Error";
                    p.Message = ex.Message;
                }
                finally
                {
                    semaphore.Release();
                }
                onProgress(p);
            }));

            await Task.WhenAll(tasks);

            // === PHASE 3: Bật lại FK constraints ===
            using (var localConn = new SqlConnection(localStr))
            {
                await localConn.OpenAsync();
                await ExecuteNonQuery(localConn,
                    "EXEC sp_MSforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL'");
            }

            // === PHASE 4: Clone Views, Procedures, Functions ===
            await CloneObjectsAsync(serverStr, localStr, onProgress);
        }

        private async Task CloneObjectsAsync(string serverStr, string localStr, Action<TableCloneProgress> onProgress)
        {
            var objects = new List<(string Name, string Type, string Definition)>();
            using (var srvConn = new SqlConnection(serverStr))
            {
                await srvConn.OpenAsync();
                // Views trước, rồi Functions, rồi Procedures
                const string sql = @"
                    SELECT o.name, RTRIM(o.type) AS type, m.definition
                    FROM sys.sql_modules m
                    JOIN sys.objects o ON o.object_id = m.object_id
                    WHERE o.type IN ('V', 'P', 'FN', 'IF', 'TF')
                      AND o.is_ms_shipped = 0
                    ORDER BY CASE o.type
                        WHEN 'V'  THEN 1
                        WHEN 'FN' THEN 2 WHEN 'IF' THEN 2 WHEN 'TF' THEN 2
                        ELSE 3 END, o.name";
                using var cmd = new SqlCommand(sql, srvConn);
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                    objects.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
            }

            using var locConn = new SqlConnection(localStr);
            await locConn.OpenAsync();

            foreach (var (name, type, definition) in objects)
            {
                string itemType = type switch
                {
                    "V" => "VIEW",
                    "P" => "PROCEDURE",
                    "FN" or "IF" or "TF" => "FUNCTION",
                    _ => type
                };
                var p = new TableCloneProgress { TableName = name, Status = "Cloning...", ItemType = itemType };
                onProgress(p);
                try
                {
                    // Drop nếu đã tồn tại
                    string dropSql = type switch
                    {
                        "V" => $"IF OBJECT_ID(N'[{name}]', 'V')  IS NOT NULL DROP VIEW [{name}]",
                        "P" => $"IF OBJECT_ID(N'[{name}]', 'P')  IS NOT NULL DROP PROCEDURE [{name}]",
                        "FN" => $"IF OBJECT_ID(N'[{name}]', 'FN') IS NOT NULL DROP FUNCTION [{name}]",
                        "IF" => $"IF OBJECT_ID(N'[{name}]', 'IF') IS NOT NULL DROP FUNCTION [{name}]",
                        "TF" => $"IF OBJECT_ID(N'[{name}]', 'TF') IS NOT NULL DROP FUNCTION [{name}]",
                        _ => ""
                    };
                    if (!string.IsNullOrEmpty(dropSql))
                        await ExecuteNonQuery(locConn, dropSql);

                    await ExecuteNonQuery(locConn, definition);
                    p.Status = "Success";
                    p.Percentage = 100;
                }
                catch (Exception ex)
                {
                    // Linked server không tồn tại ở local → đánh dấu Skipped thay vì Error
                    bool isLinkedServer = ex.Message.Contains("sys.servers", StringComparison.OrdinalIgnoreCase)
                                      || ex.Message.Contains("linked server", StringComparison.OrdinalIgnoreCase)
                                      || ex.Message.Contains("sp_addlinkedserver", StringComparison.OrdinalIgnoreCase);
                    if (isLinkedServer)
                    {
                        p.Status = "Skipped";
                        p.Message = "Linked server not available on local (expected)";
                    }
                    else
                    {
                        p.Status = "Error";
                        p.Message = ex.Message;
                    }
                }
                onProgress(p);
            }
        }

        private async Task<long> GetRowCount(SqlConnection conn, string tbl)
        {
            using var cmd = new SqlCommand(
                "SELECT SUM(p.rows) FROM sys.partitions p " +
                $"WHERE p.object_id = OBJECT_ID('{tbl}') AND p.index_id IN (0, 1)",
                conn);
            var result = await cmd.ExecuteScalarAsync();
            return (result != null && result != DBNull.Value) ? Convert.ToInt64(result) : 0;
        }

        /// <summary>
        /// DROP (nếu có) và CREATE lại bảng từ schema của source.
        /// Đảm bảo local luôn đồng bộ: không còn lỗi Invalid column name / invalid column length.
        /// </summary>
        private async Task RebuildTableSchema(SqlConnection srv, SqlConnection loc, string tbl)
        {
            using var cmd = new SqlCommand($"SELECT TOP 0 * FROM [{tbl}]", srv);
            using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly);
            var schema = reader.GetSchemaTable();
            reader.Close();
            if (schema == null) return;

            var colDefs = schema.Rows.Cast<DataRow>().Select(r =>
                $"[{r["ColumnName"]}] {GetSqlType(r)} {((bool)r["IsIdentity"] ? "IDENTITY(1,1)" : "")} {((bool)r["AllowDBNull"] ? "NULL" : "NOT NULL")}");

            // DROP nếu tồn tại (kể cả schema cũ), rồi CREATE mới từ source
            await ExecuteNonQuery(loc, $"IF OBJECT_ID(N'[{tbl}]', 'U') IS NOT NULL DROP TABLE [{tbl}]");
            string createSql = $"CREATE TABLE [{tbl}] ({string.Join(",", colDefs)})";
            await ExecuteNonQuery(loc, createSql);
        }

        private string GetSqlType(DataRow r)
        {
            string type = r["DataTypeName"].ToString()!;
            string size = r["ColumnSize"].ToString() ?? "";
            bool isMaxSize = size == "-1" || size == "2147483647";

            // Các kiểu cần độ dài
            if (type is "nvarchar" or "varchar" or "nchar" or "char")
                return $"{type}({(isMaxSize ? "max" : size)})";
            if (type is "varbinary" or "binary")
                return $"{type}({(isMaxSize ? "max" : size)})";
            if (type == "decimal" || type == "numeric")
                return $"{type}({r["NumericPrecision"]},{r["NumericScale"]})";

            // Bỏ qua timestamp/rowversion — đã lọc ở GetSafeColumns nhưng phòng hờ
            if (type == "timestamp" || type == "rowversion") return "binary(8)";

            return type;
        }

        private async Task<List<string>> GetSafeColumns(SqlConnection conn, string tbl)
        {
            var list = new List<string>();
            using var cmd = new SqlCommand(
                "SELECT name FROM sys.columns " +
                $"WHERE object_id = OBJECT_ID('{tbl}') AND system_type_id <> 189 AND is_computed = 0",
                conn);
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) list.Add(r.GetString(0));
            return list;
        }

        private async Task<bool> CheckIdentity(SqlConnection conn, string tbl)
        {
            using var cmd = new SqlCommand(
                $"SELECT 1 FROM sys.identity_columns WHERE object_id = OBJECT_ID('{tbl}')",
                conn);
            return await cmd.ExecuteScalarAsync() != null;
        }

        private async Task ExecuteNonQuery(SqlConnection conn, string sql)
        {
            using var cmd = new SqlCommand(sql, conn);
            cmd.CommandTimeout = 0;
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
