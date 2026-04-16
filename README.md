## Yêu cầu

- [.NET 8 SDK](https://dotnet.microsoft.com/download)
- [Node.js](https://nodejs.org) (để build Tailwind CSS)
- SQL Server (source + local)

## Cài đặt lần đầu

```bash
cd CloneDB
npm install
```

## Cách hoạt động (CloneService)

Quá trình clone diễn ra theo 4 phase:

1. **Preparing** – Tắt FK constraints, DROP + CREATE lại schema từng bảng từ source
2. **Cloning** – BulkCopy song song (giới hạn bởi `maxParallel`)
3. **Restore FK** – Bật lại FK constraints
4. **Objects** – Clone Views, Functions, Stored Procedures

## Tham số maxParallel

| Giá trị | Khi nào dùng |
|---------|-------------|
| 2–3 | Server yếu, mạng chậm |
| 4 | Mặc định |
| 6–8 | Server mạnh, LAN nhanh |
| 8–10 | Bảng nhỏ (< 1K rows) |

## Lưu ý

- Bảng bị loại trừ: `sysdiagrams`, `__MigrationHistory`, `__EFMigrationsHistory`
- View/Function dùng linked server sẽ được đánh dấu **Skipped** (không phải Error)
