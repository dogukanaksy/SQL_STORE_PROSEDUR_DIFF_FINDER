using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using Newtonsoft.Json;
using System.Text.RegularExpressions;
using System.Linq;

class DatabaseInfo
{
    public string Database { get; set; }
    public string Username { get; set; }
    public string Password { get; set; }
}

class Program
{
    static void Main(string[] args)
    {
        string spName = "";
        while (string.IsNullOrWhiteSpace(spName))
        {
            Console.Write("Kontrol edilecek Stored Procedure adını girin: ");
            spName = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(spName))
            {
                Console.WriteLine("Geçersiz Stored Procedure adı. Lütfen tekrar deneyin.");
            }
        }

        string jsonPath = "databases.json";
        string spCheckPath = "sp_check.sql";
        string spUpdatePath = "sp_update.sql";
        string serverListPath = "serverAddresses.txt";

        // Dosya varlıklarını kontrol et
        if (!File.Exists(jsonPath) || !File.Exists(spCheckPath) || !File.Exists(spUpdatePath) || !File.Exists(serverListPath))
        {
            Console.WriteLine("Gerekli dosyalar eksik. Lütfen 'databases.json', 'sp_check.sql', 'sp_update.sql' ve 'serverAddresses.txt' dosyalarını kontrol edin.");
            BekleVeKapat();
            return;
        }

        var dbList = JsonConvert.DeserializeObject<List<DatabaseInfo>>(File.ReadAllText(jsonPath));
        var serverAddresses = File.ReadAllLines(serverListPath);

        if (serverAddresses.Length == 0)
        {
            Console.WriteLine("serverAddresses.txt dosyası boş.");
            BekleVeKapat();
            return;
        }

        var checkScript = File.ReadAllText(spCheckPath);
        var allChecked = new List<(string server, DatabaseInfo dbInfo)>();
        var different = new List<(string server, DatabaseInfo dbInfo)>();

        foreach (var server in serverAddresses)
        {
            foreach (var db in dbList)
            {
                string connStr = $"Server={server};Database={db.Database};User Id={db.Username};Password={db.Password};TrustServerCertificate=True;";

                try
                {
                    using var conn = new SqlConnection(connStr);
                    conn.Open();

                    string sql = @"
                        SELECT sm.definition 
                        FROM sys.procedures p
                        JOIN sys.sql_modules sm ON p.object_id = sm.object_id
                        WHERE p.name = @ProcName";

                    using var cmd = new SqlCommand(sql, conn);
                    cmd.Parameters.AddWithValue("@ProcName", spName);

                    var existing = cmd.ExecuteScalar()?.ToString();

                    if (existing != null)
                    {
                        allChecked.Add((server, db));
                        var existingNormalized = NormalizeSql(existing);
                        var checkScriptNormalized = NormalizeSql(checkScript);

                        if (!string.Equals(existingNormalized, checkScriptNormalized, StringComparison.OrdinalIgnoreCase))
                        {
                            different.Add((server, db));
                        }
                    }
                    else
                    {
                        Console.WriteLine($"[YOK] {server} - {db.Database}: SP bulunamadı.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[HATA] {server} - {db.Database}: {ex.Message}");
                }
            }
        }

        var allCheckedServers = allChecked.Select(x => x.server).Distinct().ToList();
        var differentServers = different.Select(x => x.server).Distinct().ToList();
        var allSameServers = allCheckedServers.Except(differentServers).ToList();

        string desktop = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
        string logFile = Path.Combine(desktop, "different_servers.txt");
        File.WriteAllLines(logFile, differentServers);

        Console.WriteLine("\nAynı olan sunucular:");
        foreach (var s in allSameServers)
            Console.WriteLine(s);

        Console.WriteLine("\nFark olan sunucular:");
        foreach (var s in differentServers)
            Console.WriteLine(s);

        if (different.Count == 0)
        {
            Console.WriteLine("\nTüm sunuculardaki prosedürler güncel. Güncellenecek sunucu bulunamadı.");
            BekleVeKapat();
            return;
        }

        string answer = "";
        while (true)
        {
            Console.Write("\nFark olan sunucularda SP'leri güncellemek ister misiniz? (evet/hayır): ");
            answer = Console.ReadLine()?.Trim().ToLower();

            if (answer == "evet" || answer == "hayır")
                break;

            Console.WriteLine("Lütfen sadece 'evet' ya da 'hayır' yazın.");
        }

        if (answer == "evet")
        {
            foreach (var (server, dbInfo) in different)
            {
                string connStr = $"Server={server};Database={dbInfo.Database};User Id={dbInfo.Username};Password={dbInfo.Password};TrustServerCertificate=True;";

                try
                {
                    using var conn = new SqlConnection(connStr);
                    conn.Open();

                    var updateScript = File.ReadAllText(spUpdatePath);
                    var batches = Regex.Split(updateScript, @"^\s*GO\s*$", RegexOptions.Multiline | RegexOptions.IgnoreCase)
                        .Where(b => !string.IsNullOrWhiteSpace(b))
                        .ToArray();

                    foreach (var batch in batches)
                    {
                        using var updateCmd = new SqlCommand(batch, conn);
                        updateCmd.ExecuteNonQuery();
                    }

                    Console.WriteLine($"[GÜNCELLENDİ] {server} - {dbInfo.Database}: Prosedür güncellendi.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[HATA] {server} - {dbInfo.Database}: {ex.Message}");
                }
            }
        }
        else
        {
            Console.WriteLine("Güncelleme iptal edildi.");
        }

        BekleVeKapat();
    }

    static string NormalizeSql(string sql)
    {
        sql = sql.ToLowerInvariant();
        sql = sql.Replace("\r\n", "\n");
        sql = Regex.Replace(sql, @"(?m)^\s*(use\s+\[.*?\]|go|set\s+quoted_identifier\s+(on|off)|set\s+ansi_nulls\s+(on|off))\s*;?\s*$", "", RegexOptions.IgnoreCase);
        sql = Regex.Replace(sql, @"/\*{2,}.*?\*/", "", RegexOptions.Singleline);
        sql = Regex.Replace(sql, @"--.*?$", "", RegexOptions.Multiline);
        sql = Regex.Replace(sql, @"\s+", " ");
        return sql.Trim();
    }

    static void BekleVeKapat()
    {
        Console.WriteLine("\nÇıkmak için bir tuşa basın...");
        Console.ReadKey();
    }
}
