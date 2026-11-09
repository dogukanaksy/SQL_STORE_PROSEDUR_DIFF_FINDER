# SQL_STORE_PROSEDUR_DIFF_FINDER

Çok sayıda SQL Server'da (örneğin her mağazanın kendi veritabanında) aynı **stored procedure**'ün güncel olup olmadığını kontrol eden, farklı olanları listeleyip isteğe bağlı olarak **toplu güncelleyen** bir .NET konsol aracı.

![Ekran görüntüsü](https://github.com/user-attachments/assets/07313eba-5c5e-4a31-a445-56b740216b38)

## Nasıl çalışır

1. Program başında kontrol edilecek SP adı girilir.
2. `serverAddresses.txt` içindeki her sunucuya bağlanır ve SP'nin içeriğini okur.
3. İçeriği `sp_check.sql` ile karşılaştırır. Aynıysa bir sonraki sunucuya geçer.
4. Fark bulunan sunucuları konsolda gösterir ve masaüstüne bir `.txt` dosyası olarak kaydeder.
5. Kullanıcı onaylarsa `sp_update.sql` scriptini yalnızca fark bulunan sunucularda sırayla çalıştırır.

## Dosyalar

| Dosya | İçerik |
|---|---|
| `serverAddresses.txt` | Kontrol edilecek sunucu adresleri (satır başına bir adres) |
| `databases.json` | Veritabanı adı ve bağlantı bilgileri |
| `sp_check.sql` | Karşılaştırılacak SP'nin **CREATE** hali |
| `sp_update.sql` | Fark olan yerlerde çalıştırılacak **ALTER** hali |

## Teknolojiler

C# · .NET 8 Console · System.Data.SqlClient · T-SQL · Newtonsoft.Json

> ⚠️ Gerçek sunucu adreslerini ve veritabanı şifrelerini repoya eklemeyin. Örnek değerlerle çalışın.
