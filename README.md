#SQL_STORE_PROSEDUR_DIFF_FINDER

program başlangıcında yazılan sp adını serverAddresses.txt dosyasında bulunan ip adresiyle database'e bağlanıp ilgili dbde yazılan PROSEDÜRÜN İÇERİĞİNE BAKAR.

sp_check.sql deki sp ile bağlanılan yerdeki sp içeriği aynı mı değil mi kontrolü sağlanır.

Aynı ise işlem yapmadan bir sonraki ip adresine geçer.

liste tamamlandıktan sonra fark bulunan ip adresleri masaüstünde bir txt dosyasına yazılır.

Ayrıca consoleda kullanıcıya fark olan ip adresleri bunlar

sp_update.sql scriptini fark olan yerlerde çalıştırmak isteyip istemediğini sorar.

Kullanıcı hayır derse işlem yapılmadan program kapatılır.

Kullanıcı evet derse sırayla tüm fark olan dblere bağlanıp sp_update.sql scriptini çalıştırır.

Böylece spler belirlenen şekilde güncellenmiş olur.


NOT : sp_check.sql içeriğine karşılaştıracağınız sp'yi CREATE olarak yazmalısınız.

sp_update.sql txtsine de ALTER halini yani update edeceğiniz şekilde yazmalısınız.

databases.json dosyasına database bağlantı bilgileri girilmelidir.

