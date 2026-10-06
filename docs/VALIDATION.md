# Doğrulama notları

## GitHub hazırlık kontrolü — 6 Ekim 2026

Güncel çalışan v3 kaynaklarından hazırlanan temiz kopyada 9 Node.js ve 6 Python birim testi geçti. Takip edilen dosyalar özel anahtar/GitHub token desenleri açısından kontrol edildi; gerçek yerel HMAC anahtarlarıyla eşleşme bulunmadı. TLS, `.env`, SQLite, model ve bağımlılık klasörlerinin ignore kuralları doğrulandı.

GitHub Actions yapılandırması eklendi; uzaktaki ilk çalışmasının sonucu yükleme sonrasında ayrıca değerlendirilmelidir. Bu hazırlık kontrolünde Docker servisleri yeniden kurulmadı.

## Önceki laboratuvar çalışmaları

30 Eylül 2026 tarihinde yerel Apache Kafka 3.9.1/KRaft, FastAPI, Node.js gateway ve SQLite üzerinde normal mesaj, değiştirilmiş MAC, replay, bozuk zarf ve AI kesintisi senaryoları çalıştırıldı. Kafka verified/quarantine/security_events çıktıları ve kalıcı olay API'si kontrol edildi.

Ayrı bir testte 11 Kafka mTLS/ACL senaryosu geçti. Aynı erişim kontrolleri kullanıcının çalışan yerel kurulumuna karşı da doğrulandı; kişisel yollar ve ham olay verileri bu depoya dahil edilmedi. Paylaşılan çıktı yalnızca senaryo, başarı durumu ve kısa sonucu içerir: [kafka-security-results.json](kafka-security-results.json).

Bu sonuçlar performans benchmark'ı, uçuş güvenliği doğrulaması veya bütün olası ACL yapılandırmalarının denetimi değildir. GitHub Actions birim testleri, gerçek Kafka/Docker entegrasyon testlerinden ayrıdır. Yeni bir ortamda `Start-SecureLab.ps1` veya README'deki Docker komutlarıyla erişim testlerini yeniden çalıştırın.

## Model değerlendirmesi

Önceki sentetik değerlendirme, ayrı rastgele tohumla 10.000 normal ve 1.000 adet 5000 °C örneği kullandı. Isolation Forest bu uç örneklerin %25,7'sini yakaladı; normal örneklerde yanlış alarm oranı %1,36 idi. Ayrı demo aralığı kuralı tüm uç örnekleri yakaladı. Bu kuralın sonucu, modelin öğrenilmiş tespit başarısı olarak sunulmamalıdır.

Bu veri, gerçek uçuş fazlarını veya siber saldırı çeşitliliğini temsil etmez. Modelin gerçek kullanım için uygunluğu gösterilmiş değildir. Model yanıtı `model_is_anomaly` ve `rule_anomaly` değerlerini ayrı verir.

## Sonraki doğrulama

- Gerçekçi ağ trafiği ve sistem kayıtları üzerinde saldırı türü bazlı precision, recall ve yanlış alarm oranı.
- Bağımsız eğitim/test ayrımı ve farklı uçuş fazlarında sensör anomali değerlendirmesi.
- Gecikme, yük, disk dolması, Kafka yeniden başlatma ve uzun kesinti testleri.
- Sertifika yenileme/iptal, kontrollü anahtar rotasyonu ve tüm ACL'lerin denetimi.
- Gerçek AHMS veri tüketicilerinin yalnızca verified akışından okuduğunun doğrulanması.
