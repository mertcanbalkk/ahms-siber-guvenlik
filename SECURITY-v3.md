# Kafka taşıma ve yetkilendirme

| Kimlik | İzin |
| --- | --- |
| `CN=producer` | `flight_telemetry_raw`: Write/Describe |
| `CN=gateway` | raw: Read/Describe; `ahms-security-gateway-v2` grubuna Read; verified/quarantine/security_events: Write/Describe |
| `CN=reader` | verified: Read/Describe; `ahms-verified-reader` grubuna Read |
| `CN=probe` | ACL verilmez; yalnızca olumsuz test |
| `CN=admin` | Konu ve ACL kurulum operatörü, superuser |
| `CN=kafka` | Broker kimliği, superuser |

SSL dinleyicilerinde istemci sertifikası zorunludur. Hostname ve CA doğrulaması açıktır. PLAINTEXT Kafka istemci portu bulunmaz. Eşleşen ACL yoksa erişim reddedilir. Gateway konu oluşturmaz; ayrı, tamamlanınca çıkan `acl-init` servisi konuları/yetkileri hazırlar. HMAC doğrulaması ve kalıcı replay kontrolü TLS'ye ek olarak sürer. Anomali hâlâ gözlem amaçlı kabul edilir.

Her çalışma servisine yalnızca kendi TLS klasörü bağlanır. CA özel anahtarı `security/tls/authority` içindedir ve çalışma servislerine bağlanmaz. Admin sertifikası yalnızca kurulum servisine verilir. Test konteyneri producer/reader/probe kimliklerini birlikte kullanır; yalnızca `test` profiliyle çağrılır ve CA/admin özel anahtarını almaz. Anahtarlar, parolalar ve TLS klasörü ZIP'e eklenmez.

Sertifika üretimi `scripts/generate_tls.py` ile yapılır. RSA-3072, SHA-256, CA için 730 gün, servis sertifikaları için 365 gün kullanılır. Mevcut sertifikalar hash ve süre kontrolünden geçirilir; otomatik üzerine yazılmaz. Yedi günden az süre kalırsa betik yenileme gereğini bildirir. İptal, CRL/OCSP, otomatik yenileme ve sertifikadan HMAC producer_id eşleştirmesi bu pakette yoktur. ACL'nin izin verdiği üretici ele geçirilirse ham veriyi yine gönderebilir; HMAC/model bunun tüm türlerini yakalayamaz.

`init_acl.sh` beklenen ACL'leri idempotent biçimde ekler; önceden elle verilmiş geniş izinleri temizlemez. Güncellemedeki erişim testleri önemli kaçışları kontrol eder; tüm ACL'lerin denetimi yerine geçmez. Bu demo tek broker ve tek partition kullanır.

ZooKeeper yalnızca broker ile ayrı, dış erişime kapalı Compose ağındadır. ZooKeeper bağlantısı TLS/SASL ile korunmuş değildir; bu ayrım host/broker ele geçirilmesini çözmez. AI HTTP bağlantısı ayrı ağdadır; AI API ve panelde kullanıcı doğrulaması/HTTPS henüz yoktur. Host portları loopback'e bağlı kalır. Bu paket Kafka katmanını güçlendirir; tüm mikroservislerin kriptografik güvenliğini tamamlamaz.

Proje OneDrive altında olduğundan `security` klasöründeki özel anahtarlar da senkronize olabilir. Laboratuvar dışına taşırken anahtarları senkronize edilmeyen korumalı bir konuma almak gerekir.

`verified_reader.py` kabul edilmiş olayları okuyan örnek adaptördür. Gerçek AHMS tüketicisi, `event_id` tekilleştirmesini veri yazımıyla aynı işlemde yapmalıdır. Bu örnek yalnızca konsola yazar; MongoDB/InfluxDB entegrasyonu değildir.

Paylaşılan test kaydı: [11 izin/TLS senaryosu](docs/kafka-security-results.json). Diğer test koşulları: [doğrulama notları](docs/VALIDATION.md). İlk test brokeri Apache Kafka 3.9.1 KRaft'tır; erişim kontrolleri çalışan yerel kurulumda da doğrulanmıştır. Yeni bir kurulumda Compose erişim testlerini ayrıca çalıştırın. Test laboratuvarındaki loopback controller portu plaintext'tir; istemciler yalnızca mTLS portuna bağlanır. Bu controller yapılandırması dağıtım Compose'una eklenmez.

Kaynaklar: [Confluent TLS yapılandırması](https://docs.confluent.io/platform/7.4/security/security_tutorial.html), [Confluent yetkilendirme](https://docs.confluent.io/platform/7.4/kafka/authorization.html).
