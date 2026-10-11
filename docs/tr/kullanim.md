# loadfleet kullanım rehberi

loadfleet, SSH gerektirmeden birçok makineden dağıtık yük testi yapar. Bir kontrol sunucusu ve paneli vardır;
agent'lar sunucuya **kendileri** bağlanır (dışa doğru wss), bu yüzden NAT ve güvenlik duvarı sorun olmaz.
İngilizce özet için: [README.md](../../README.md).

> **Sorumlu kullanım.** Yalnızca sahibi olduğunuz veya test etmeye açıkça izin verilen sistemlere yük verin.
> Hedef izin listesi (`--allow`) zorunludur, agent'lar hedefi ayrıca kontrol eder, test en fazla 2 saat sürer
> ve agent sunucuyu kaybettikten 15 sn sonra yükü kendiliğinden durdurur.

## 1. Kavramlar

- **Sunucu** (`loadfleet server`): panel, API, agent'ların bağlandığı uç nokta ve SQLite veritabanı (`loadfleet.db`).
- **Agent** (`loadfleet agent`): yükü üreten makine.
- **Plan**: YAML dosyası; hedef izin listesi, senaryo, kademeler ve durdurma kuralları.
- **Pay**: her kademenin hedefi agent'lara ağırlığa göre bölünür; zaman çizelgesi herkes için aynıdır.

## 2. Hızlı deneme (tek makine)

```yaml
name: hello
target: { allow: ["127.0.0.1"] }
scenario: { type: http, http: { url: "http://127.0.0.1:8080/" } }
load: { model: closed, stages: [{target: 50, ramp: 10s, hold: 30s}] }
stop: { error_rate: 0.02, p95_ms: { http: 300 } }
```

Önce `127.0.0.1:8080` üzerinde dinleyen bir hedef çalıştırın; yoksa hata oranı kuralı testi hemen durdurur.

```sh
loadfleet run hello.yaml     # reports/hello-<zaman>/ altına report.md, report.json, series.csv yazar
```

`loadfleet run <PLAN> [--out DİZİN] [--allow GİRİŞ] [--quiet]`. `--allow`, planın kendi izin listesini daraltır.
Daha fazla örnek: `examples/` klasörü.

## 3. Sunucuyu kurmak

Ayrı bir küçük makineye ya da (ör. SwarmUI'da) **ayrı bir stack** olarak kurun. Agent'ları test edilen servisin
kendi sunucularında **asla** çalıştırmayın: yük ölçülen sistemle kaynak yarışına girer ve sonuç anlamını yitirir.

### Docker Compose + Cloudflare Tunnel

```sh
mkdir loadfleet && cd loadfleet
curl --proto '=https' --tlsv1.2 -fsSLO https://raw.githubusercontent.com/alicankuklaci/loadfleet/main/deploy/docker-compose.server.yml
cat > .env <<EOT
LOADFLEET_ADMIN_PASSWORD=$(openssl rand -hex 16)
LOADFLEET_ALLOW=api.example.com,ws.example.com
TUNNEL_TOKEN=<Cloudflare Tunnel token>
EOT
chmod 600 .env
docker compose -f docker-compose.server.yml up -d
docker compose -f docker-compose.server.yml logs loadfleet | grep 'agent token'
```

- Tünelin public hostname'ini `http://loadfleet:8080` adresine yönlendirin (WebSocket açık olmalı).
- `LOADFLEET_ALLOW`: yük gönderilebilecek alan adları/IP'ler (**zorunlu**, `.env` içinde). Yalnızca test
  ortamının adlarını yazın; prod adları asla eklenmez.
- Konteyner kilitlidir: tüm yetkiler düşürülür (`cap_drop: [ALL]`), `no-new-privileges`, salt okunur kök
  dosya sistemi; yazılabilir yerler yalnızca veri birimi (`/data`, SQLite) ve küçük bir `/tmp`.
- Yönetici şifresi ve token'lar ortam değişkeniyle verilir (`LOADFLEET_ADMIN_PASSWORD`, `LOADFLEET_TOKEN`);
  komut satırına yazmayın, çünkü `ps` ile herkes görür.

### Proxy / Cloudflare Tunnel arkasında doğru istemci IP'si

- `--trust-proxy` **CIDR (veya IP) listesi** alır, tek başına kullanılmaz. Başlık (`X-Forwarded-For`,
  `X-Forwarded-Proto`) yalnızca doğrudan bağlanan eş bu listedeyse dikkate alınır.
- Docker'da çalışan loadfleet'in önünde ana makinedeki nginx varsa eş adresi köprü ağının geçidi olur, `127.0.0.1` değil; onu verin.
- Cloudflare Tunnel'da sunucuya doğrudan bağlanan `cloudflared` konteyneridir. Bu yüzden `cloudflared` ve
  `loadfleet`'in paylaştığı docker ağının CIDR'ını verin ve `--trust-cf-header` ekleyin
  (`CF-Connecting-IP` kullanılır). Compose dosyası bunu hazır yapar: ağ `edge`, alt ağ `172.30.0.0/24`,
  `LOADFLEET_TRUST_PROXY=172.30.0.0/24`, `LOADFLEET_TRUST_CF_HEADER=true`. Kendi ağınızın alt ağını
  `docker network inspect <ağ>` ile görebilirsiniz.
- `--trust-cf-header` **yalnızca** Cloudflare/cloudflared arkasında kullanılır; başka bir proxy, istemcinin
  gönderdiği `CF-Connecting-IP` değerini olduğu gibi geçirir ve IP sahteciliğine yol açar.
- `--trust-cf-header` yoksa istemci IP'si, `X-Forwarded-For` içindeki güvenilen proxy olmayan en sağdaki
  adrestir. Bu yüzden proxy'niz `X-Forwarded-For`'a **istemcinin yalın IP'sini** eklemelidir
  (`203.0.113.7`; port `203.0.113.7:51234`, köşeli parantez veya tırnak olmaz). nginx'te
  `$proxy_add_x_forwarded_for` bunu yapar. Çözümlenemeyen bir değer gelirse sunucu proxy'nin kendi adresini
  kullanır ve tüm istemciler aynı giriş limitini paylaşır.
- Panel HTTPS'te sunuluyorsa `--secure-cookies` kullanın.
- `POST /login` yalnızca `Origin` (yoksa `Referer`) başlığı panelin kendi `şema://host` adresi olan
  istekleri kabul eder. Şema `--secure-cookies` ile ya da güvenilen proxy `X-Forwarded-Proto: https`
  gönderdiğinde `https` olur; host, güvenilen proxy'nin `X-Forwarded-Host` değeri, yoksa `Host`'tur. nginx'te
  `proxy_set_header Host $host;` (veya `X-Forwarded-Host $host`) verin; cloudflared public `Host`'u korur.
- Yönetici şifresi en az 12 karakter olmalıdır, yoksa sunucu açılmaz. Son bir saatte tüm istemcilerden
  200'den fazla hatalı giriş olursa girişler sunucu genelinde yavaşlatılır: 10 saniyede bir deneme kabul
  edilir, diğerleri HTTP 429 alır. Bu bir kilit değildir; yönetici boş bir aralıkta yine girebilir ve
  hatalar bir saatlik pencereden çıkınca normal hıza dönülür. Açık oturumlar çalışmaya devam eder,
  sunucuyu yeniden başlatmak sayacı sıfırlar.

### Docker olmadan

```sh
export LOADFLEET_ADMIN_PASSWORD='<güçlü bir şifre>'
loadfleet server --allow loadtest-api.example.com,loadtest-ws.example.com --trust-proxy 127.0.0.1
```

Şifre verilmezse sunucu rastgele bir şifre üretir ve yalnızca bir kez stderr'e yazar. Açılışta
`agent token: ...` satırı yazılır; token **Agents** sayfasında da görünür ve **Rotate** ile yenilenir
(bağlı agent'lar düşmez, yeni agent'lar yeni token ister).

Sunucu bayrakları: `--listen` (varsayılan `0.0.0.0:8080`), `--db` (`loadfleet.db`), `--allow` (zorunlu),
`--admin-password`, `--trust-proxy`, `--trust-cf-header`, `--secure-cookies`, `--max-conns-per-agent`
(100000), `--max-duration` (`2h`). Hepsinin `LOADFLEET_*` ortam değişkeni karşılığı vardır.

## 4. Agent kurmak

Agent'ı önce ayarlayın (dosya tanıtıcı ve geçici port sınırlarını yükseltin), sonra başlatın. Token'ı ortam
değişkeniyle verin.

### macOS (ör. M5 Pro)

```sh
curl --proto '=https' --tlsv1.2 -fsSL https://github.com/alicankuklaci/loadfleet/releases/latest/download/install.sh | sh
sudo sysctl -w kern.maxfiles=2097152 kern.maxfilesperproc=1048576
sudo sysctl -w net.inet.ip.portrange.first=1024 net.inet.ip.portrange.hifirst=1024
ulimit -n 1048576
export LOADFLEET_TOKEN='<TOKEN>'
loadfleet agent --server wss://loadfleet.example.com --name m5-pro
```

Servis dosyaları `/usr/local/bin/loadfleet` yolunu varsayar; yükleyici bu dizine yazamazsa `~/.local/bin`'e kurar, o zaman dosyadaki yolu düzeltin. Kalıcı çalıştırmak için `deploy/io.loadfleet.agent.plist` dosyasını `~/Library/LaunchAgents/` altına kopyalayıp
`REPLACE_WITH_*` değerlerini doldurun (launchd `~` açmaz; günlük yolu `/Users/<kullanıcı>/Library/Logs/loadfleet/agent.log`
olur, dizini yükleyici oluşturur), `chmod 600` yapın ve `launchctl load -w ~/Library/LaunchAgents/io.loadfleet.agent.plist`
çalıştırın.

### Linux sunucu

```sh
curl --proto '=https' --tlsv1.2 -fsSL https://github.com/alicankuklaci/loadfleet/releases/latest/download/install.sh | sh
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535" fs.file-max=2097152 net.core.somaxconn=65535
```

systemd için `deploy/loadfleet-agent.service` ve `/etc/loadfleet/agent.env` (`LOADFLEET_SERVER`,
`LOADFLEET_TOKEN`, `LOADFLEET_NAME`; `chmod 600`) kullanılır. Servis kum havuzunda çalışır (salt okunur
sistem, ev dizini yok, özel /tmp, yetki yok, yalnızca IPv4/IPv6/unix soketleri; yazılabilir tek yer
`/var/lib/loadfleet`). Docker ile:

```sh
export LOADFLEET_TOKEN='<TOKEN>'
docker run -d --name lf-agent --restart unless-stopped --network host \
  --ulimit nofile=1048576:1048576 -e LOADFLEET_TOKEN ghcr.io/alicankuklaci/loadfleet \
  agent --server wss://loadfleet.example.com --name s1
```

Agent bayrakları: `--server`, `--token`, `--name` (varsayılan: makine adı), `--max-conns` (100000),
`--allow` (yerel ek izin listesi), `--public-ip`.

Agents sayfasında agent'ın **ready** olması gerekir. Saat ofseti 50 ms'yi geçerse uyarı verilir; 1 sn'yi
geçerse agent teste alınmaz. NTP'yi eşitleyin (`sudo sntp -sS time.apple.com` / `chronyc makestep`).

Bir kaynak IP'nin hedef `ip:port` başına yaklaşık 64 bin geçici portu vardır; makine ve hedef adres başına
yaklaşık 25 bin bağlantı planlayın.

## 5. Cloudflare WAF izin listesi

Hedef Cloudflare arkasındaysa agent trafiği, geçirilmezse challenge veya rate limit'e takılır. **Agents**
sayfası bağlı agent'ların sunucunun gördüğü adreslerinden hazır bir ifade üretir (IPv4 olduğu gibi, IPv6
`/64` olarak):

```
(ip.src in {203.0.113.7 198.51.100.4 2001:db8:1:2::/64})
```

`--public-ip` ile başlatılan bir agent'ın beyan ettiği adres, gözlenen adresin yanında "reported" olarak
gösterilir ama ifadeye hiç girmez: aksi halde agent token'ına sahip herkes WAF kuralınıza istediği adresi
ekletebilirdi. Agent'ın hedefe çıkan adresi sunucunun gördüğünden farklıysa (başka NAT/çıkış), beyan
edilen değeri kendiniz doğrulayıp kurala elle ekleyin.
Cloudflare, hedef zone, Security, WAF, Custom rules bölümünde **sırayla iki kural** oluşturun; ikisini de
yalnızca yük testi hostlarına bağlayın:

1. **Skip**: ifade = panelin ifadesi `and (http.host in {"loadtest-api.example.com" "loadtest-ws.example.com"})`;
   atlanacaklar: kalan tüm custom rules, rate limiting rules, managed rules ve Super Bot Fight Mode
   kuralları. Düz Bot Fight Mode custom rule ile atlanamayabilir; test hostlarının zone'unda test süresince
   kapatın.
2. **Block**: ifade = `(http.host in {"loadtest-api.example.com" "loadtest-ws.example.com"}) and not ip.src in {203.0.113.7 ...}`
   (aynı adres kümesi). Böylece yük testi yığınına agent'larınız dışında kimse ulaşamaz.

Test bitince iki kuralı da kapatın; agent'lar değiştiğinde ifadeyi yenileyin.

## 6. Plan yazmak

| anahtar | anlamı |
| --- | --- |
| `target.allow` | planın vurabileceği host, `*.alan`, IP, CIDR (sunucu `--allow` listesi de izin vermeli) |
| `scenario.type` | `http`, `tcp`, `ws` (veya derlemeye eklenmiş bir senaryo eklentisinin türü) ve eşleşen blok |
| `load.model` | `closed` (kademe hedefi = eşzamanlı VU) veya `open` (hedef = ops/sn; `max_vus` uçuştaki işlemi sınırlar) |
| `load.stages` | `{target, ramp, hold}`; geçiş bir önceki hedeften doğrusaldır |
| `stop` | `error_rate`, `p95_ms: {ölçüt: ms}`, `stop_on_agent_loss`, `max_duration` (en çok 2 saat) |
| `agents` | `min` hazır agent, `weights: auto \| equal \| {isim: ağırlık}` |

HTTP/TCP/WS içeriğinde şablonlar: `{{vu}}`, `{{iter}}`, `{{uuid}}`, `{{run}}`, `{{agent}}`,
`{{env.LOADFLEET_VAR_AD}}`.

`{{env.…}}` test başlarken her agent'ın kendi ortamından okunur ve yalnızca adı `LOADFLEET_VAR_` ile
başlayan değişkenler kullanılabilir (diğerleri plan hatasıdır). `LOADFLEET_VAR_API_TOKEN` gibi bir
gizli değer için bunun anlamı:

- değer planda yer almaz; sunucuda saklanmaz, panelde görünmez, agent bağlantısından geçmez. Onu
  yalnızca agent'lar okur ve planın hedefine gönderir;
- plan başlatabilen herkes (yönetici ya da panel parolasını ele geçiren) her agent'ın
  `LOADFLEET_VAR_*` değerlerini sunucunun ve o agent'ın `--allow` listelerinin izin verdiği herhangi
  bir host'a gönderebilir. Oraya yalnızca bu host'lara gitmesinde sakınca olmayan değerleri koyun;
- agent makinesindeki diğer değişkenler (bulut kimlik bilgileri, `LOADFLEET_TOKEN` vb.) planlarca
  okunamaz.

```yaml
name: post-json-open
target: { allow: ["api.example.com"] }
scenario:
  type: http
  http:
    method: POST
    url: "https://api.example.com/v1/score"
    headers: { content-type: application/json, authorization: "Bearer {{env.LOADFLEET_VAR_API_TOKEN}}" }
    body: '{"player":"{{vu}}","n":{{iter}}}'
    expect_status: [200, 201]
load: { model: open, max_vus: 4000, stages: [{target: 2000, ramp: 30s, hold: 120s}] }
stop: { error_rate: 0.01, p95_ms: { http: 250 } }
```

Panelde **New test** sayfasındaki şablonlar bu planların başlangıç noktasıdır.

## 7. Sonuçları okumak

- **Open model**: gecikme, işlemin *planlanan* gönderim zamanından ölçülür; yavaşlayan sunucu, yavaşlayan
  istemcinin arkasına saklanamaz (coordinated omission düzeltmesi). VU yuvaları dolu olduğu için hiç
  başlayamayan işlemler `open.dropped` sayılır: hata sayılırlar ve kademe **rate not achieved** (hedef
  ops/sn sağlanamadı) olarak işaretlenir. Bu kademenin gecikmesine bakıp "sunucu iyi" demeyin.
- **Closed model**: her istek gerçek gönderiminden ölçülür; doygunlukta gecikmeyi olduğundan düşük gösterir
  (istemci bekler, yavaş istek sayısı azalır). Gecikme hedefleri (SLO) için open modeli tercih edin.
- **LIMITED**: o kademede bir agent %85 CPU'nun, 50 ms runtime lag p95'in, %80 geçici port kullanımının
  üstündeydi ya da `client:*` soket hataları oldu. Sonuç hedefi olduğu kadar yük üretecini de ölçer;
  agent ekleyin veya agent başına payı düşürüp tekrar deneyin.
- Raporlar: `report.md`, `report.json`, `series.csv`; kademe başına yüzdelikler agent histogramlarının
  birleşiminden gelir. History sayfasından JSON/CSV/Markdown dışa aktarılır, iki koşu karşılaştırılır.

## 8. Güvenlik özeti

- Yalnızca izinli sistemler; sunucu izin listesi zorunlu, agent ikinci kontrolü yapar.
- Testler en fazla 2 saat; bağlantı sınırları `--max-conns-per-agent` / `--max-conns`.
- Agent token'ı ve yönetici şifresi ortam değişkeniyle verilir, git'e girmez; `.env` dosyaları `chmod 600`.
- Sunucu 15 sn sessiz kalırsa agent yükü kendiliğinden durdurur; Stop düğmesi bağlantıları hızla kapatır.
- Agent'ları hedefin kendi sunucularında çalıştırmayın.

## 9. Sorun giderme

| Belirti | Neden / çözüm |
| --- | --- |
| `client:too_many_files` | `ulimit -n` düşük; 4. bölümdeki ayarları yapın |
| `client:addr_not_available` | geçici port bitti; port aralığını genişletin, daha çok agent/IP kullanın |
| Agent "not ready" | saat ofseti 1 sn'den büyük; NTP eşitleyin |
| `blocked:<host>` | hedef planın veya agent'ın izin listesinde değil |
| Kademe "LIMITED" | agent CPU/lag/port sınırında; agent ekleyin ya da hedefi düşürün |
| Kademe "rate not achieved" | open modelde `max_vus` yetmedi ya da hedef yavaşladı; `max_vus`'u artırın, agent ekleyin |
| Agent bağlantısında 401 | token yenilenmiş; panelden yeni token alın |
| Panelde yanlış istemci IP'si | `--trust-proxy` CIDR'ı proxy'nin gerçek adresini kapsamıyor (bölüm 3) |
