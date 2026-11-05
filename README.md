# Airflow Receh Toolkit

Workshop: **Your First Data Pipeline with Apache Airflow**.

Di workshop ini kita membuat satu DAG yang membaca `orders.csv`, menghitung penjualan harian, memeriksa hasilnya, dan menyimpan `daily_sales.csv`.

## Sebelum mulai

- Docker Desktop (sudah berjalan) dan Docker Compose v2.
- VS Code atau editor lain.
- Git **atau** download ZIP dari GitHub.
- Siapkan setidaknya sekitar 4 GB memori untuk Docker. Jika Docker Desktop berat, ikuti demo mentor.
- **Tidak perlu install Python atau Airflow di laptop.** Keduanya berjalan di container.

## 1. Download repository

```bash
git clone https://github.com/caesarmario/airflow-receh-toolkit.git
cd airflow-receh-toolkit
```

Atau download ZIP dari GitHub, ekstrak, lalu buka foldernya di VS Code.

## 2. Jalankan Airflow

Pastikan Docker Desktop sudah aktif. Dari folder repository jalankan:

```bash
docker compose up -d
```

Buka **http://localhost:18088**. Halaman Airflow dapat diakses tanpa login karena environment ini khusus latihan lokal.

Saat pertama kali dijalankan, Airflow bisa membutuhkan waktu untuk inisialisasi. Kalau halaman belum terbuka, lihat logs:

```bash
docker compose logs -f airflow
```

Tekan `Ctrl+C` untuk berhenti melihat logs; container tetap berjalan.

**Pengguna Linux:** jika folder `output/` tidak dapat ditulisi container, buat file `.env` berisi `AIRFLOW_UID` sesuai user Linux, lalu jalankan ulang Compose. Contoh:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up -d
```

## 3. Buat DAG bersama mentor

Buka folder `dags/` dan buat file **`daily_sales_pipeline.py`** saat sesi live coding.

Kita akan membuat empat task:

1. `extract` — membaca CSV.
2. `transform` — menghitung transaksi berstatus `paid` per tanggal.
3. `validate` — memeriksa ringkasan penjualan.
4. `publish` — menulis file hasil ke folder `output/`.

Ini memang **starter repo**: belum ada kode jawaban DAG. Wajar kalau DAG workshop belum muncul sebelum kita membuatnya bersama.

## 4. Jalankan dan periksa hasilnya

Setelah menyimpan file DAG, buka Airflow UI, cari `daily_sales_pipeline`, lalu **Trigger DAG**.

Hasil yang diharapkan di `output/daily_sales.csv`:

```csv
date,total_orders,revenue
2026-11-01,2,150000
2026-11-02,1,75000
```

Urutan task: **extract → transform → validate → publish**.

## 5. Latihan troubleshooting

Buka `data/orders.csv` dan ubah baris berikut:

```text
O004,2026-11-02,75000,paid
```

menjadi:

```text
O004,2026-11-02,invalid,paid
```

Trigger DAG lagi. Temukan task yang gagal dan baca logs-nya. Setelah itu, kembalikan nilai menjadi `75000`, trigger ulang, dan periksa hasilnya.

**Catatan:** file CSV hasil dari run sukses sebelumnya mungkin masih ada walaupun run terbaru gagal. Periksa **status DAG Run** dan hasil terbaru, bukan hanya keberadaan file.

## 6. Matikan Airflow

```bash
docker compose down
```

Perintah di atas menghentikan container tanpa menghapus metadata Airflow. Untuk reset database latihan, gunakan `docker compose down -v` (menghapus metadata latihan). File di `output/` tidak otomatis dihapus.

## Jika terjadi masalah

| Masalah | Cek dulu |
|---|---|
| Docker tidak berjalan | Buka Docker Desktop lalu ulangi perintah |
| Port 18088 sudah dipakai | Tutup service yang memakai port; jangan menjalankan dua repo workshop sekaligus |
| Airflow UI belum siap | `docker compose logs -f airflow` |
| DAG tidak muncul | Pastikan file ada di `dags/`, disimpan, dan tidak ada import error |
| Output tidak ada | Periksa task `publish` dan logs task tersebut |
| Permission denied pada output (Linux) | Atur `AIRFLOW_UID` seperti instruksi di atas |

> Setup ini khusus latihan di laptop: satu container, SQLite, dan autentikasi lokal disederhanakan. **Jangan digunakan untuk production atau dibuka ke internet.**
