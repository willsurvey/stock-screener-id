# 📈 Stock Screener Indonesia - API Endpoint

Automated stock screener untuk pasar saham Indonesia (IHSG) dengan strategi **MA50 Pullback + SMC Confirmation**.

Jalan otomatis setiap hari kerja pukul **18:00 WIB** via GitHub Actions.

---

## 🚀 Fitur Utama

| Fitur | Deskripsi |
|-------|-----------|
| 📊 **Screening Otomatis** | Scan 950+ saham IDX setiap hari |
| 🤖 **Smart Money Concepts** | Deteksi BOS, Order Block, FVG |
| 📉 **Technical Analysis** | MA50 Pullback, ATR, Volume Ratio |
| 💾 **Cache System** | Download data sekali, update harian |
| 🔗 **API Endpoint** | Akses hasil screening via JSON |
| 📅 **Jadwal Otomatis** | Senin-Jumat 18:00 WIB |

---

## 🔗 API Endpoint

### ✅ Latest Screening Results (URL STABIL)

Ini adalah URL utama yang **TIDAK BERUBAH** setiap hari:


GET https://raw.githubusercontent.com/willsurvey/stock-screener-id/main/latest_screening.json



### 📜 Historical Results (URL DATED)

Arsip hasil screening per tanggal:


GET https://raw.githubusercontent.com/willsurvey/stock-screener-id/main/watchlist_trader_id_smc_YYYYMMDD.json


Contoh:


https://raw.githubusercontent.com/willsurvey/stock-screener-id/main/watchlist_trader_id_smc_20250218.json


