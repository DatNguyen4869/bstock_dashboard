# bStocks Tracker Dashboard

Tool độc lập để theo dõi volume, điểm, phí (spread + gas) và thứ hạng Leaderboard chiến dịch Trust Wallet bStocks trên BSC.

## Yêu cầu
- Python 3.9+
- Thư viện `requests` (`pip install requests`)

## Cách chạy
- Nháy đúp `start_tracker.bat` hoặc chạy:
  ```bash
  python tracker_server.py
  ```
- Mở trình duyệt tại `http://localhost:8898`.
- Trên giao diện có thể dán địa chỉ ví BSC và chỉnh chu kỳ tự động refresh (mặc định `10s`, chọn nhanh `5s / 10s / 15s` hoặc nhập số giây tùy ý).
