# CSV Pipeline

Pipeline chuyển đổi dữ liệu từ CSV sang SQLite và đánh giá khả năng sinh SQL tự động (Text-to-SQL) của các mô hình ngôn ngữ lớn. Dữ liệu đầu vào là các file CSV thực tế tiếng Việt, đầu ra là điểm Execution Accuracy so với Gold SQL.

## Môi trường và cài đặt

**Yêu cầu:** Python 3.12 trở lên, `uv` phiên bản mới nhất.

```bash
uv venv
uv sync
```

Tạo file `.env` từ mẫu và điền API key tương ứng:

```bash
cp .env.example .env
```

Các biến môi trường được hỗ trợ:

| Biến | Mô tả | Mặc định |
|------|--------|----------|
| `ANTHROPIC_API_KEY` | API key của Anthropic (Claude) | bắt buộc |
| `OPENAI_API_KEY` | API key của OpenAI | bắt buộc |
| `GEMINI_API_KEY` | API key của Google Gemini | bắt buộc |
| `CLAUDE_MODEL` | Model Claude sử dụng | `claude-haiku-4-5-20251001` |
| `GEMINI_MODEL` | Model Gemini sử dụng | `gemini-2.5-flash` |
| `OPENAI_MODEL` | Model OpenAI sử dụng | `gpt-4o-mini` |
| `CLAUDE_MAX_TOKENS` | Giới hạn token đầu ra cho Claude | `4096` |
| `GEMINI_MAX_TOKENS` | Giới hạn token đầu ra cho Gemini | `1024` |
| `OPENAI_MAX_TOKENS` | Giới hạn token đầu ra cho OpenAI | `4096` |
| `CLAUDE_CONCURRENCY` | Số luồng song song (chế độ `--concurrent`) | `5` |
| `GEMINI_CONCURRENCY` | Số luồng song song | `10` |
| `OPENAI_CONCURRENCY` | Số luồng song song | `20` |

## Cấu trúc đầu vào

Đặt file câu hỏi tại `input/questions.json` với định dạng mảng JSON:

```json
[
  {
    "db_id": "ten_bang",
    "question": "Câu hỏi bằng tiếng tự nhiên",
    "sql": "SELECT ...",
    "external_knowledge": ""
  }
]
```

Các file CSV tương ứng đặt trong `input/csv/`, đặt tên theo `<db_id>.csv`. Trường `external_knowledge` được nạp vào prompt dưới nhãn `Evidence`.

## Cách chạy

### Chạy toàn bộ pipeline

```bash
uv run main.py --model claude
uv run main.py --model gemini
uv run main.py --model openai
uv run main.py --model all       # chạy lần lượt tất cả ba model
```

Các tham số tùy chọn:

| Tham số | Mô tả | Mặc định |
|---------|--------|----------|
| `--model` | Backend LLM sử dụng (`claude`, `gemini`, `openai`, `all`) | `claude` |
| `--minibatch N` | Số câu hỏi mỗi batch gửi lên API | `50` |
| `--limit N` | Chỉ xử lý N câu hỏi đầu tiên | tất cả |
| `--fresh` | Bỏ qua checkpoint, chạy lại từ đầu | tắt |
| `--concurrent` | Gửi từng request song song thay vì dùng Batch API | tắt |
| `--debug` | Re-raise exception khi có lỗi | tắt |

Ví dụ chạy thử nhanh với giới hạn câu hỏi và bắt đầu mới hoàn toàn:

```bash
uv run main.py --model claude --fresh --limit 50 --minibatch 20
```

### Checkpoint và tiếp tục

Pipeline lưu checkpoint tại `output/checkpoint/<model>_checkpoint.json` sau mỗi minibatch. Nếu quá trình bị gián đoạn, chạy lại cùng lệnh để tiếp tục từ điểm dừng mà không mất dữ liệu đã xử lý. Để bắt đầu lại từ đầu:

```bash
uv run main.py --model claude --fresh
```

Với Claude, `batch_id` được lưu vào checkpoint trước khi bắt đầu polling. Điều này đảm bảo có thể resume ngay cả khi bị crash trong khi chờ kết quả batch.

### Chỉ chuyển đổi CSV sang SQLite

```bash
uv run main.py --convert-only
```

Bước này là idempotent: file SQLite đã tồn tại sẽ bị bỏ qua. Kết quả lưu tại `input/sqlite/`.

### Đánh giá kết quả

```bash
uv run main.py --eval --model claude
uv run main.py --eval --model all --limit 100
```

Lệnh `--eval` đọc file `output/<model>.json`, thực thi lại SQL dự đoán và SQL vàng trên SQLite, rồi tính Execution Accuracy (EA) theo phong cách Spider2. Kết quả in ra stdout:

```
[claude] Execution Accuracy: 0.7500 (75/100)
```

Log chi tiết bao gồm phân loại lỗi: SQL sinh ra bị lỗi cú pháp, kết quả sai nhưng không lỗi, và các trường hợp không tìm thấy gold.

### Giới hạn dataset để thử nghiệm

Pipeline không tích hợp việc chia train/test. Để thử nghiệm trên một phần nhỏ của dữ liệu, dùng `--limit`:

```bash
uv run main.py --model claude --limit 100
uv run main.py --eval --model claude --limit 100
```

Giá trị `--limit` được áp dụng theo thứ tự trong `questions.json`.

## Kết quả đầu ra

| File | Mô tả |
|------|--------|
| `output/<model>.json` | Kết quả sinh SQL và thực thi của từng câu hỏi |
| `output/checkpoint/<model>_checkpoint.json` | Checkpoint để tiếp tục nếu bị gián đoạn |
| `input/sqlite/<db_id>.sqlite` | SQLite được chuyển đổi từ CSV |

## Prompt

Template prompt được đọc từ `prompt.txt` tại thư mục gốc. Ba placeholder `{DATABASE SCHEMA}`, `{EVIDENCE}`, và `{QUESTION}` sẽ được thay thế trước khi gửi lên API. Khi `evidence` rỗng, nhãn "Evidence:" sẽ được bỏ qua hoàn toàn thay vì để trống.
