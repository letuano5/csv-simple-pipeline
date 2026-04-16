# CSV Simple Pipeline

## Môi trường

- [ ] `uv init` + `uv venv` đã chạy
- [ ] Dependencies đã thêm qua `uv add`, không cài thủ công
- [ ] Chạy bằng `uv run <file>.py`, không dùng `python <file>.py`

## Cách chạy

- Chạy toàn bộ

```
uv run main.py --model <tên model> --limit <số lượng câu hỏi>
```

- Chấm điểm

```
uv run main.py --eval --model <tên model> --limit <số lượng câu hỏi>
```
