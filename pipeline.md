# Pipeline đề xuất

## Làm sạch dữ liệu
- Encoding, delimiter detection
- Unmerge cells
- Normalize ("", " " → None) 
- Excel error replacement: #N/A, #VALUE!, #REF! → NULL
- Drop empty/duplicate rows/cols
- Header detection: Chọn dòng toàn phần tử phân biệt đầu tiên làm header, và kết hợp heuristics 
```code=python
def detect_header(rows, n=10):
    for i, row in enumerate(rows[:n]):
        non_null = [v for v in row if v is not None and str(v).strip() != ""]
        
        if len(non_null) < max(3, len(row) * 0.5):  # quá thưa → title row
            continue
        if len(non_null) != len(set(non_null)):      # có duplicate → skip
            continue
        if any(is_numeric(v) for v in non_null):     # có số → data row
            continue
        
        return i  # header row
    
    return 0  # fallback: dòng đầu tiên
```

- Column name sanitization: unidecode → lowercase → spaces/special chars thành underscore → deduplicate (col, col_2, col_3)
- Ditto mark resolution: thay ", '', nt, như trên bằng giá trị hàng trên cùng cột

## Convert sang SQLite 
- Mapping datatype
	- Ưu tiên theo thứ tự: date → boolean → integer → float → text
	- Date patterns: dd/mm/yyyy, yyyy-mm-dd, dd-mm-yyyy, dd.mm.yyyy
	- Số VN: dấu chấm nghìn + dấu phẩy thập phân (1.234.567,89)
	- Bảo vệ leading-zero strings (mã số, SĐT): nếu cột có ≥1 giá trị bắt đầu bằng 0 và parse được integer → giữ là TEXT
- Table name: Lấy tên file, dùng LLM chuẩn hoá nếu cần 

## Inference
- Tạo prompt, sử dụng M-Schema
	- Schema, sample rows, sample values
- Thử nghiệm các mô hình, sử dụng OpenRouter 
	- LLM: Claude, GPT, Qwen3.5 Coder 
	- SLM: Qwen2.5 Coder 7B 

## Đánh giá 
- Execution Accuracy 
	- Bỏ header 
	- Nếu không yêu cầu sort => Sort cả 2 input 
	- Column subset check: nếu ground truth có N cột, predicted có M ≥ N cột — chỉ cần N cột ground truth xuất hiện trong result của predicted (cho phép trả thừa cột)
	- Numeric tolerance: so sánh float với tolerance 1e-6 (tránh lỗi làm tròn)
	- NULL handling: NULL == NULL là true khi compare
- LLM-as-a-judge: Dùng lại framework của OmniSQL để đánh 