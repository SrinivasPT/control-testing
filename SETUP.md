# Quick Setup & Verification

## Install Dependencies

```bash
cd c:\Users\omega\projects\control-tester
pip install -r requirements.txt
```

This will install:
- `pydantic>=2.0.0` (DSL validation)
- `duckdb>=0.9.0` (SQL engine)
- `pandas>=2.0.0` (Excel reading)
- `openpyxl>=3.1.0` (Excel support)
- `pyarrow>=12.0.0` (Parquet files)
- `python-dotenv>=1.0.0` (Environment variables)
- `instructor>=0.4.0` (AI integration - optional)
- `openai>=1.0.0` (DeepSeek API - optional)

## Optional: Setup Environment Variables

Create `.env` file in project root:

```bash
# For AI-powered DSL generation (optional - can use --mock-ai instead)
DEEPSEEK_API_KEY=your_api_key_here
```

## Verify Installation

```bash
# Test imports
python -c "from src.orchestrator import BatchOrchestrator; print('✅ Success')"

# Run test suite
python tests/test_orchestrator.py
```

## Quick Test Run

```bash
# Test with mock AI (no API key needed)
python -m src.orchestrator --input data/input --mock-ai
```

## Common Issues

### Issue: `ModuleNotFoundError: No module named 'duckdb'`

**Solution:**
```bash
pip install -r requirements.txt
```

### Issue: `DEEPSEEK_API_KEY not found`

**Solutions:**
1. Use mock mode: `python -m src.orchestrator --mock-ai`
2. Or create `.env` file with API key

### Issue: `No Excel files found - skipping project`

**Solution:** Add `.xlsx` or `.xls` files to project folders:
```bash
# Example structure:
data/input/P1000/
  ├── control-information.md
  └── evidence_file.xlsx      # Add this
```

## Next Actions

1. **Install dependencies**: `pip install -r requirements.txt`
2. **Add evidence files**: Place Excel files in `data/input/P1000/`
3. **Run test**: `python -m src.orchestrator --mock-ai`
4. **Review results**: Check `data/audit.db` with SQLite browser
