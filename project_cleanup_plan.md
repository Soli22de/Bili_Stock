# Project Reorganization and Cleanup Plan

## Context
The user wants to prepare the project for GitHub sharing.
This involves:
1.  Verifying the `config.py` update (already done by user).
2.  Cleaning up the project structure by moving "immature" or "unused" code (specifically the Bilibili-related parts) to a separate `legacy/` or `archive/` folder.
3.  Ensuring the "Xueqiu Smart Momentum" core strategy is prominent.
4.  Committing the changes to Git.

## Objectives
1.  **Create Archive**: Create an `archive/bilibili_legacy` directory.
2.  **Move Legacy Code**: Move Bilibili-related scripts and core modules to the archive.
    *   `scripts/bili/` -> `archive/bilibili_legacy/scripts/`
    *   `core/bili_collector.py`, `core/ocr_validation.py`, etc. -> `archive/bilibili_legacy/core/`
    *   Root level scripts like `extract_video_keyframes.py`, `fetch_video_covers.py` -> `archive/bilibili_legacy/scripts/`
3.  **Clean Root**: Remove clutter files like `import asyncio.py`, `test_gemini.py`, `github_key` (sensitive!).
4.  **Organize Xueqiu Scripts**: Ensure `scripts/xueqiu/` contains the latest strategies.
5.  **Update README**: Briefly explain the new structure.
6.  **Git Commit**: Commit the cleanup.

## Task Breakdown

### 1. Create Archive Directory Structure
-   `mkdir -p archive/bilibili_legacy/core`
-   `mkdir -p archive/bilibili_legacy/scripts`
-   `mkdir -p archive/bilibili_legacy/docs`

### 2. Move Bilibili/Legacy Files
**Core:**
-   `core/bili_collector.py`
-   `core/extract_signals_llm.py`
-   `core/llm_processor.py`
-   `core/ocr_validation.py`
-   `core/ai_strategy.py` (seems legacy)

**Scripts (Root):**
-   `extract_video_keyframes.py`
-   `fetch_video_covers.py`
-   `fetch_new_blogger.py`
-   `manage_bloggers.py`
-   `process_ocr_gemini.py`
-   `research_ocr_demo.py`
-   `run_ocr_backtest.py`
-   `clean_and_rank_bloggers.py`
-   `discover_active_trading_ups.py`
-   `discover_ups.py`

**Scripts (Bili):**
-   `scripts/bili/` (entire folder)

### 3. Cleanup Root
-   Delete `github_key` and `github_key.pub` (Security Risk! Should not be in repo if they are real keys, or move to private backup if just generated for this task). **I will move them to a non-git folder or delete them if they are auto-generated.**
-   Delete `import asyncio.py` (looks like a typo file).
-   Delete `.~lock.*` files.

### 4. Git Operations
-   `git rm` moved files.
-   `git add` new locations.
-   `git commit -m "refactor: archive legacy bilibili components and organize project structure"`

## Deliverables
-   Cleaned up project structure.
-   `archive/` folder containing legacy code.
-   Git commit reflecting the reorganization.
