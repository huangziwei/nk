from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from fastapi import Body, FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response

from .book_io import is_original_text_file
from .library import list_books_sorted
from .refine import (
    append_override_entry,
    create_token_from_selection,
    edit_single_token,
    load_override_config,
    load_refine_config,
    refine_book,
    refine_chapter,
    remove_token,
)
from .uploads import UploadJob, UploadManager
from .web_assets import NK_APPLE_TOUCH_ICON_PNG, NK_FAVICON_URL

INDEX_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <title>Reader</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="apple-mobile-web-app-capable" content="yes">
  <link rel="icon" type="image/svg+xml" href="__NK_FAVICON__">
  <link rel="apple-touch-icon" href="/apple-touch-icon.png">
  <style>
    :root {
      color-scheme: dark;
      font-family: -apple-system, BlinkMacSystemFont, "Hiragino Sans", "Segoe UI", sans-serif;
      --bg: #05060b;
      --panel: #111423;
      --panel-alt: #191d31;
      --outline: #1f243d;
      --sidebar: #090b13;
      --text: #f3f4f6;
      --muted: #a3a8c5;
      --accent: #38bdf8;
      --accent-soft: rgba(56,189,248,0.15);
      --danger: #f87171;
      --warn: #fcd34d;
      --token-bg: rgba(59,130,246,0.3);
      --token-border: rgba(255,255,255,0.25);
      --token-active: #f97316;
    }
    html {
      -webkit-text-size-adjust: 100%;
      text-size-adjust: 100%;
    }
    * {
      box-sizing: border-box;
    }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
    }
    .app {
      display: grid;
      grid-template-columns: 320px 1fr;
      min-height: 100vh;
      position: relative;
    }
    .hidden {
      display: none !important;
    }
    body.modal-open {
      overflow: hidden;
    }
    aside {
      background: var(--sidebar);
      border-right: 1px solid var(--outline);
      padding: 1.2rem 1rem;
      display: flex;
      flex-direction: column;
      gap: 1rem;
      min-height: 100vh;
      transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    aside h1 {
      margin: 0;
      font-size: 1.25rem;
    }
    aside h1 a {
      color: inherit;
      text-decoration: none;
    }
    aside h1 a:focus-visible {
      outline: 2px solid var(--accent);
      outline-offset: 4px;
      border-radius: 6px;
    }
    .filter {
      display: flex;
      gap: 0.5rem;
    }
    .filter input[type="search"] {
      flex: 1;
      padding: 0.4rem 0.75rem;
      border-radius: 12px;
      border: 1px solid var(--outline);
      background: rgba(0,0,0,0.2);
      color: var(--text);
      font-size: 0.95rem;
    }
    .filter button {
      background: var(--panel-alt);
      border: 1px solid var(--outline);
      border-radius: 12px;
      color: var(--text);
      padding: 0 0.9rem;
      cursor: pointer;
      font-weight: 600;
      font-size: 0.85rem;
    }
    .sort-select {
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
      font-size: 0.8rem;
      color: var(--muted);
    }
    .sort-select select {
      border-radius: 10px;
      border: 1px solid var(--outline);
      background: rgba(0,0,0,0.2);
      color: var(--text);
      padding: 0.35rem 0.6rem;
      font-size: 0.85rem;
    }
    .upload-panel {
      border-radius: 16px;
      border: 1px solid rgba(255,255,255,0.06);
      background: rgba(0,0,0,0.15);
      padding: 0.9rem;
      display: flex;
      flex-direction: column;
      gap: 0.65rem;
    }
    .upload-drop {
      border: 1px dashed rgba(56,189,248,0.4);
      border-radius: 16px;
      padding: 0.85rem;
      text-align: center;
      cursor: pointer;
      background: rgba(56,189,248,0.06);
      transition: border-color 0.15s ease, background 0.15s ease, opacity 0.15s ease;
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
      outline: none;
    }
    .upload-drop strong {
      font-size: 0.95rem;
    }
    .upload-drop p {
      margin: 0;
      font-size: 0.8rem;
      color: var(--muted);
    }
    .upload-drop:focus-visible {
      outline: 2px solid var(--accent);
      outline-offset: 3px;
    }
    .upload-drop.dragging {
      border-color: var(--accent);
      background: rgba(56,189,248,0.15);
    }
    .upload-drop.upload-busy {
      opacity: 0.65;
      pointer-events: none;
    }
    .upload-actions {
      display: inline-flex;
      justify-content: center;
      gap: 0.4rem;
      flex-wrap: wrap;
      margin-top: 0.35rem;
    }
    .upload-actions button {
      background: var(--accent-soft);
      border: 1px solid rgba(56,189,248,0.4);
      border-radius: 999px;
      color: var(--accent);
      padding: 0.2rem 0.9rem;
      font-weight: 600;
      cursor: pointer;
    }
    .upload-error {
      min-height: 1.1rem;
      color: var(--danger);
      font-size: 0.8rem;
    }
    .upload-jobs {
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
      max-height: 35vh;
      overflow-y: auto;
    }
    .upload-empty {
      color: var(--muted);
      font-size: 0.85rem;
    }
    .upload-job {
      border: 1px solid var(--outline);
      border-radius: 12px;
      padding: 0.6rem 0.75rem;
      background: rgba(0,0,0,0.12);
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
    }
    .upload-job-header {
      display: flex;
      justify-content: space-between;
      gap: 0.4rem;
      font-size: 0.85rem;
      align-items: center;
    }
    .upload-job-title {
      font-weight: 600;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .upload-job-status {
      font-size: 0.78rem;
      border-radius: 999px;
      padding: 0.1rem 0.6rem;
      border: 1px solid rgba(255,255,255,0.12);
    }
    .upload-job[data-status="success"] .upload-job-status {
      color: #86efac;
      border-color: rgba(134,239,172,0.5);
    }
    .upload-job[data-status="error"] .upload-job-status {
      color: var(--danger);
      border-color: rgba(248,113,113,0.5);
    }
    .upload-job-message {
      font-size: 0.8rem;
      color: var(--muted);
      min-height: 1rem;
      word-break: break-word;
    }
    .upload-target {
      font-size: 0.8rem;
      color: var(--text);
      font-weight: 600;
    }
    .upload-progress {
      height: 6px;
      background: rgba(255,255,255,0.08);
      border-radius: 999px;
      overflow: hidden;
    }
    .upload-progress-bar {
      height: 100%;
      width: 0%;
      background: var(--accent);
      transition: width 0.2s ease;
    }
    .refine-progress {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      margin-top: 0.35rem;
    }
    .refine-progress.hidden {
      display: none;
    }
    .refine-progress-bar {
      position: relative;
      flex: 1 1 auto;
      height: 6px;
      border-radius: 999px;
      background: rgba(255,255,255,0.08);
      overflow: hidden;
    }
    .refine-progress-bar::before {
      content: '';
      position: absolute;
      top: 0;
      left: -40%;
      width: 40%;
      height: 100%;
      background: linear-gradient(90deg, rgba(56,189,248,0), rgba(56,189,248,0.8), rgba(56,189,248,0));
      animation: refine-progress 1.1s infinite linear;
    }
    @keyframes refine-progress {
      0% { transform: translateX(0%); }
      100% { transform: translateX(250%); }
    }
    .chapter-list {
      list-style: none;
      padding: 0;
      margin: 0;
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
      overflow-y: auto;
    }
    .chapter {
      border-radius: 12px;
      border: 1px solid transparent;
      background: transparent;
      padding: 0.6rem 0.75rem;
      padding-left: calc(0.75rem + var(--indent, 0rem));
      text-align: left;
      color: var(--text);
      cursor: pointer;
      transition: background 0.15s ease, border 0.15s ease;
      position: relative;
    }
    .chapter:hover {
      background: rgba(56,189,248,0.08);
    }
    .chapter.active {
      border-color: var(--accent);
      background: rgba(56,189,248,0.18);
    }
    .chapter .name {
      font-size: 0.95rem;
      font-weight: 600;
    }
    .chapter .meta {
      font-size: 0.78rem;
      color: var(--muted);
      margin-top: 0.15rem;
    }
    .chapter.folder-toggle::before {
      content: '▸';
      position: absolute;
      left: calc(0.35rem + var(--indent, 0rem));
      top: 50%;
      transform: translateY(-50%);
      color: var(--muted);
      transition: transform 0.15s ease;
      font-size: 0.78rem;
    }
    .chapter-folder.expanded > .folder-toggle::before {
      transform: translateY(-50%) rotate(90deg);
    }
    .chapter.folder-toggle:disabled {
      opacity: 0.8;
      cursor: default;
    }
    .chapter-folder,
    .chapter-leaf {
      list-style: none;
      margin: 0;
      padding: 0;
      display: block;
      width: 100%;
    }
    .chapter-children {
      list-style: none;
      margin: 0.35rem 0 0;
      padding: 0;
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
    }
    .chapter-children.collapsed {
      display: none;
    }
    main {
      padding: 1.4rem;
      display: flex;
      flex-direction: column;
      gap: 1.2rem;
      min-width: 0;
    }
    .status {
      color: var(--muted);
      font-size: 0.95rem;
      word-break: break-word;
      overflow-wrap: anywhere;
    }
    .sidebar-toggle {
      position: fixed;
      top: 0.85rem;
      right: 0.85rem;
      z-index: 50;
      background: var(--panel-alt);
      color: var(--text);
      border: 1px solid var(--outline);
      border-radius: 999px;
      padding: 0.35rem 0.95rem;
      font-size: 0.85rem;
      font-weight: 600;
      cursor: pointer;
      display: none;
    }
    .panel {
      background: var(--panel);
      border-radius: 16px;
      padding: 1rem 1.1rem;
      border: 1px solid var(--outline);
      box-shadow: 0 10px 30px rgba(0,0,0,0.2);
    }
    .panel h2,
    .panel h3 {
      margin: 0 0 0.6rem;
      font-size: 1rem;
      letter-spacing: 0.02em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .panel-heading {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.75rem;
      flex-wrap: wrap;
      margin-bottom: 0.6rem;
    }
    .panel h3 {
      font-size: 0.95rem;
    }
    .meta-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 0.75rem;
    }
    .meta-item {
      background: var(--panel-alt);
      border-radius: 12px;
      padding: 0.75rem 0.9rem;
      border: 1px solid rgba(255,255,255,0.05);
    }
    .meta-item dt {
      margin: 0;
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }
    .meta-item dd {
      margin: 0.35rem 0 0;
      font-weight: 600;
      font-size: 0.95rem;
      word-break: break-word;
    }
    .diagnostic-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 0.65rem;
      margin-bottom: 0.75rem;
    }
    .diagnostic-search {
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      align-items: center;
      margin-top: 0.35rem;
      margin-bottom: 0.4rem;
    }
    .diagnostic-search input[type="search"] {
      flex: 1;
      min-width: 160px;
      padding: 0.45rem 0.75rem;
      border-radius: 10px;
      border: 1px solid var(--outline);
      background: rgba(0,0,0,0.2);
      color: var(--text);
    }
    .diagnostic-search select {
      padding: 0.42rem 0.6rem;
      border-radius: 10px;
      border: 1px solid var(--outline);
      background: rgba(0,0,0,0.2);
      color: var(--text);
    }
    .diagnostic-results {
      display: flex;
      flex-direction: column;
      gap: 0.6rem;
      margin: 0.4rem 0 0.8rem;
    }
    .diagnostic-result {
      background: var(--panel-alt);
      border: 1px solid var(--outline);
      border-radius: 12px;
      padding: 0.75rem 0.85rem;
      display: grid;
      gap: 0.4rem;
    }
    .diagnostic-result-header {
      display: flex;
      justify-content: space-between;
      gap: 0.4rem;
      flex-wrap: wrap;
      align-items: baseline;
    }
    .diagnostic-result-title {
      font-weight: 700;
      font-size: 1rem;
      word-break: break-word;
    }
    .diagnostic-result-meta {
      display: inline-flex;
      gap: 0.35rem;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 0.85rem;
    }
    .diagnostic-result-context {
      color: var(--muted);
      font-size: 0.88rem;
      word-break: break-word;
    }
    .diagnostic-result-actions {
      display: flex;
      gap: 0.4rem;
      flex-wrap: wrap;
    }
    .diagnostic-result button {
      border-radius: 8px;
      border: 1px solid var(--outline);
      background: rgba(255,255,255,0.04);
      color: var(--text);
      padding: 0.3rem 0.75rem;
      cursor: pointer;
      font-weight: 600;
      font-size: 0.85rem;
    }
    .selection-toolbar {
      position: absolute;
      background: var(--panel-alt);
      border: 1px solid var(--outline);
      border-radius: 10px;
      padding: 0.35rem 0.6rem;
      display: inline-flex;
      gap: 0.4rem;
      align-items: center;
      box-shadow: 0 10px 30px rgba(0,0,0,0.35);
      z-index: 150;
    }
    .selection-toolbar.hidden {
      display: none;
    }
    .selection-toolbar button {
      border-radius: 8px;
      border: 1px solid var(--outline);
      background: var(--accent-soft);
      color: var(--text);
      padding: 0.25rem 0.65rem;
      cursor: pointer;
      font-weight: 600;
      font-size: 0.85rem;
    }
    .selection-toolbar button.secondary {
      background: transparent;
      border-color: rgba(255,255,255,0.2);
      color: var(--muted);
    }
    .diagnostic-card {
      background: var(--panel-alt);
      border-radius: 12px;
      padding: 0.65rem 0.75rem;
      border: 1px solid rgba(255,255,255,0.07);
    }
    .diagnostic-card .label {
      margin: 0;
      font-size: 0.72rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }
    .diagnostic-card .value {
      margin: 0.35rem 0 0;
      font-weight: 700;
      font-size: 1.05rem;
    }
    .diagnostic-card.warn {
      border-color: rgba(248,113,113,0.45);
      color: #fecdd3;
    }
    .diagnostic-conflicts-wrap {
      margin-top: 0.8rem;
      display: flex;
      flex-direction: column;
      gap: 0.4rem;
    }
    .diagnostic-conflicts {
      display: flex;
      flex-direction: column;
      gap: 0.65rem;
    }
    .diagnostic-conflict {
      background: var(--panel-alt);
      border: 1px solid var(--outline);
      border-radius: 12px;
      padding: 0.75rem 0.85rem;
    }
    .diagnostic-conflict.flagged {
      border-color: rgba(248,113,113,0.55);
      background: rgba(248,113,113,0.08);
    }
    .diagnostic-conflict-header {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 0.4rem;
      flex-wrap: wrap;
    }
    .diagnostic-surface {
      font-weight: 700;
      font-size: 1rem;
      word-break: break-word;
    }
    .diagnostic-meta {
      display: inline-flex;
      gap: 0.5rem;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 0.85rem;
    }
    .diagnostic-readings {
      margin: 0.35rem 0 0;
      font-size: 0.9rem;
      line-height: 1.4;
    }
    .diagnostic-chip {
      display: inline-flex;
      align-items: center;
      gap: 0.3rem;
      font-size: 0.78rem;
      border-radius: 9px;
      padding: 0.15rem 0.65rem;
      border: 1px solid rgba(255,255,255,0.12);
      background: rgba(255,255,255,0.04);
    }
    .diagnostic-chip.warn {
      color: var(--warn);
      border-color: rgba(252,211,77,0.55);
    }
    .diagnostic-empty {
      color: var(--muted);
      font-size: 0.9rem;
    }
    .text-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 1rem;
    }
    .text-grid.single-column {
      grid-template-columns: 1fr;
    }
    @media (max-width: 900px) {
      .text-grid {
        grid-template-columns: 1fr;
      }
    }
    .text-panel {
      background: var(--panel-alt);
      border-radius: 14px;
      border: 1px solid rgba(255,255,255,0.04);
      padding: 0.75rem;
      min-height: 240px;
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
      min-width: 0;
    }
    .text-panel header {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      font-size: 0.85rem;
      color: var(--muted);
      gap: 1rem;
    }
    .text-panel header strong {
      color: var(--text);
      font-size: 0.95rem;
    }
    .text-grid .text-controls {
      grid-column: 1 / -1;
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem;
      justify-content: flex-start;
      align-items: center;
    }
    .chapter-nav {
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      gap: 0.75rem;
      flex-wrap: wrap;
      margin-top: 0.5rem;
    }
    .chapter-nav-bottom {
      margin-top: 1rem;
    }
    .chapter-nav button {
      border-radius: 999px;
      border: 1px solid var(--outline);
      background: rgba(255,255,255,0.07);
      color: var(--text);
      padding: 0.35rem 0.9rem;
      font-weight: 600;
      font-size: 0.9rem;
      cursor: pointer;
      transition: background 0.15s ease, color 0.15s ease;
    }
    .chapter-nav button:hover:not(:disabled) {
      background: rgba(56,189,248,0.1);
      border-color: rgba(56,189,248,0.4);
    }
    .chapter-nav button:disabled {
      opacity: 0.45;
      cursor: not-allowed;
    }
    .chapter-nav-status {
      flex: 1;
      min-width: 160px;
      text-align: center;
      color: var(--muted);
      font-size: 0.9rem;
    }
    .toggle {
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      font-size: 0.85rem;
      background: rgba(0,0,0,0.2);
      border-radius: 999px;
      padding: 0.25rem 0.75rem;
      border: 1px solid var(--outline);
      cursor: pointer;
    }
    .toggle span {
      color: var(--muted);
    }
    .toggle input {
      accent-color: var(--accent);
    }
    .text-content {
      flex: 1;
      border-radius: 10px;
      border: 1px solid rgba(255,255,255,0.06);
      padding: 0.75rem;
      background: rgba(0,0,0,0.15);
      overflow: auto;
      min-width: 0;
    }
    .text-content.empty {
      display: flex;
      align-items: center;
      justify-content: center;
      color: var(--muted);
      font-style: italic;
    }
    .text-lines {
      display: flex;
      flex-direction: column;
      gap: 0.15rem;
      min-width: 100%;
    }
    .text-line {
      display: grid;
      grid-template-columns: 3rem minmax(0, 1fr);
      gap: 0.75rem;
      align-items: flex-start;
      font-size: 0.9rem;
    }
    .text-grid.single-column .text-line {
      grid-template-columns: minmax(0, 1fr);
    }
    .line-number {
      min-width: 3rem;
      font-family: "Hiragino Sans", "Yu Gothic", "Meiryo", "Noto Sans CJK JP", "Noto Sans JP", "Source Han Sans JP", "Source Han Serif JP", "Noto Serif CJK JP", "HanaMinA", "HanaMinB", "BabelStone Han", "SimSun-ExtB", "MingLiU-ExtB", "PMingLiU-ExtB", "MS Mincho", "MS Gothic", "Songti SC", "STHeiti", "PingFang SC", -apple-system, BlinkMacSystemFont, "Segoe UI", serif;
      color: var(--muted);
      text-align: right;
      padding-right: 0.4rem;
      border-right: 1px solid rgba(255,255,255,0.08);
      font-variant-numeric: tabular-nums;
    }
    .text-grid.single-column .line-number {
      display: none;
    }
    .line-body {
      display: block;
      width: 100%;
      font-family: "Hiragino Sans", "Yu Gothic", "Meiryo", "Noto Sans CJK JP", "Noto Sans JP", "Source Han Sans JP", "Source Han Serif JP", "Noto Serif CJK JP", "HanaMinA", "HanaMinB", "BabelStone Han", "SimSun-ExtB", "MingLiU-ExtB", "PMingLiU-ExtB", "MS Mincho", "MS Gothic", "Songti SC", "STHeiti", "PingFang SC", -apple-system, BlinkMacSystemFont, "Segoe UI", serif;
      white-space: pre-wrap;
      word-break: break-word;
      overflow-wrap: anywhere;
      line-height: 1.6;
      min-height: 1.6rem;
    }
    .text-panel.hidden {
      display: none;
    }
    .token-chunk {
      background: var(--token-bg);
      border-bottom: 1px solid var(--token-border);
      border-radius: 4px;
      padding: 0 0.15rem;
      cursor: pointer;
    }
    .token-chunk ruby {
      ruby-position: over;
      line-height: 1.2;
    }
    .token-chunk rt {
      font-size: 0.65em;
      font-weight: 500;
      color: var(--muted);
      letter-spacing: 0.08em;
    }
    .token-chunk.active {
      outline: 2px solid var(--token-active);
      background: rgba(249,115,22,0.25);
    }
    .modal {
      position: fixed;
      inset: 0;
      background: rgba(5,6,11,0.85);
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 1.5rem;
      z-index: 2000;
    }
    .modal-card {
      background: var(--panel-alt);
      border: 1px solid var(--outline);
      border-radius: 18px;
      padding: 1.3rem 1.5rem;
      width: min(480px, 95vw);
      max-height: 92vh;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      box-shadow: 0 25px 60px rgba(0,0,0,0.45);
    }
    .overrides-card {
      width: min(920px, 96vw);
      max-height: 92vh;
    }
    .modal-card h3 {
      margin: 0;
      font-size: 1.2rem;
    }
    .modal-card p {
      color: var(--muted);
      margin: 0.3rem 0 1rem;
      font-size: 0.9rem;
    }
    .refine-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 0.4rem;
      margin: -0.4rem 0 0.8rem;
      color: var(--muted);
      font-size: 0.85rem;
    }
    .overrides-body {
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      align-items: stretch;
      overflow: auto;
      padding-right: 0.25rem;
    }
    .overrides-list {
      border: 1px solid var(--outline);
      border-radius: 12px;
      background: rgba(0,0,0,0.1);
      max-height: 30vh;
      overflow-y: auto;
      display: flex;
      flex-direction: column;
    }
    .overrides-item {
      padding: 0.75rem 0.8rem;
      border-bottom: 1px solid rgba(255,255,255,0.05);
      display: grid;
      gap: 0.3rem;
      text-align: left;
      background: transparent;
      color: inherit;
      cursor: pointer;
      border: none;
    }
    .overrides-item:hover,
    .overrides-item:focus-visible {
      background: rgba(59,130,246,0.1);
      outline: none;
    }
    .overrides-item.active {
      background: rgba(59,130,246,0.18);
      border-left: 3px solid var(--accent);
    }
    .overrides-item strong {
      font-size: 0.95rem;
    }
    .overrides-item .meta {
      display: flex;
      gap: 0.35rem;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 0.85rem;
    }
    .overrides-empty {
      padding: 0.75rem;
      color: var(--muted);
      font-size: 0.9rem;
    }
    .overrides-form {
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
    }
    .overrides-form h4 {
      margin: 0.25rem 0;
      font-size: 1rem;
    }
    .overrides-grid {
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    }
    .checkbox-field {
      display: flex;
      align-items: center;
      gap: 0.5rem;
      font-weight: 600;
      font-size: 0.9rem;
    }
    .overrides-buttons {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 0.5rem;
    }
    .overrides-error {
      color: var(--danger);
      font-size: 0.9rem;
      flex: 1;
      text-align: center;
    }
    .overrides-header {
      display: flex;
      justify-content: space-between;
      gap: 0.8rem;
      align-items: center;
      flex-wrap: wrap;
      margin-bottom: 0.8rem;
    }
    .overrides-actions {
      display: flex;
      gap: 0.5rem;
      flex-wrap: wrap;
    }
    @media (max-width: 900px) {
      .overrides-body {
        grid-template-columns: 1fr;
      }
    }
    .refine-chip {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      padding: 0.15rem 0.65rem;
      border-radius: 999px;
      background: rgba(255,255,255,0.05);
      border: 1px solid rgba(255,255,255,0.12);
      font-size: 0.8rem;
    }
    .refine-chip.warn {
      color: var(--warn);
      border-color: rgba(252,211,77,0.55);
    }
    .form-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 0.9rem;
    }
    .form-field {
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
      font-size: 0.85rem;
    }
    .form-field input,
    .form-field textarea {
      border-radius: 10px;
      border: 1px solid rgba(255,255,255,0.15);
      background: rgba(0,0,0,0.25);
      padding: 0.45rem 0.6rem;
      font: inherit;
      color: inherit;
    }
    .form-field textarea {
      min-height: 4rem;
      resize: vertical;
    }
    .form-field.checkbox {
      flex-direction: row;
      align-items: center;
      gap: 0.5rem;
    }
    .modal-actions {
      margin-top: 1.2rem;
      display: flex;
      flex-direction: column;
      gap: 0.85rem;
    }
    .modal-button-group {
      display: flex;
      gap: 0.5rem;
    }
    .modal-actions-rows {
      display: flex;
      flex-direction: column;
      gap: 0.65rem;
      width: 100%;
    }
    .modal-button-row {
      display: flex;
      justify-content: space-between;
      gap: 0.75rem;
      flex-wrap: wrap;
      align-items: center;
    }
    .modal-button-row.end {
      justify-content: flex-end;
    }
    .modal-actions-label {
      color: var(--muted);
      font-size: 0.9rem;
      white-space: nowrap;
    }
    .modal-button-group.token-actions {
      justify-content: flex-end;
    }
    .modal-button-group.override-actions {
      justify-content: flex-end;
    }
    .modal-actions button {
      border-radius: 999px;
      border: 1px solid rgba(255,255,255,0.25);
      background: var(--accent-soft);
      color: var(--text);
      padding: 0.35rem 1rem;
      font-weight: 600;
      cursor: pointer;
    }
    .modal-actions button.secondary {
      background: transparent;
      color: var(--muted);
    }
    .modal-note {
      color: var(--danger);
      font-size: 0.85rem;
      min-height: 1.2rem;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 0.3rem;
      font-size: 0.78rem;
      border-radius: 999px;
      padding: 0.2rem 0.6rem;
      border: 1px solid rgba(255,255,255,0.12);
    }
    .pill.ok {
      color: #86efac;
      border-color: rgba(134,239,172,0.5);
    }
    .pill.missing {
      color: var(--danger);
      border-color: rgba(248,113,113,0.5);
    }
    .warn {
      color: var(--warn);
    }
    @media (max-width: 900px) {
      .app {
        grid-template-columns: 1fr;
      }
      aside {
        position: fixed;
        top: 0;
        left: 0;
        bottom: 0;
        width: min(320px, 85vw);
        max-width: 90vw;
        z-index: 40;
        box-shadow: 0 10px 40px rgba(0,0,0,0.4);
      }
      body.sidebar-collapsed aside {
        transform: translateX(-105%);
        box-shadow: none;
        pointer-events: none;
      }
      .sidebar-toggle {
        display: inline-flex;
        align-items: center;
        gap: 0.35rem;
      }
    }
  </style>
</head>
<body>
  <div class="app">
    <aside>
      <div>
        <h1><a href="/" id="home-link">nk Reader</a></h1>
        <p style="margin:0.35rem 0 0;font-size:0.85rem;color:var(--muted);">
          Select a chapter and hover tokens to compare transformed vs original offsets.
        </p>
      </div>
      <section class="upload-panel">
        <div class="upload-drop" id="upload-drop" role="button" tabindex="0" aria-label="Upload EPUB">
          <input type="file" accept=".epub" id="upload-input" hidden>
          <strong>Upload EPUB</strong>
          <p>Drag & drop an .epub here or click to select a file. nk will chapterize it automatically.</p>
          <div class="upload-actions">
            <button type="button">Select EPUB</button>
          </div>
        </div>
        <div class="upload-error" id="upload-error"></div>
        <div class="upload-jobs" id="upload-jobs">
          <div class="upload-empty">No uploads yet.</div>
        </div>
      </section>
      <div class="filter">
        <input type="search" placeholder="Filter chapters…" id="chapter-filter">
        <button id="refresh">↻</button>
      </div>
      <label class="sort-select" for="sort-order">
        <select id="sort-order">
          <option value="author">Author · Title</option>
          <option value="recent">Recently Added</option>
          <option value="played">Recently Played</option>
        </select>
      </label>
      <ul class="chapter-list" id="chapter-list"></ul>
    </aside>
    <main id="details">
      <button type="button" id="sidebar-toggle" class="sidebar-toggle" aria-controls="chapter-list" aria-expanded="true">
        Hide chapters
      </button>
      <div class="status" id="status">Loading chapters…</div>
      <section class="panel text-grid" id="text-grid">
        <div class="text-controls">
          <label class="toggle">
            <input type="checkbox" id="toggle-original" checked>
            <span>Original text</span>
          </label>
          <label class="toggle">
            <input type="checkbox" id="toggle-transformed">
            <span>Transformed text</span>
          </label>
        </div>
        <div class="chapter-nav" aria-label="Chapter navigation">
          <button type="button" data-role="chapter-prev" aria-label="Previous chapter" disabled>&larr; Previous</button>
          <div class="chapter-nav-status" data-role="chapter-nav-status">Select a chapter to preview.</div>
          <button type="button" data-role="chapter-next" aria-label="Next chapter" disabled>Next &rarr;</button>
        </div>
        <div class="text-panel" id="original-panel">
          <header>
            <strong>Original text</strong>
            <span id="original-meta" class="pill">—</span>
          </header>
          <div class="text-content empty" id="original-text">Original text unavailable.</div>
        </div>
        <div class="text-panel" id="transformed-panel">
          <header>
            <strong>Transformed text</strong>
            <span id="transformed-meta" class="pill">—</span>
          </header>
          <div class="text-content empty" id="transformed-text">Select a chapter to preview.</div>
        </div>
        <div class="chapter-nav chapter-nav-bottom" aria-label="Chapter navigation">
          <button type="button" data-role="chapter-prev" aria-label="Previous chapter" disabled>&larr; Previous</button>
          <div class="chapter-nav-status" data-role="chapter-nav-status">Select a chapter to preview.</div>
          <button type="button" data-role="chapter-next" aria-label="Next chapter" disabled>Next &rarr;</button>
        </div>
      </section>
      <section class="panel" id="diagnostics-panel" hidden>
        <div class="panel-heading">
          <h2>diagnostics</h2>
          <button type="button" class="secondary" id="overrides-open" disabled>View custom tokens</button>
        </div>
        <div class="diagnostic-grid" id="diagnostic-summary">
          <div class="diagnostic-empty">Load a chapter to view diagnostics.</div>
        </div>
        <div class="diagnostic-search">
          <input type="search" id="diagnostic-search-input" placeholder="Search surface (exact)" aria-label="Search token surface">
          <select id="diagnostic-search-scope" aria-label="Search scope">
            <option value="book">Whole book</option>
            <option value="chapter" selected>This chapter</option>
          </select>
        </div>
        <div class="diagnostic-results" id="diagnostic-search-results"></div>
        <div class="diagnostic-conflicts-wrap">
          <h3>ambiguous readings</h3>
          <div id="diagnostic-conflicts" class="diagnostic-conflicts">
            <div class="diagnostic-empty">No data yet.</div>
          </div>
        </div>
      </section>
      <section class="panel" id="meta-panel" hidden>
        <h2>chapter info</h2>
        <div class="meta-grid" id="meta-grid"></div>
      </section>
    </main>
  </div>
  <div id="overrides-modal" class="modal hidden" aria-hidden="true">
    <div class="modal-card overrides-card" role="dialog" aria-modal="true" aria-labelledby="overrides-title">
        <div class="overrides-header">
          <div>
            <h3 id="overrides-title">All custom tokens in</h3>
            <p class="modal-meta" id="overrides-meta">Edit custom_token.json for this book.</p>
          </div>
          <div class="overrides-actions">
            <button type="button" class="secondary" id="overrides-add">Add override</button>
            <button type="button" class="secondary" id="remove-add">Add removal</button>
            <button type="button" class="secondary" id="overrides-close">Close</button>
          </div>
        </div>
        <div class="overrides-body">
          <form class="overrides-form" id="overrides-form">
            <h4>Overrides</h4>
            <div class="overrides-list" id="overrides-list">
              <div class="overrides-empty">No overrides yet.</div>
            </div>
            <div class="form-grid overrides-grid">
              <label class="form-field">
                <span>Pattern *</span>
                <input type="text" id="override-pattern" required>
              </label>
            <label class="form-field">
              <span>Replacement</span>
              <input type="text" id="override-replacement">
            </label>
            <label class="form-field">
              <span>Reading</span>
              <input type="text" id="override-reading">
            </label>
            <label class="form-field">
              <span>Surface</span>
              <input type="text" id="override-surface">
            </label>
            <label class="form-field">
              <span>Match surface</span>
              <input type="text" id="override-match-surface">
            </label>
            <label class="form-field">
              <span>Part of speech</span>
              <input type="text" id="override-pos">
            </label>
            <label class="form-field">
              <span>Accent</span>
              <input type="number" id="override-accent" min="0">
            </label>
              <label class="checkbox-field">
                <input type="checkbox" id="override-regex">
                <span>Regex pattern</span>
              </label>
            </div>
            <h4>Removal rules</h4>
            <div class="overrides-list" id="remove-list">
              <div class="overrides-empty">No removal rules yet.</div>
            </div>
            <div class="form-grid overrides-grid">
              <label class="form-field">
                <span>Reading</span>
                <input type="text" id="remove-reading">
              </label>
              <label class="form-field">
                <span>Surface</span>
                <input type="text" id="remove-surface">
              </label>
            </div>
            <div class="overrides-buttons">
              <button type="button" class="danger secondary" id="override-delete">Delete override</button>
              <button type="button" class="danger secondary" id="remove-delete">Delete removal</button>
              <span class="overrides-error" id="overrides-error"></span>
              <button type="submit" id="override-save">Save changes to file</button>
            </div>
          </form>
        </div>
    </div>
  </div>
  <div id="refine-modal" class="modal hidden" aria-hidden="true">
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="refine-title">
      <h3 id="refine-title">Refine token</h3>
      <p id="refine-context">Choose a token to begin.</p>
      <div id="refine-meta" class="refine-meta"></div>
      <form id="refine-form" class="modal-form">
        <div class="form-grid">
          <label class="form-field">
            <span>Pattern *</span>
            <input type="text" id="refine-pattern" required>
          </label>
          <label class="form-field">
            <span>Replacement</span>
            <input type="text" id="refine-replacement">
          </label>
          <label class="form-field">
            <span>Reading</span>
            <input type="text" id="refine-reading">
          </label>
          <label class="form-field">
            <span>Accent</span>
            <input type="number" id="refine-accent" inputmode="numeric" min="0">
          </label>
          <label class="form-field">
            <span>Surface (kanji)</span>
            <input type="text" id="refine-surface">
          </label>
          <label class="form-field">
            <span>Match surface (original)</span>
            <input type="text" id="refine-match-surface" placeholder="Optional: require original surface">
          </label>
          <label class="form-field">
            <span>Part of speech</span>
            <input type="text" id="refine-pos" placeholder="noun, verb...">
          </label>
          <label class="form-field checkbox">
            <input type="checkbox" id="refine-regex">
            <span>Use regular expression</span>
          </label>
        </div>
        <div class="modal-actions">
          <div class="modal-note" id="refine-error"></div>
          <div class="refine-progress hidden" id="refine-progress">
            <div class="refine-progress-bar" id="refine-progress-bar"></div>
            <span class="pill" id="refine-progress-label">Refining…</span>
          </div>
          <div class="modal-actions-rows">
            <div class="modal-button-row">
              <span class="modal-actions-label">Apply changes to current token</span>
              <div class="modal-button-group token-actions">
                <button type="button" class="danger secondary" id="refine-delete-token" disabled>
                  Remove token
                </button>
                <button type="button" data-scope="token" id="refine-submit-token">
                  Save token
                </button>
              </div>
            </div>
            <div class="modal-button-row">
              <span class="modal-actions-label">Apply changes to all matching tokens</span>
              <div class="modal-button-group override-actions">
                <button type="button" data-scope="chapter" class="secondary" id="refine-submit-chapter">
                  This chapter only
                </button>
                <button type="button" data-scope="book" class="secondary" id="refine-submit-book">
                  The whole book
                </button>
              </div>
            </div>
            <div class="modal-button-row end">
              <button type="button" class="secondary" id="refine-cancel">Cancel</button>
            </div>
          </div>
        </div>
      </form>
    </div>
  </div>
  <script>
    (() => {
      const state = {
        chapters: [],
        filtered: [],
        selectedPath: null,
        tokens: [],
        filterValue: '',
        activeToken: null,
        chapterPayload: null,
        folderState: {},
        uploadJobs: [],
        sortOrder: 'author',
        pendingHighlightIndex: null,
      };
      const overridesState = {
        items: [],
        remove: [],
        selectedIndex: -1,
        selectedRemoveIndex: -1,
        bookId: null,
        path: null,
      };
      const baseTitle = document.title || 'nk Reader';
      const CHAPTER_HASH_PREFIX = '#chapter=';
      const CHAPTER_NAV_DEFAULT = 'Select a chapter to preview.';

      function getChapterPathFromHash() {
        const hash = window.location.hash || '';
        if (!hash.startsWith(CHAPTER_HASH_PREFIX)) {
          return null;
        }
        const encoded = hash.slice(CHAPTER_HASH_PREFIX.length);
        if (!encoded) {
          return null;
        }
        try {
          return decodeURIComponent(encoded);
        } catch (error) {
          console.warn('Failed to parse chapter hash', error);
          return encoded;
        }
      }

      function updateChapterHash(path) {
        const currentHash = window.location.hash || '';
        const nextHash = path ? `${CHAPTER_HASH_PREFIX}${encodeURIComponent(path)}` : '';
        if (currentHash === nextHash) {
          return;
        }
        if (path) {
          window.location.hash = nextHash;
          return;
        }
        if (currentHash) {
          const newUrl = `${window.location.pathname}${window.location.search}`;
          window.history.replaceState(null, '', newUrl);
        }
      }

      function currentBookId() {
        if (!state.selectedPath) return null;
        const parts = state.selectedPath.split('/');
        if (!parts.length) return null;
        parts.pop();
        return parts.join('/');
      }

      function updateOverridesButton() {
        if (!overridesOpenBtn) return;
        overridesOpenBtn.disabled = !currentBookId();
      }

      const listEl = document.getElementById('chapter-list');
      const homeLink = document.getElementById('home-link');
      const filterEl = document.getElementById('chapter-filter');
      const refreshBtn = document.getElementById('refresh');
      const sortSelect = document.getElementById('sort-order');
      const statusEl = document.getElementById('status');
      const metaPanel = document.getElementById('meta-panel');
      const textGrid = document.getElementById('text-grid');
      const metaGrid = document.getElementById('meta-grid');
      const diagnosticsPanel = document.getElementById('diagnostics-panel');
      const diagnosticSummary = document.getElementById('diagnostic-summary');
      const diagnosticConflicts = document.getElementById('diagnostic-conflicts');
      const diagnosticSearchInput = document.getElementById('diagnostic-search-input');
      const diagnosticSearchScope = document.getElementById('diagnostic-search-scope');
      const diagnosticSearchResults = document.getElementById('diagnostic-search-results');
      const selectionToolbar = document.createElement('div');
      selectionToolbar.className = 'selection-toolbar hidden';
      const selectionAddButton = document.createElement('button');
      selectionAddButton.type = 'button';
      selectionAddButton.textContent = 'Add token';
      const selectionCancelButton = document.createElement('button');
      selectionCancelButton.type = 'button';
      selectionCancelButton.className = 'secondary';
      selectionCancelButton.textContent = 'Cancel';
      selectionToolbar.appendChild(selectionAddButton);
      selectionToolbar.appendChild(selectionCancelButton);
      document.body.appendChild(selectionToolbar);
      let selectionRange = null;
      let selectionViewportRect = null;
      const transformedMeta = document.getElementById('transformed-meta');
      const originalMeta = document.getElementById('original-meta');
      const transformedText = document.getElementById('transformed-text');
      const originalText = document.getElementById('original-text');
      const transformedPanel = document.getElementById('transformed-panel');
      const originalPanel = document.getElementById('original-panel');
      const toggleTransformed = document.getElementById('toggle-transformed');
      const toggleOriginal = document.getElementById('toggle-original');
      const prevChapterButtons = Array.from(
        document.querySelectorAll('[data-role="chapter-prev"]')
      );
      const nextChapterButtons = Array.from(
        document.querySelectorAll('[data-role="chapter-next"]')
      );
      const chapterNavStatusNodes = Array.from(
        document.querySelectorAll('[data-role="chapter-nav-status"]')
      );
      let preferTransformedView = false;
      let hideOriginalView = false;
      try {
        const params = new URLSearchParams(window.location.search);
        const viewParam = params.get('view');
        const transformedParam = params.get('transformed');
        const originalParam = params.get('original');
        const normalizedView = typeof viewParam === 'string' ? viewParam.trim().toLowerCase() : '';
        const normalizedTransformed =
          typeof transformedParam === 'string' ? transformedParam.trim().toLowerCase() : '';
        const normalizedOriginal =
          typeof originalParam === 'string' ? originalParam.trim().toLowerCase() : '';
        if (normalizedView === 'transformed-only') {
          preferTransformedView = true;
          hideOriginalView = true;
        } else if (normalizedView === 'transformed') {
          preferTransformedView = true;
        } else if (['1', 'true', 'yes', 'on'].includes(normalizedTransformed)) {
          preferTransformedView = true;
        }
        if (['0', 'false', 'off', 'hide', 'no'].includes(normalizedOriginal)) {
          hideOriginalView = true;
        }
      } catch (error) {
        preferTransformedView = false;
        hideOriginalView = false;
      }
      const sidebarToggle = document.getElementById('sidebar-toggle');
      const mobileQuery = window.matchMedia('(max-width: 900px)');
      const bodyEl = document.body;
      const uploadDrop = document.getElementById('upload-drop');
      const uploadInput = document.getElementById('upload-input');
      const uploadErrorEl = document.getElementById('upload-error');
      const uploadJobsEl = document.getElementById('upload-jobs');
      const lineRegistry = {
        transformed: [],
        original: [],
      };
      const refineModal = document.getElementById('refine-modal');
      const refineForm = document.getElementById('refine-form');
      const refinePatternInput = document.getElementById('refine-pattern');
      const refineReplacementInput = document.getElementById('refine-replacement');
      const refineReadingInput = document.getElementById('refine-reading');
      const refineAccentInput = document.getElementById('refine-accent');
      const refineSurfaceInput = document.getElementById('refine-surface');
      const refineMatchSurfaceInput = document.getElementById('refine-match-surface');
      const refinePosInput = document.getElementById('refine-pos');
      const refineRegexInput = document.getElementById('refine-regex');
      const refineScopeButtons = Array.from(document.querySelectorAll('[data-scope]'));
      const refineSubmitBook = document.getElementById('refine-submit-book');
      const refineSubmitChapter = document.getElementById('refine-submit-chapter');
      const refineSubmitToken = document.getElementById('refine-submit-token');
      const refineDeleteToken = document.getElementById('refine-delete-token');
      const refineCancel = document.getElementById('refine-cancel');
      const refineError = document.getElementById('refine-error');
      const refineProgress = document.getElementById('refine-progress');
      const refineProgressLabel = document.getElementById('refine-progress-label');
      const refineContextLabel = document.getElementById('refine-context');
      const refineMeta = document.getElementById('refine-meta');
      const overridesModal = document.getElementById('overrides-modal');
      const overridesOpenBtn = document.getElementById('overrides-open');
      const overridesList = document.getElementById('overrides-list');
      const removeList = document.getElementById('remove-list');
      const overridesForm = document.getElementById('overrides-form');
      const overridesAddBtn = document.getElementById('overrides-add');
      const removeAddBtn = document.getElementById('remove-add');
      const overridesCloseBtn = document.getElementById('overrides-close');
      const overridesError = document.getElementById('overrides-error');
      const overridesMeta = document.getElementById('overrides-meta');
      const overridePatternInput = document.getElementById('override-pattern');
      const overrideReplacementInput = document.getElementById('override-replacement');
      const overrideReadingInput = document.getElementById('override-reading');
      const overrideSurfaceInput = document.getElementById('override-surface');
      const overrideMatchSurfaceInput = document.getElementById('override-match-surface');
      const overridePosInput = document.getElementById('override-pos');
      const overrideAccentInput = document.getElementById('override-accent');
      const overrideRegexInput = document.getElementById('override-regex');
      const overrideDeleteBtn = document.getElementById('override-delete');
      const removeDeleteBtn = document.getElementById('remove-delete');
      const removeReadingInput = document.getElementById('remove-reading');
      const removeSurfaceInput = document.getElementById('remove-surface');
      const overrideSaveBtn = document.getElementById('override-save');
      let alignFrame = null;
      let refineCurrentScope = 'book';
      const SORT_STORAGE_KEY = 'nkReaderSortOrder';
      const SORT_OPTIONS = ['author', 'recent', 'played'];
      const storedSort = window.localStorage.getItem(SORT_STORAGE_KEY);
      if (SORT_OPTIONS.includes(storedSort)) {
        state.sortOrder = storedSort;
      }
      let uploadPollTimer = null;
      let uploadDragDepth = 0;
      let refineContext = null;
      let refineBusy = false;
      let diagnosticSearchTimer = null;
      let selectionAnchor = null;
      const UPLOAD_POLL_INTERVAL = 4000;
      const initialHashPath = getChapterPathFromHash();
      if (initialHashPath) {
        state.selectedPath = initialHashPath;
        expandFoldersForPath(initialHashPath);
      }
      if (preferTransformedView && toggleTransformed) {
        toggleTransformed.checked = true;
      }
      if (hideOriginalView && toggleOriginal) {
        toggleOriginal.checked = false;
      }
      if (sortSelect) {
        sortSelect.value = state.sortOrder;
        sortSelect.addEventListener('change', () => {
          const next = SORT_OPTIONS.includes(sortSelect.value) ? sortSelect.value : 'author';
          if (state.sortOrder === next) {
            return;
          }
          state.sortOrder = next;
          try {
            window.localStorage.setItem(SORT_STORAGE_KEY, next);
          } catch (error) {
            console.warn('Failed to persist sort order', error);
          }
          loadChapters();
        });
      }
      if (refineCancel) {
        refineCancel.addEventListener('click', () => {
          closeRefineModal();
        });
      }
      if (refineModal) {
        refineModal.addEventListener('click', (event) => {
          if (event.target === refineModal) {
            closeRefineModal();
          }
        });
      }
      window.addEventListener('keydown', (event) => {
        if (event.key !== 'Escape') return;
        if (overridesModal && !overridesModal.classList.contains('hidden')) {
          closeOverridesModal();
          return;
        }
        if (refineModal && !refineModal.classList.contains('hidden')) {
          closeRefineModal();
        }
      });

      function submitCreateToken(payload) {
        if (!state.selectedPath) {
          setRefineError('Select a chapter before creating a token.');
          return;
        }
        if (
          !payload
          || typeof payload.start !== 'number'
          || typeof payload.end !== 'number'
          || payload.end <= payload.start
        ) {
          setRefineError('Invalid selection.');
          return;
        }
        const body = {
          start: payload.start,
          end: payload.end,
          path: state.selectedPath,
        };
        if (payload.replacement) body.replacement = payload.replacement;
        if (payload.reading) body.reading = payload.reading;
        if (payload.surface) body.surface = payload.surface;
        if (payload.pos) body.pos = payload.pos;
        if (typeof payload.accent === 'number') body.accent = payload.accent;
        refineCurrentScope = 'token';
        setRefineBusy(true);
        setRefineError('');
        const params = new URLSearchParams();
        params.set('path', state.selectedPath);
        fetchJSON(`/api/create-token`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...body, path: state.selectedPath }),
        })
          .then(() => {
            closeRefineModal();
            state.pendingHighlightIndex = null;
            openChapter(state.selectedPath, { autoCollapse: false });
          })
          .catch((error) => {
            setRefineError(error.message || 'Failed to create token.');
          })
          .finally(() => {
            setRefineBusy(false);
          });
      }

      function submitRefine(scope) {
        if (!state.selectedPath) {
          setRefineError('Select a chapter before applying overrides.');
          return;
        }
        const normalizedScope =
          scope === 'chapter' ? 'chapter' : (scope === 'token' ? 'token' : 'book');
        refineCurrentScope = normalizedScope;
        const pattern = refinePatternInput ? refinePatternInput.value.trim() : '';
        if (normalizedScope !== 'token' && !pattern) {
          setRefineError('Pattern is required.');
          return;
        }
        const replacement = refineReplacementInput ? refineReplacementInput.value.trim() : '';
        const reading = refineReadingInput ? refineReadingInput.value.trim() : '';
        const surface = refineSurfaceInput ? refineSurfaceInput.value.trim() : '';
        const matchSurface = refineMatchSurfaceInput
          ? refineMatchSurfaceInput.value.trim()
          : (refineContext && refineContext.token ? (refineContext.token.surface || '') : '');
        const pos = refinePosInput ? refinePosInput.value.trim() : '';
        const accentRaw = refineAccentInput ? refineAccentInput.value.trim() : '';
        let accentPayload = null;
        if (accentRaw) {
          const parsed = Number.parseInt(accentRaw, 10);
          if (Number.isNaN(parsed)) {
            setRefineError('Accent must be an integer.');
            return;
          }
          accentPayload = parsed;
        }
        if (
          normalizedScope === 'token'
          && refineContext
          && refineContext.selection
          && (typeof refineContext.selection.start === 'number')
          && (typeof refineContext.selection.end === 'number')
        ) {
          // Treat as create-token when selection is provided and no token index is set
          submitCreateToken({
            start: refineContext.selection.start,
            end: refineContext.selection.end,
            replacement,
            reading,
            surface,
            pos,
            accent: accentPayload,
          });
          return;
        }
        if (
          normalizedScope === 'token' &&
          (!refineContext || typeof refineContext.index !== 'number')
        ) {
          setRefineError('Select a token to edit.');
          return;
        }
        const payload = {
          path: state.selectedPath,
          scope: normalizedScope,
        };
        if (normalizedScope !== 'token') {
          payload.pattern = pattern;
          payload.regex = Boolean(refineRegexInput && refineRegexInput.checked);
        } else if (refineContext && typeof refineContext.index === 'number') {
          payload.token_index = refineContext.index;
        }
        if (replacement) {
          payload.replacement = replacement;
        }
        if (reading) payload.reading = reading;
        if (surface) payload.surface = surface;
        if (normalizedScope !== 'token' && matchSurface) {
          payload.match_surface = matchSurface;
        }
        if (pos) payload.pos = pos;
        if (accentPayload !== null) payload.accent = accentPayload;
        setRefineBusy(true);
        setRefineError('');
        fetchJSON('/api/refine', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        })
            .then((result) => {
              const updated = result && typeof result.updated === 'number' ? result.updated : 0;
              const scopeResult =
                result && typeof result.scope === 'string' ? result.scope : normalizedScope;
              const chapterLabel = result && result.chapter ? result.chapter : state.selectedPath;
              const bookLabel =
                (result && result.book) ||
                (state.currentBook && (state.currentBook.title || state.currentBook.id)) ||
                (state.currentBook && state.currentBook.id) ||
                'book';
              let statusMessage = '';
              if (scopeResult === 'token') {
                statusMessage = updated
                  ? `Updated token in ${chapterLabel}.`
                  : `No changes applied to token in ${chapterLabel}.`;
              } else if (scopeResult === 'chapter') {
                if (updated) {
                  statusMessage = `Refined current chapter (${chapterLabel}).`;
                } else {
                  statusMessage = `No changes required for ${chapterLabel}.`;
                }
              } else {
                if (updated) {
                  statusMessage = `Refined ${updated} chapter(s) in ${bookLabel}.`;
                } else {
                  statusMessage = `No changes required in ${bookLabel}.`;
                }
              }
              renderStatus(statusMessage);
              closeRefineModal();
              if (state.selectedPath) {
                openChapter(state.selectedPath, { autoCollapse: false, preserveScroll: true });
              }
            })
            .catch((error) => {
              setRefineError(error.message || 'Failed to apply override.');
            })
            .finally(() => {
            setRefineBusy(false);
          });
      }

      function removeCurrentToken() {
        if (!state.selectedPath) {
          setRefineError('Select a chapter before removing tokens.');
          return;
        }
        if (!refineContext || typeof refineContext.index !== 'number') {
          setRefineError('Select a token to remove.');
          return;
        }
        const tokenIndex = refineContext.index;
        const tokenLabel =
          (refineContext.token && (refineContext.token.surface || refineContext.token.reading))
          || refineContext.text
          || `token #${tokenIndex}`;
        const confirmed = window.confirm(`Remove ${tokenLabel}? This restores the transformed text to its original surface.`);
        if (!confirmed) {
          return;
        }
        if (refineDeleteToken) {
          refineDeleteToken.textContent = 'Removing token…';
        }
        refineCurrentScope = 'token';
        setRefineBusy(true);
        setRefineError('');
        fetchJSON('/api/remove-token', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: state.selectedPath, token_index: tokenIndex }),
        })
          .then((result) => {
            const chapterLabel = result && result.chapter ? result.chapter : state.selectedPath;
            const removed = result && typeof result.removed === 'boolean' ? result.removed : true;
            const statusMessage = removed
              ? `Removed token #${tokenIndex} from ${chapterLabel}.`
              : `No token removed in ${chapterLabel}.`;
            renderStatus(statusMessage);
            closeRefineModal();
            if (state.selectedPath) {
              openChapter(state.selectedPath, { autoCollapse: false, preserveScroll: true });
            }
          })
          .catch((error) => {
            setRefineError(error.message || 'Failed to remove token.');
          })
          .finally(() => {
            if (refineDeleteToken) {
              refineDeleteToken.textContent = 'Remove token';
            }
            setRefineBusy(false);
          });
      }

      if (refineForm) {
        refineForm.addEventListener('submit', (event) => {
          event.preventDefault();
          submitRefine('book');
        });
      }
      if (refineScopeButtons.length) {
        refineScopeButtons.forEach((button) => {
          button.addEventListener('click', (event) => {
            event.preventDefault();
            const scopeValue = (button.dataset.scope || 'book').toLowerCase();
            submitRefine(scopeValue);
          });
        });
      }
      if (refineDeleteToken) {
        refineDeleteToken.addEventListener('click', (event) => {
          event.preventDefault();
          removeCurrentToken();
        });
      }
      if (overridesOpenBtn) {
        overridesOpenBtn.addEventListener('click', (event) => {
          event.preventDefault();
          openOverridesModal();
        });
      }
      if (overridesCloseBtn) {
        overridesCloseBtn.addEventListener('click', (event) => {
          event.preventDefault();
          closeOverridesModal();
        });
      }
      if (overridesModal) {
        overridesModal.addEventListener('click', (event) => {
          if (event.target === overridesModal) {
            closeOverridesModal();
          }
        });
      }
      if (overridesAddBtn) {
        overridesAddBtn.addEventListener('click', (event) => {
          event.preventDefault();
          syncFormToState();
          overridesState.items.push({ pattern: '', regex: false });
          overridesState.selectedIndex = overridesState.items.length - 1;
          overridesState.selectedRemoveIndex = -1;
          renderOverridesList();
          if (overridePatternInput) {
            overridePatternInput.focus();
          }
        });
      }
      if (removeAddBtn) {
        removeAddBtn.addEventListener('click', (event) => {
          event.preventDefault();
          syncFormToState();
          syncRemoveFormToState();
          overridesState.remove.push({ reading: '', surface: '' });
          overridesState.selectedRemoveIndex = overridesState.remove.length - 1;
          overridesState.selectedIndex = overridesState.items.length ? overridesState.selectedIndex : -1;
          renderOverridesList();
          renderRemoveList();
          if (removeReadingInput) {
            removeReadingInput.focus();
          }
        });
      }
      if (overrideDeleteBtn) {
        overrideDeleteBtn.addEventListener('click', (event) => {
          event.preventDefault();
          if (overridesState.selectedIndex < 0) return;
          syncFormToState();
          overridesState.items.splice(overridesState.selectedIndex, 1);
          overridesState.selectedIndex = overridesState.items.length ? 0 : -1;
          renderOverridesList();
        });
      }
      if (removeDeleteBtn) {
        removeDeleteBtn.addEventListener('click', (event) => {
          event.preventDefault();
          if (overridesState.selectedRemoveIndex < 0) return;
          syncRemoveFormToState();
          overridesState.remove.splice(overridesState.selectedRemoveIndex, 1);
          overridesState.selectedRemoveIndex = overridesState.remove.length ? 0 : -1;
          renderRemoveList();
        });
      }
      if (overridesForm) {
        overridesForm.addEventListener('submit', (event) => {
          event.preventDefault();
          saveOverrides();
        });
      }
      function setOverridesError(message) {
        if (overridesError) {
          overridesError.textContent = message || '';
        }
      }

      function closeOverridesModal() {
        if (!overridesModal) return;
        overridesModal.classList.add('hidden');
        overridesModal.setAttribute('aria-hidden', 'true');
        document.body.classList.remove('modal-open');
        overridesState.selectedIndex = -1;
        overridesState.selectedRemoveIndex = -1;
        setOverridesError('');
      }

      function populateOverrideForm(entry) {
        if (!entry) {
          if (overridePatternInput) overridePatternInput.value = '';
          if (overrideReplacementInput) overrideReplacementInput.value = '';
          if (overrideReadingInput) overrideReadingInput.value = '';
          if (overrideSurfaceInput) overrideSurfaceInput.value = '';
          if (overrideMatchSurfaceInput) overrideMatchSurfaceInput.value = '';
          if (overridePosInput) overridePosInput.value = '';
          if (overrideAccentInput) overrideAccentInput.value = '';
          if (overrideRegexInput) overrideRegexInput.checked = false;
          return;
        }
        if (overridePatternInput) overridePatternInput.value = entry.pattern || '';
        if (overrideReplacementInput) overrideReplacementInput.value = entry.replacement || '';
        if (overrideReadingInput) overrideReadingInput.value = entry.reading || '';
        if (overrideSurfaceInput) overrideSurfaceInput.value = entry.surface || '';
        if (overrideMatchSurfaceInput) overrideMatchSurfaceInput.value = entry.match_surface || '';
        if (overridePosInput) overridePosInput.value = entry.pos || '';
        if (overrideAccentInput) {
          overrideAccentInput.value = Number.isFinite(entry.accent) ? String(entry.accent) : '';
        }
        if (overrideRegexInput) overrideRegexInput.checked = Boolean(entry.regex);
      }

      function syncFormToState() {
        if (overridesState.selectedIndex < 0 || overridesState.selectedIndex >= overridesState.items.length) {
          return;
        }
        const target = overridesState.items[overridesState.selectedIndex];
        target.pattern = overridePatternInput ? overridePatternInput.value.trim() : '';
        target.replacement = overrideReplacementInput ? overrideReplacementInput.value.trim() : '';
        target.reading = overrideReadingInput ? overrideReadingInput.value.trim() : '';
        target.surface = overrideSurfaceInput ? overrideSurfaceInput.value.trim() : '';
        target.match_surface = overrideMatchSurfaceInput ? overrideMatchSurfaceInput.value.trim() : '';
        target.pos = overridePosInput ? overridePosInput.value.trim() : '';
        if (overrideAccentInput) {
          const parsed = Number.parseInt(overrideAccentInput.value, 10);
          target.accent = Number.isFinite(parsed) ? parsed : undefined;
        }
        target.regex = overrideRegexInput ? Boolean(overrideRegexInput.checked) : false;
      }

      function populateRemoveForm(entry) {
        if (!entry) {
          if (removeReadingInput) removeReadingInput.value = '';
          if (removeSurfaceInput) removeSurfaceInput.value = '';
          return;
        }
        if (removeReadingInput) removeReadingInput.value = entry.reading || '';
        if (removeSurfaceInput) removeSurfaceInput.value = entry.surface || '';
      }

      function syncRemoveFormToState() {
        if (
          overridesState.selectedRemoveIndex < 0
          || overridesState.selectedRemoveIndex >= overridesState.remove.length
        ) {
          return;
        }
        const target = overridesState.remove[overridesState.selectedRemoveIndex];
        target.reading = removeReadingInput ? removeReadingInput.value.trim() : '';
        target.surface = removeSurfaceInput ? removeSurfaceInput.value.trim() : '';
      }

      function renderOverridesList() {
        if (!overridesList) return;
        overridesList.innerHTML = '';
        if (!overridesState.items.length) {
          const empty = document.createElement('div');
          empty.className = 'overrides-empty';
          empty.textContent = 'No overrides yet.';
          overridesList.appendChild(empty);
          populateOverrideForm(null);
          return;
        }
        overridesState.items.forEach((entry, index) => {
          const button = document.createElement('button');
          button.type = 'button';
          button.className = 'overrides-item' + (index === overridesState.selectedIndex ? ' active' : '');
          const title = document.createElement('strong');
          title.textContent = entry.pattern || '(no pattern)';
          const meta = document.createElement('div');
          meta.className = 'meta';
          if (entry.replacement) {
            meta.appendChild(document.createTextNode(`→ ${entry.replacement}`));
          }
          if (entry.reading) {
            const span = document.createElement('span');
            span.textContent = `reading: ${entry.reading}`;
            meta.appendChild(span);
          }
          if (entry.surface) {
            const span = document.createElement('span');
            span.textContent = `surface: ${entry.surface}`;
            meta.appendChild(span);
          }
          if (entry.pos) {
            const span = document.createElement('span');
            span.textContent = entry.pos;
            meta.appendChild(span);
          }
          if (entry.regex) {
            const span = document.createElement('span');
            span.textContent = 'regex';
            meta.appendChild(span);
          }
          button.appendChild(title);
          button.appendChild(meta);
          button.addEventListener('click', () => {
            syncFormToState();
            overridesState.selectedIndex = index;
            populateOverrideForm(entry);
            renderOverridesList();
          });
          overridesList.appendChild(button);
        });
        if (overridesState.selectedIndex >= 0 && overridesState.selectedIndex < overridesState.items.length) {
          populateOverrideForm(overridesState.items[overridesState.selectedIndex]);
        } else {
          populateOverrideForm(null);
        }
        renderRemoveList();
      }

      function renderRemoveList() {
        if (!removeList) return;
        removeList.innerHTML = '';
        if (!overridesState.remove.length) {
          const empty = document.createElement('div');
          empty.className = 'overrides-empty';
          empty.textContent = 'No removal rules yet.';
          removeList.appendChild(empty);
          populateRemoveForm(null);
          return;
        }
        overridesState.remove.forEach((entry, index) => {
          const button = document.createElement('button');
          button.type = 'button';
          button.className = 'overrides-item' + (index === overridesState.selectedRemoveIndex ? ' active' : '');
          const title = document.createElement('strong');
          title.textContent = entry.surface || entry.reading || '(no rule)';
          const meta = document.createElement('div');
          meta.className = 'meta';
          if (entry.reading) {
            const span = document.createElement('span');
            span.textContent = `reading: ${entry.reading}`;
            meta.appendChild(span);
          }
          if (entry.surface) {
            const span = document.createElement('span');
            span.textContent = `surface: ${entry.surface}`;
            meta.appendChild(span);
          }
          button.appendChild(title);
          button.appendChild(meta);
          button.addEventListener('click', () => {
            syncFormToState();
            syncRemoveFormToState();
            overridesState.selectedRemoveIndex = index;
            populateRemoveForm(entry);
            renderRemoveList();
          });
          removeList.appendChild(button);
        });
        if (
          overridesState.selectedRemoveIndex >= 0
          && overridesState.selectedRemoveIndex < overridesState.remove.length
        ) {
          populateRemoveForm(overridesState.remove[overridesState.selectedRemoveIndex]);
        } else {
          populateRemoveForm(null);
        }
      }

      function openOverridesModal() {
        const bookId = currentBookId();
        if (!bookId) {
          renderStatus('Select a chapter to edit overrides.');
          return;
        }
        overridesState.bookId = bookId;
        setOverridesError('');
        if (overridesMeta) {
          overridesMeta.textContent = `custom_token.json for ${bookId}`;
        }
        fetchJSON(`/api/books/${encodeURIComponent(bookId)}/overrides`)
          .then((payload) => {
            overridesState.items = Array.isArray(payload.overrides)
              ? payload.overrides.map(entry => ({ ...entry }))
              : [];
            overridesState.remove = Array.isArray(payload.remove)
              ? payload.remove.map(entry => ({ ...entry }))
              : [];
            overridesState.selectedIndex = overridesState.items.length ? 0 : -1;
            overridesState.selectedRemoveIndex = overridesState.remove.length ? 0 : -1;
            renderOverridesList();
            renderRemoveList();
            if (!overridesModal) return;
            overridesModal.classList.remove('hidden');
            overridesModal.setAttribute('aria-hidden', 'false');
            document.body.classList.add('modal-open');
            if (overridePatternInput) {
              overridePatternInput.focus();
            }
          })
          .catch((err) => {
            setOverridesError(err.message || 'Failed to load overrides.');
          });
      }

      function saveOverrides() {
        if (!overridesState.bookId) return;
        syncFormToState();
        syncRemoveFormToState();
        const normalized = overridesState.items.map(entry => ({
          pattern: (entry.pattern || '').trim(),
          replacement: (entry.replacement || '').trim(),
          reading: (entry.reading || '').trim(),
          surface: (entry.surface || '').trim(),
          match_surface: (entry.match_surface || '').trim(),
          pos: (entry.pos || '').trim(),
          accent: Number.isFinite(entry.accent) ? entry.accent : undefined,
          regex: Boolean(entry.regex),
        })).map(entry => {
          const clean = { pattern: entry.pattern, regex: entry.regex };
          if (entry.replacement) clean.replacement = entry.replacement;
          if (entry.reading) clean.reading = entry.reading;
          if (entry.surface) clean.surface = entry.surface;
          if (entry.match_surface) clean.match_surface = entry.match_surface;
          if (entry.pos) clean.pos = entry.pos;
          if (entry.accent !== undefined) clean.accent = entry.accent;
          return clean;
        }).filter(entry => entry.pattern);
        const normalizedRemove = overridesState.remove.map(entry => ({
          reading: (entry.reading || '').trim(),
          surface: (entry.surface || '').trim(),
        })).map(entry => {
          const clean = {};
          if (entry.reading) clean.reading = entry.reading;
          if (entry.surface) clean.surface = entry.surface;
          return clean;
        }).filter(entry => Object.keys(entry).length > 0);
        setOverridesError('');
        overrideSaveBtn.disabled = true;
        fetchJSON(`/api/books/${encodeURIComponent(overridesState.bookId)}/overrides`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ overrides: normalized, remove: normalizedRemove }),
        })
          .then((payload) => {
            overridesState.items = Array.isArray(payload.overrides)
              ? payload.overrides.map(entry => ({ ...entry }))
              : [];
            overridesState.remove = Array.isArray(payload.remove)
              ? payload.remove.map(entry => ({ ...entry }))
              : [];
            overridesState.selectedIndex = overridesState.items.length ? 0 : -1;
            overridesState.selectedRemoveIndex = overridesState.remove.length ? 0 : -1;
            renderOverridesList();
            renderRemoveList();
            closeOverridesModal();
            renderStatus('Overrides saved.');
          })
          .catch((err) => {
            setOverridesError(err.message || 'Failed to save overrides.');
          })
          .finally(() => {
            overrideSaveBtn.disabled = false;
          });
      }
      if (homeLink) {
        homeLink.addEventListener('click', (event) => {
          event.preventDefault();
          if (state.selectedPath) {
            state.selectedPath = null;
            renderChapterList();
            clearSelection();
          }
          updateChapterHash(null);
          renderStatus('Select a chapter to preview.');
        });
      }

      function updateSidebarToggleLabel(collapsed) {
        if (!sidebarToggle) return;
        sidebarToggle.setAttribute('aria-expanded', String(!collapsed));
        sidebarToggle.textContent = collapsed ? 'Show chapters' : 'Hide chapters';
      }

      function setSidebarCollapsed(collapsed) {
        bodyEl.classList.toggle('sidebar-collapsed', collapsed);
        updateSidebarToggleLabel(collapsed);
      }

      function handleSidebarMediaChange(event) {
        if (event.matches) {
          setSidebarCollapsed(true);
        } else {
          setSidebarCollapsed(false);
        }
      }

      function setDocumentTitle(label) {
        document.title = label ? `${label} – ${baseTitle}` : baseTitle;
      }

      function chapterLabelFromPath(path) {
        if (!path) {
          return '';
        }
        const match = state.chapters.find((chapter) => chapter.path === path);
        if (match && typeof match.name === 'string' && match.name.trim()) {
          return match.name;
        }
        return path;
      }

      function setChapterNavLabel(label) {
        const normalized = label && String(label).trim() ? String(label).trim() : CHAPTER_NAV_DEFAULT;
        chapterNavStatusNodes.forEach((node) => {
          node.textContent = normalized;
        });
      }

      function adjacentChapterPath(direction) {
        if (!state.chapters.length || !state.selectedPath || !direction) {
          return null;
        }
        const step = direction < 0 ? -1 : 1;
        const currentIndex = state.chapters.findIndex((chapter) => chapter.path === state.selectedPath);
        if (currentIndex === -1) {
          return null;
        }
        for (
          let idx = currentIndex + step;
          idx >= 0 && idx < state.chapters.length;
          idx += step
        ) {
          const candidate = state.chapters[idx];
          if (candidate && typeof candidate.path === 'string') {
            return candidate.path;
          }
        }
        return null;
      }

      function updateChapterNavButtons() {
        const hasPrev = Boolean(adjacentChapterPath(-1));
        const hasNext = Boolean(adjacentChapterPath(1));
        prevChapterButtons.forEach((button) => {
          button.disabled = !hasPrev;
        });
        nextChapterButtons.forEach((button) => {
          button.disabled = !hasNext;
        });
        if (!state.selectedPath) {
          setChapterNavLabel('');
        }
      }

      function navigateAdjacentChapter(direction) {
        const targetPath = adjacentChapterPath(direction);
        if (!targetPath) {
          return;
        }
        openChapter(targetPath, { autoCollapse: false });
      }

      setChapterNavLabel('');
      updateChapterNavButtons();

      function renderStatus(text) {
        statusEl.textContent = text;
      }

      function setRefineError(message) {
        if (refineError) {
          refineError.textContent = message || '';
        }
      }

      function canRemoveCurrentToken() {
        return (
          !!refineContext
          && typeof refineContext.index === 'number'
          && Number.isFinite(refineContext.index)
        );
      }

      function updateRefineButtons() {
        if (refineDeleteToken) {
          refineDeleteToken.disabled = refineBusy || !canRemoveCurrentToken();
        }
      }

      function setRefineBusy(busy) {
        refineBusy = busy;
        if (!refineForm) {
          updateRefineButtons();
          return;
        }
        const showProgress = busy && (refineCurrentScope === 'book' || refineCurrentScope === 'chapter');
        if (refineProgress) {
          refineProgress.classList.toggle('hidden', !showProgress);
        }
        if (refineProgressLabel) {
          const label =
            refineCurrentScope === 'book'
              ? 'Refining whole book…'
              : 'Refining chapter…';
          refineProgressLabel.textContent = label;
        }
        const controls = refineForm.querySelectorAll('input, button, textarea');
        controls.forEach((control) => {
          if (control === refineCancel) {
            control.disabled = false;
          } else {
            control.disabled = busy;
          }
        });
        if (refineSubmitBook) {
          refineSubmitBook.textContent = busy ? 'Applying to whole book…' : 'The whole book';
        }
        if (refineSubmitChapter) {
          refineSubmitChapter.textContent = busy ? 'Applying to chapter…' : 'This chapter only';
        }
        if (refineSubmitToken) {
          refineSubmitToken.textContent = busy ? 'Saving token…' : 'Save token';
        }
        updateRefineButtons();
      }

      function closeRefineModal() {
        if (!refineModal) return;
        refineContext = null;
        refineModal.classList.add('hidden');
        bodyEl.classList.remove('modal-open');
        setRefineError('');
        setRefineBusy(false);
      }

      function openRefineModal(context) {
        if (!refineModal || !refineForm) return;
        if (!state.selectedPath) {
          renderStatus('Open a chapter before refining tokens.');
          return;
        }
        const token = (context && context.token) || {};
        const chunk = typeof (context && context.text) === 'string' ? context.text : '';
        const view = context && context.view ? context.view : 'transformed';
        const selection = context && context.selection ? context.selection : null;
        const tokenIndex =
          context && typeof context.index === 'number' && Number.isFinite(context.index)
            ? context.index
            : null;
        const surfaceLabel = token.surface || chunk || '';
        const originalTextValue =
          state.chapterPayload && typeof state.chapterPayload.original_text === 'string'
            ? state.chapterPayload.original_text
            : null;
        const isSelectionToken =
          token && Array.isArray(token.sources) && token.sources.includes('selection');

        const sliceOriginalSurface = (candidateToken, allowFallbackSurface = true) => {
          if (!candidateToken) return '';
          const startOriginal = offsetValue(candidateToken.start, 'original');
          const endOriginal = offsetValue(candidateToken.end, 'original');
          if (
            originalTextValue
            && Number.isFinite(startOriginal)
            && Number.isFinite(endOriginal)
            && endOriginal > startOriginal
          ) {
            return originalTextValue.slice(startOriginal, endOriginal);
          }
          if (allowFallbackSurface) {
            return candidateToken.surface || '';
          }
          return '';
        };

        const selectionMatchSurface = (() => {
          const tokenSurface = sliceOriginalSurface(token, !isSelectionToken);
          if (tokenSurface) return tokenSurface;
          if (
            selection
            && typeof selection.start === 'number'
            && typeof selection.end === 'number'
            && Array.isArray(state.tokens)
          ) {
            const surfaces = state.tokens
              .map((entry) => {
                const tStart = offsetValue(entry.start, 'transformed');
                const tEnd = offsetValue(entry.end, 'transformed');
                if (!Number.isFinite(tStart) || !Number.isFinite(tEnd) || tEnd <= tStart) {
                  return '';
                }
                const overlaps = tEnd > selection.start && selection.end > tStart;
                return overlaps ? sliceOriginalSurface(entry, true) : '';
              })
              .filter(Boolean);
            if (surfaces.length) {
              return surfaces.join('');
            }
          }
          return '';
        })();

        const readingLabel = token.reading || '';
        const defaultPattern =
          view === 'transformed' && chunk
            ? chunk
            : (readingLabel || surfaceLabel);
        const defaultReplacement = chunk || '';
        const defaultReading = token.reading || chunk || '';
        const accentValue =
          typeof token.accent === 'number' && Number.isFinite(token.accent)
            ? String(token.accent)
            : '';
        if (refinePatternInput) {
          refinePatternInput.value = defaultPattern || '';
        }
        if (refineReplacementInput) {
          refineReplacementInput.value = defaultReplacement;
        }
        if (refineReadingInput) {
          refineReadingInput.value = defaultReading;
        }
        if (refineAccentInput) {
          refineAccentInput.value = accentValue;
        }
        if (refineSurfaceInput) {
          const defaultSurface = selectionMatchSurface || token.surface || '';
          refineSurfaceInput.value = defaultSurface;
        }
        if (refineMatchSurfaceInput) {
          refineMatchSurfaceInput.value = selectionMatchSurface || '';
        }
        if (refinePosInput) {
          refinePosInput.value = token.pos || '';
        }
        if (refineRegexInput) {
          refineRegexInput.checked = Boolean(context && context.regex);
        }
        if (refineContextLabel) {
          const parts = [];
          if (state.selectedPath) {
            parts.push(state.selectedPath);
          }
          if (selection && typeof selection.start === 'number' && typeof selection.end === 'number') {
            parts.push(
              `${surfaceLabel || chunk || '—'} → ${readingLabel || chunk || '—'} @ ${selection.start}–${selection.end}`
            );
          } else if (surfaceLabel || readingLabel) {
            parts.push(`${surfaceLabel || '—'} → ${readingLabel || '—'}`);
          }
          if (tokenIndex !== null) {
            parts.push(`Token #${tokenIndex}`);
          }
          refineContextLabel.textContent = parts.join(' · ');
        }
        if (refineMeta) {
          const metaBits = [];
          const sources = Array.isArray(token.sources) ? token.sources : [];
          const sourceLabel = sources.length ? sources.join(', ') : 'unknown';
          metaBits.push({ label: 'Source', value: sourceLabel });
          refineMeta.innerHTML = '';
          metaBits.forEach((bit) => {
            const chip = document.createElement('span');
            chip.className = 'refine-chip';
            chip.textContent = `${bit.label}: ${bit.value}`;
            refineMeta.appendChild(chip);
          });
        }
        refineContext = { token, view, text: chunk, index: tokenIndex };
        if (selection) {
          refineContext.selection = selection;
        }
        setRefineError('');
        setRefineBusy(false);
        refineModal.classList.remove('hidden');
        bodyEl.classList.add('modal-open');
        if (refinePatternInput) {
          refinePatternInput.focus();
          refinePatternInput.select();
        }
      }

      function fetchJSON(url, options = {}) {
        return fetch(url, options).then(async (res) => {
          if (!res.ok) {
            let detail = '';
            try {
              detail = await res.text();
            } catch (error) {
              detail = '';
            }
            throw new Error(detail || `Request failed: ${res.status}`);
          }
          if (res.status === 204) {
            return null;
          }
          return res.json();
        });
      }

      function formatBytes(bytes) {
        if (!Number.isFinite(bytes)) return '—';
        if (bytes < 1024) return `${bytes} B`;
        if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
        return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
      }

      function formatNumber(value) {
        if (!Number.isFinite(value)) return '—';
        return value.toLocaleString();
      }

      function formatPercent(value) {
        if (!Number.isFinite(value)) return '—';
        return `${(value * 100).toFixed(1)}%`;
      }

      function formatDate(value) {
        if (!value) return '—';
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) return value;
        return date.toLocaleString();
      }

      function isFolderExpanded(path) {
        if (!path) {
          return true;
        }
        if (Object.prototype.hasOwnProperty.call(state.folderState, path)) {
          return Boolean(state.folderState[path]);
        }
        return false;
      }

      function setFolderExpanded(path, expanded) {
        if (!path) {
          return;
        }
        state.folderState[path] = Boolean(expanded);
      }

      function expandFoldersForPath(path) {
        if (!path) {
          return;
        }
        const parts = path.split('/');
        if (parts.length <= 1) {
          return;
        }
        let cursor = '';
        for (let i = 0; i < parts.length - 1; i += 1) {
          const part = parts[i];
          if (!part) {
            continue;
          }
          cursor = cursor ? `${cursor}/${part}` : part;
          setFolderExpanded(cursor, true);
        }
      }

      function setLineRegistry(key, lines) {
        if (!lineRegistry[key]) {
          lineRegistry[key] = [];
        }
        lineRegistry[key] = Array.isArray(lines) ? lines : [];
      }

      function clearLineHeights() {
        Object.values(lineRegistry).forEach((lines) => {
          lines.forEach((line) => {
            line.style.minHeight = '';
          });
        });
      }

      function alignTextLines() {
        if (!transformedPanel || !originalPanel) return;
        clearLineHeights();
        const transformedVisible = !transformedPanel.classList.contains('hidden');
        const originalVisible = !originalPanel.classList.contains('hidden');
        if (!(transformedVisible && originalVisible)) {
          return;
        }
        const maxLines = Math.max(lineRegistry.transformed.length, lineRegistry.original.length);
        for (let i = 0; i < maxLines; i += 1) {
          const nodes = [];
          const tLine = lineRegistry.transformed[i];
          const oLine = lineRegistry.original[i];
          if (tLine) nodes.push(tLine);
          if (oLine) nodes.push(oLine);
          if (nodes.length < 2) continue;
          const maxHeight = Math.max(...nodes.map((node) => node.offsetHeight || 0));
          nodes.forEach((node) => {
            node.style.minHeight = `${maxHeight}px`;
          });
        }
      }

      function scheduleAlignLines() {
        if (alignFrame !== null) {
          cancelAnimationFrame(alignFrame);
        }
        alignFrame = requestAnimationFrame(() => {
          alignFrame = null;
          alignTextLines();
        });
      }

      function updatePanelVisibility(panel, toggle) {
        if (!panel || !toggle) return;
        panel.classList.toggle('hidden', !toggle.checked);
        updateTextGridLayout();
        scheduleAlignLines();
      }

      function updateTextGridLayout() {
        if (!textGrid) return;
        const visiblePanels = [transformedPanel, originalPanel].filter(
          (panel) => panel && !panel.classList.contains('hidden')
        ).length;
        textGrid.classList.toggle('single-column', visiblePanels <= 1);
      }

      function bindPanelToggle(panel, toggle, onEnable) {
        if (!panel || !toggle) return;
        updatePanelVisibility(panel, toggle);
        toggle.addEventListener('change', () => {
          updatePanelVisibility(panel, toggle);
          if (toggle.checked && typeof onEnable === 'function') {
            onEnable();
          }
        });
      }

      function resetScrollPositions() {
        if (transformedText) transformedText.scrollTop = 0;
        if (originalText) originalText.scrollTop = 0;
      }

      function snapshotScrollPositions() {
        return {
          transformed: transformedText ? transformedText.scrollTop : 0,
          original: originalText ? originalText.scrollTop : 0,
        };
      }

      function restoreScrollPositions(snapshot) {
        if (!snapshot) {
          return;
        }
        if (originalText && typeof snapshot.original === 'number') {
          originalText.scrollTop = snapshot.original;
        }
        if (transformedText && typeof snapshot.transformed === 'number') {
          transformedText.scrollTop = snapshot.transformed;
        }
      }

      prevChapterButtons.forEach((button) => {
        button.addEventListener('click', () => navigateAdjacentChapter(-1));
      });
      nextChapterButtons.forEach((button) => {
        button.addEventListener('click', () => navigateAdjacentChapter(1));
      });

      let scrollSyncLock = false;
      function syncScroll(source, target) {
        if (!source || !target) return;
        if (scrollSyncLock) return;
        scrollSyncLock = true;
        target.scrollTop = source.scrollTop;
        scrollSyncLock = false;
      }

      bindPanelToggle(transformedPanel, toggleTransformed, () => {
        requestTransformedLoad();
      });
      bindPanelToggle(originalPanel, toggleOriginal);

      if (sidebarToggle) {
        sidebarToggle.addEventListener('click', () => {
          const collapsed = bodyEl.classList.contains('sidebar-collapsed');
          setSidebarCollapsed(!collapsed);
        });
      }
      if (typeof mobileQuery.addEventListener === 'function') {
        mobileQuery.addEventListener('change', handleSidebarMediaChange);
      } else if (typeof mobileQuery.addListener === 'function') {
        mobileQuery.addListener(handleSidebarMediaChange);
      }
      setSidebarCollapsed(mobileQuery.matches);

      if (transformedText && originalText) {
        transformedText.addEventListener('scroll', () => syncScroll(transformedText, originalText));
        originalText.addEventListener('scroll', () => syncScroll(originalText, transformedText));
      }
      window.addEventListener('resize', () => scheduleAlignLines());

      function setHighlighted(index) {
        const target = index === null || index === undefined ? null : String(index);
        state.activeToken = target;
        document.querySelectorAll('[data-token-index]').forEach((node) => {
          if (!(node instanceof HTMLElement)) {
            return;
          }
          if (target !== null && node.dataset.tokenIndex === target) {
            node.classList.add('active');
          } else {
            node.classList.remove('active');
          }
        });
      }

      function scrollTokenIntoView(index) {
        if (index === null || index === undefined) return;
        const selector = `[data-token-index="${index}"]`;
        const originalVisible = originalPanel && !originalPanel.classList.contains('hidden');
        const transformedVisible = transformedPanel && !transformedPanel.classList.contains('hidden');
        const preferredContainers = [];
        if (originalVisible && originalText) {
          preferredContainers.push(originalText);
        }
        if (transformedVisible && transformedText && !originalVisible) {
          preferredContainers.push(transformedText);
        }
        let node = null;
        for (const container of preferredContainers) {
          node = container.querySelector(selector);
          if (node) break;
        }
        if (!node) {
          node = document.querySelector(selector);
        }
        if (node && typeof node.scrollIntoView === 'function') {
          node.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
      }

      function hideSelectionToolbar() {
        selectionRange = null;
        selectionViewportRect = null;
        selectionToolbar.classList.add('hidden');
      }

      function attachHighlightHandlers(element, index) {
        element.addEventListener('mouseenter', () => setHighlighted(index));
        element.addEventListener('mouseleave', () => setHighlighted(null));
        element.addEventListener('focus', () => setHighlighted(index));
        element.addEventListener('blur', () => setHighlighted(null));
      }

      function selectionTextAndOffsets() {
        const selection = window.getSelection ? window.getSelection() : null;
        if (!selection || selection.isCollapsed || selection.rangeCount === 0) {
          return null;
        }
        const range = selection.getRangeAt(0);
        const container = transformedText;
        if (!container || !container.contains(range.commonAncestorContainer)) {
          return null;
        }

        const parseOffset = (value) => {
          const num = Number(value);
          return Number.isFinite(num) ? num : null;
        };

        const findChunkBoundary = (node) => {
          let current = node;
          while (current && current !== container) {
            if (current.nodeType === Node.ELEMENT_NODE) {
              const start = parseOffset(current.getAttribute('data-offset-start'));
              const end = parseOffset(current.getAttribute('data-offset-end'));
              if (start !== null && end !== null) {
                return { node: current, start, end };
              }
            }
            current = current.parentNode;
          }
          return null;
        };

        const offsetWithinBoundary = (boundary, targetNode, targetOffset) => {
          if (!boundary) return null;
          let total = 0;
          const walker = document.createTreeWalker(
            boundary.node,
            NodeFilter.SHOW_TEXT,
            {
              acceptNode: (n) => {
                const parent = n.parentElement;
                if (parent && parent.tagName === 'RT') {
                  return NodeFilter.FILTER_REJECT;
                }
                return NodeFilter.FILTER_ACCEPT;
              },
            },
          );
          let current = walker.nextNode();
          while (current) {
            if (current === targetNode) {
              const textLen = current.textContent ? current.textContent.length : 0;
              const delta = Math.max(0, Math.min(textLen, targetOffset));
              return total + delta;
            }
            total += current.textContent ? current.textContent.length : 0;
            current = walker.nextNode();
          }
          return null;
        };

        const startBoundary = findChunkBoundary(range.startContainer);
        const endBoundary = findChunkBoundary(range.endContainer);
        if (!startBoundary || !endBoundary) {
          return null;
        }
        const startDelta = offsetWithinBoundary(startBoundary, range.startContainer, range.startOffset);
        const endDelta = offsetWithinBoundary(endBoundary, range.endContainer, range.endOffset);
        if (startDelta === null || endDelta === null) {
          return null;
        }
        let start = startBoundary.start + startDelta;
        let end = endBoundary.start + endDelta;
        const transformedTextValue =
          state.chapterPayload && typeof state.chapterPayload.text === 'string'
            ? state.chapterPayload.text
            : null;
        if (transformedTextValue) {
          const maxLen = transformedTextValue.length;
          start = Math.max(0, Math.min(start, maxLen));
          end = Math.max(0, Math.min(end, maxLen));
        }
        if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
          return null;
        }
        const selectedText = transformedTextValue
          ? transformedTextValue.slice(start, end)
          : range.toString();
        return {
          range,
          text: selectedText,
          start,
          end,
          rect: range.getBoundingClientRect(),
        };
      }

      function createOffsetConverter(text) {
        if (!text) {
          return () => null;
        }
        let hasSurrogates = false;
        for (let i = 0; i < text.length; i += 1) {
          const code = text.charCodeAt(i);
          if (code >= 0xD800 && code <= 0xDBFF) {
            hasSurrogates = true;
            break;
          }
        }
        if (!hasSurrogates) {
          return (value) => (typeof value === 'number' ? value : null);
        }
        const offsets = [0];
        let codeUnitIndex = 0;
        for (const char of text) {
          codeUnitIndex += char.length;
          offsets.push(codeUnitIndex);
        }
        const maxIndex = offsets.length - 1;
        return (value) => {
          if (typeof value !== 'number' || !Number.isFinite(value)) {
            return null;
          }
          if (value <= 0) {
            return 0;
          }
          if (value >= maxIndex) {
            return offsets[maxIndex];
          }
          return offsets[value];
        };
      }

      function offsetValue(entry, key) {
        if (typeof entry === 'number') {
          return entry;
        }
        if (entry && typeof entry === 'object') {
          const value = entry[key];
          if (typeof value === 'number') {
            return value;
          }
          if (key === 'transformed' && typeof entry.transformed === 'number') {
            return entry.transformed;
          }
        }
        return null;
      }

      function renderTextView(container, text, tokens, key) {
        container.innerHTML = '';
        if (!text) {
          container.classList.add('empty');
          container.textContent = key === 'original' ? 'Original text unavailable.' : 'No text available.';
          return [];
        }
        container.classList.remove('empty');
        const length = text.length;
        const convertOffset = createOffsetConverter(text);
        let cursor = 0;
        const segments = [];
        const ordered = tokens
          .map((token, index) => ({
            token,
            index,
            start: convertOffset(offsetValue(token.start, key)),
            end: convertOffset(offsetValue(token.end, key)),
          }))
          .filter((entry) => Number.isFinite(entry.start) && Number.isFinite(entry.end))
          .map((entry) => ({
            ...entry,
            start: Math.max(0, Math.min(length, entry.start)),
            end: Math.max(0, Math.min(length, entry.end)),
          }))
          .filter((entry) => entry.end > entry.start)
          .sort((a, b) => (a.start - b.start) || (a.index - b.index));

        const isOriginalView = key === 'original';

        const pushTextSegment = (value, start) => {
          if (value) {
            segments.push({ type: 'text', text: value, start, end: start + value.length });
          }
        };
        const pushTokenSegment = (value, entry, start) => {
          if (!value) {
            return;
          }
          segments.push({
            type: 'token',
            text: value,
            token: entry.token,
            index: entry.index,
            start,
            end: start + value.length,
          });
        };

        ordered.forEach((entry) => {
          const start = Math.max(entry.start, cursor);
          if (start > cursor) {
            pushTextSegment(text.slice(cursor, start), cursor);
          }
          const end = Math.max(start, entry.end);
          if (end > start) {
            pushTokenSegment(text.slice(start, end), entry, start);
          }
          cursor = Math.max(cursor, entry.end);
        });

        if (cursor < length) {
          pushTextSegment(text.slice(cursor), cursor);
        }

        const linesContainer = document.createElement('div');
        linesContainer.className = 'text-lines';
        const lines = [];
        let currentLineNodes = [];
        let lineCounter = 1;

        const flushLine = () => {
          const lineEl = document.createElement('div');
          lineEl.className = 'text-line';
          const numberEl = document.createElement('div');
          numberEl.className = 'line-number';
          numberEl.textContent = String(lineCounter++);
          const bodyEl = document.createElement('div');
          bodyEl.className = 'line-body';
          if (!currentLineNodes.length) {
            bodyEl.appendChild(document.createTextNode('\u00a0'));
          } else {
            currentLineNodes.forEach((node) => bodyEl.appendChild(node));
          }
          lineEl.appendChild(numberEl);
          lineEl.appendChild(bodyEl);
          linesContainer.appendChild(lineEl);
          lines.push(lineEl);
          currentLineNodes = [];
        };

        const appendSegmentContent = (segment, chunk, chunkStart, chunkEnd) => {
          if (!chunk) {
            return;
          }
          if (segment.type === 'token') {
            if (key === 'transformed') {
              const originalSurface = segment.token.surface || '';
              if (originalSurface && chunk === originalSurface) {
                const textSpan = document.createElement('span');
                textSpan.className = 'text-chunk';
                textSpan.dataset.offsetStart = String(chunkStart);
                textSpan.dataset.offsetEnd = String(chunkEnd);
                textSpan.textContent = chunk;
                currentLineNodes.push(textSpan);
                return;
              }
            }
            const span = document.createElement('span');
            span.className = 'token-chunk';
            span.dataset.tokenIndex = String(segment.index);
            span.title = `${segment.token.surface || ''} → ${segment.token.reading || ''}`;
            span.dataset.offsetStart = String(chunkStart);
            span.dataset.offsetEnd = String(chunkEnd);
            span.tabIndex = 0;
            const annotation = isOriginalView ? (segment.token.reading || '') : (segment.token.surface || '');
            const trimmedAnnotation = annotation && annotation.trim();
            if (trimmedAnnotation) {
              const ruby = document.createElement('ruby');
              const rb = document.createElement('span');
              rb.textContent = chunk;
              rb.setAttribute('data-offset-start', String(chunkStart));
              rb.setAttribute('data-offset-end', String(chunkEnd));
              ruby.appendChild(rb);
              const rt = document.createElement('rt');
              rt.textContent = annotation;
              ruby.appendChild(rt);
              span.appendChild(ruby);
            } else {
              const rb = document.createElement('span');
              rb.textContent = chunk;
              rb.setAttribute('data-offset-start', String(chunkStart));
              rb.setAttribute('data-offset-end', String(chunkEnd));
              span.appendChild(rb);
            }
            attachHighlightHandlers(span, segment.index);
            span.addEventListener('click', (event) => {
              event.preventDefault();
              openRefineModal({
                token: segment.token,
                text: chunk,
                view: isOriginalView ? 'original' : 'transformed',
                index: segment.index,
              });
            });
            span.addEventListener('keydown', (event) => {
              if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                openRefineModal({
                  token: segment.token,
                  text: chunk,
                  view: isOriginalView ? 'original' : 'transformed',
                  index: segment.index,
                });
              }
            });
            currentLineNodes.push(span);
          } else {
            const span = document.createElement('span');
            span.className = 'text-chunk';
            span.dataset.offsetStart = String(chunkStart);
            span.dataset.offsetEnd = String(chunkEnd);
            span.textContent = chunk;
            currentLineNodes.push(span);
          }
        };

        segments.forEach((segment) => {
          let start = 0;
          while (start <= segment.text.length) {
            const newlineIndex = segment.text.indexOf('\\n', start);
            if (newlineIndex === -1) {
              const finalChunk = segment.text.slice(start);
              if (finalChunk) {
                appendSegmentContent(segment, finalChunk, segment.start + start, segment.start + segment.text.length);
              }
              break;
            }
            const chunk = segment.text.slice(start, newlineIndex);
            if (chunk) {
              appendSegmentContent(segment, chunk, segment.start + start, segment.start + newlineIndex);
            }
            flushLine();
            start = newlineIndex + 1;
          }
        });
        flushLine();
        container.appendChild(linesContainer);
        return lines;
      }

      function updateTextMeta(element, textValue, hasFile, loaded = true) {
        if (!hasFile) {
          element.textContent = 'Missing';
          element.className = 'pill missing';
          return;
        }
        if (!loaded) {
          element.textContent = 'Not loaded';
          element.className = 'pill';
          return;
        }
        if (!textValue) {
          element.textContent = 'Empty text';
          element.className = 'pill warn';
          return;
        }
        element.textContent = `${textValue.length.toLocaleString()} chars`;
        element.className = 'pill ok';
      }

      function updateMetaPanel(payload) {
        const chapter = payload.chapter || {};
        const tokenVersion = payload.token_version ?? '—';
        const textLength =
          typeof payload.text_length === 'number'
            ? payload.text_length
            : (typeof payload.text === 'string' ? payload.text.length : null);
        const originalLength =
          typeof payload.original_length === 'number'
            ? payload.original_length
            : (typeof payload.original_text === 'string' ? payload.original_text.length : null);
        const fields = [
          ['Path', chapter.path || '—'],
          ['Book', chapter.book || '—'],
          ['File size', formatBytes(Number(chapter.size))],
          ['Text length', formatNumber(textLength)],
          ['Original length', formatNumber(originalLength)],
          ['Token version', tokenVersion],
          ['Token SHA1', payload.token_sha1 || '—'],
          ['Token file', payload.token_path || 'missing'],
          ['Original file', payload.original_path || 'missing'],
          ['Updated', formatDate(chapter.modified)],
        ];
        metaGrid.innerHTML = '';
        fields.forEach(([label, value]) => {
          const card = document.createElement('dl');
          card.className = 'meta-item';
          const dt = document.createElement('dt');
          dt.textContent = label;
          const dd = document.createElement('dd');
          dd.textContent = value === null || value === undefined ? '—' : String(value);
          card.appendChild(dt);
          card.appendChild(dd);
          metaGrid.appendChild(card);
        });
        metaPanel.hidden = false;
      }

      function computeDiagnostics(tokens) {
        const stats = {
          totalTokens: Array.isArray(tokens) ? tokens.length : 0,
          uniqueSurfaces: 0,
          missingSurface: 0,
          missingReading: 0,
          missingAccent: 0,
          missingSources: 0,
          multiReadingSurfaces: 0,
          flaggedSurfaces: 0,
          flaggedTokens: 0,
        };
        const surfaceMap = new Map();
        tokens.forEach((token, index) => {
          const surfaceRaw = typeof token.surface === 'string' ? token.surface : '';
          const surface = surfaceRaw.trim();
          const readingRaw = typeof token.reading === 'string' ? token.reading : '';
          const reading = readingRaw.trim();
          const tokenSources = Array.isArray(token.sources)
            ? token.sources.filter((src) => typeof src === 'string' && src.trim())
            : [];
          const hasAccent = Number.isFinite(token.accent);
          const hasSource = Array.isArray(token.sources) && token.sources.length > 0;
          if (!surface) {
            stats.missingSurface += 1;
            if (!reading) {
              stats.missingReading += 1;
            }
            if (!hasAccent) {
              stats.missingAccent += 1;
            }
            if (!hasSource) {
              stats.missingSources += 1;
            }
            return;
          }
          if (!reading) {
            stats.missingReading += 1;
          }
          if (!hasAccent) {
            stats.missingAccent += 1;
          }
          if (!hasSource) {
            stats.missingSources += 1;
          }
          let entry = surfaceMap.get(surface);
          if (!entry) {
            entry = {
              surface,
              total: 0,
              readings: new Map(),
              sampleIndices: [],
              sourceCounts: new Map(),
            };
            surfaceMap.set(surface, entry);
          }
          entry.total += 1;
          if (entry.sampleIndices.length < 5) {
            entry.sampleIndices.push(index);
          }
          tokenSources.forEach((source) => {
            const normalizedSource = source.trim();
            if (!normalizedSource) return;
            entry.sourceCounts.set(
              normalizedSource,
              (entry.sourceCounts.get(normalizedSource) || 0) + 1
            );
          });
          const readingKey = reading || '—';
          let readingEntry = entry.readings.get(readingKey);
          if (!readingEntry) {
            readingEntry = { reading: readingKey, count: 0, indices: [], sourceCounts: new Map() };
            entry.readings.set(readingKey, readingEntry);
          }
          readingEntry.count += 1;
          if (readingEntry.indices.length < 3) {
            readingEntry.indices.push(index);
          }
          tokenSources.forEach((source) => {
            const normalizedSource = source.trim();
            if (!normalizedSource) return;
            readingEntry.sourceCounts.set(
              normalizedSource,
              (readingEntry.sourceCounts.get(normalizedSource) || 0) + 1
            );
          });
        });
        const surfaces = Array.from(surfaceMap.values());
        stats.uniqueSurfaces = surfaces.length;
        const conflicts = surfaces
          .filter((entry) => entry.readings.size > 1)
          .map((entry) => {
            const readings = Array.from(entry.readings.values())
              .map((item) => ({
                reading: item.reading,
                count: item.count,
                share: entry.total ? item.count / entry.total : 0,
                indices: item.indices,
                sources: Array.from(item.sourceCounts.entries())
                  .map(([name, count]) => ({ name, count }))
                  .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name)),
              }))
              .sort(
                (a, b) =>
                  b.count - a.count
                  || a.reading.localeCompare(b.reading, 'ja', { numeric: true, sensitivity: 'base' })
              );
            const primary = readings[0] || { share: 0 };
            const primaryShare = primary.share;
            const flagged = primaryShare < 0.9 || readings.length >= 3;
            const score = (1 - primaryShare) * entry.total;
            const sources = Array.from(entry.sourceCounts.entries())
              .map(([name, count]) => ({ name, count }))
              .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
            return {
              surface: entry.surface,
              total: entry.total,
              readings,
              primaryShare,
              flagged,
              sampleIndices: entry.sampleIndices,
              score,
              sources,
            };
          })
          .sort(
            (a, b) =>
              Number(b.flagged) - Number(a.flagged)
              || b.score - a.score
              || b.total - a.total
              || a.surface.localeCompare(b.surface, 'ja', { numeric: true, sensitivity: 'base' })
          );
        stats.multiReadingSurfaces = conflicts.length;
        stats.flaggedSurfaces = conflicts.filter((entry) => entry.flagged).length;
        stats.flaggedTokens = conflicts
          .filter((entry) => entry.flagged)
          .reduce((sum, entry) => sum + entry.total, 0);
        return { stats, conflicts };
      }

      function clearDiagnostics() {
        if (diagnosticSummary) {
          diagnosticSummary.innerHTML = '<div class="diagnostic-empty">Load a chapter to view diagnostics.</div>';
        }
        if (diagnosticConflicts) {
          diagnosticConflicts.innerHTML = '<div class="diagnostic-empty">No data yet.</div>';
        }
        if (diagnosticsPanel) {
          diagnosticsPanel.hidden = true;
        }
      }

      function renderDiagnostics(tokens) {
        if (!diagnosticSummary || !diagnosticConflicts || !diagnosticsPanel) {
          return;
        }
        if (!Array.isArray(tokens) || !tokens.length) {
          diagnosticSummary.innerHTML = '<div class="diagnostic-empty">No token data.</div>';
          diagnosticConflicts.innerHTML = '<div class="diagnostic-empty">No ambiguous readings detected.</div>';
          diagnosticsPanel.hidden = false;
          return;
        }
        const { stats, conflicts } = computeDiagnostics(tokens);
        const summaryItems = [
          { label: 'Tokens', value: stats.totalTokens },
          { label: 'Unique surfaces', value: stats.uniqueSurfaces },
          ...(stats.missingSurface
            ? [{ label: 'Missing surfaces', value: stats.missingSurface, tone: 'warn' }]
            : []),
          { label: 'Missing readings', value: stats.missingReading, tone: stats.missingReading ? 'warn' : '' },
          { label: 'Missing accent', value: stats.missingAccent, tone: stats.missingAccent ? 'warn' : '' },
          { label: 'Missing sources', value: stats.missingSources, tone: stats.missingSources ? 'warn' : '' },
          { label: 'Multi-reading surfaces', value: stats.multiReadingSurfaces },
          { label: 'Likely conflict surfaces', value: stats.flaggedSurfaces, tone: stats.flaggedSurfaces ? 'warn' : '' },
          { label: 'Tokens needing review', value: stats.flaggedTokens, tone: stats.flaggedTokens ? 'warn' : '' },
        ];
        diagnosticSummary.innerHTML = '';
        summaryItems.forEach((item) => {
          const card = document.createElement('div');
          card.className = 'diagnostic-card' + (item.tone === 'warn' ? ' warn' : '');
          const label = document.createElement('div');
          label.className = 'label';
          label.textContent = item.label;
          const value = document.createElement('div');
          value.className = 'value';
          value.textContent = formatNumber(item.value);
          card.appendChild(label);
          card.appendChild(value);
          diagnosticSummary.appendChild(card);
        });
        diagnosticConflicts.innerHTML = '';
        const topConflicts = conflicts.slice(0, 15);
        if (!topConflicts.length) {
          const empty = document.createElement('div');
          empty.className = 'diagnostic-empty';
          empty.textContent = 'All surfaces use a single reading.';
          diagnosticConflicts.appendChild(empty);
        } else {
          topConflicts.forEach((entry) => {
            const card = document.createElement('div');
            card.className = 'diagnostic-conflict' + (entry.flagged ? ' flagged' : '');
            const header = document.createElement('div');
            header.className = 'diagnostic-conflict-header';
            const surface = document.createElement('div');
            surface.className = 'diagnostic-surface';
            surface.textContent = entry.surface || '—';
            header.appendChild(surface);
            const meta = document.createElement('div');
            meta.className = 'diagnostic-meta';
            if (entry.flagged) {
              const chip = document.createElement('span');
              chip.className = 'diagnostic-chip warn';
              chip.textContent = 'needs review';
              meta.appendChild(chip);
            }
            const share = document.createElement('span');
            share.textContent = `Top share ${formatPercent(entry.primaryShare)}`;
            meta.appendChild(share);
            const count = document.createElement('span');
            count.textContent = `${entry.total} token${entry.total === 1 ? '' : 's'}`;
            meta.appendChild(count);
            const sourceLabel = entry.sources && entry.sources.length
              ? entry.sources
                  .slice(0, 3)
                  .map((src) => `${src.name} (${src.count})`)
                  .join(', ')
              : 'unknown';
            const sources = document.createElement('span');
            sources.textContent = `Sources: ${sourceLabel}`;
            meta.appendChild(sources);
            header.appendChild(meta);
            card.appendChild(header);
            const readingsLabel = document.createElement('div');
            readingsLabel.className = 'diagnostic-readings';
            readingsLabel.textContent = entry.readings
              .map((reading) => {
                const srcLabel = reading.sources && reading.sources.length
                  ? reading.sources
                      .slice(0, 2)
                      .map((src) => `${src.name} ${src.count}`)
                      .join(', ')
                  : '';
                const sourceSuffix = srcLabel ? `; ${srcLabel}` : '';
                return `${reading.reading} (${reading.count}/${entry.total}, ${formatPercent(reading.share)}${sourceSuffix})`;
              })
              .join(' · ');
            card.appendChild(readingsLabel);
            if (entry.sampleIndices && entry.sampleIndices.length) {
              const sample = document.createElement('div');
              sample.className = 'diagnostic-meta';
              const sampleLabel = entry.sampleIndices.length > 1 ? 'Sample tokens' : 'Sample token';
              sample.textContent = `${sampleLabel} #${entry.sampleIndices.slice(0, 3).join(', #')}`;
              card.appendChild(sample);
            }
            diagnosticConflicts.appendChild(card);
          });
        }
        diagnosticsPanel.hidden = false;
      }

      function formatContextSnippet(result) {
        const prefix = result.context_prefix || '';
        const suffix = result.context_suffix || '';
        const surface = result.surface || '';
        return `${prefix}${surface}${suffix}`;
      }

      function positionSelectionToolbar() {
        if (!selectionToolbar || !selectionViewportRect) return;
        const rect = selectionViewportRect;
        const scrollX = window.scrollX || window.pageXOffset;
        const scrollY = window.scrollY || window.pageYOffset;
        const top = rect.top + scrollY - selectionToolbar.offsetHeight - 8;
        const left = rect.left + scrollX + rect.width / 2 - selectionToolbar.offsetWidth / 2;
        selectionToolbar.style.top = `${Math.max(8, top)}px`;
        selectionToolbar.style.left = `${Math.max(8, left)}px`;
      }

      function renderSearchResults(results, query, scope) {
        if (!diagnosticSearchResults) return;
        diagnosticSearchResults.innerHTML = '';
        if (!query) {
          diagnosticSearchResults.innerHTML = '<div class="diagnostic-empty">Enter a surface to search.</div>';
          return;
        }
        if (!Array.isArray(results) || !results.length) {
          diagnosticSearchResults.innerHTML = '<div class="diagnostic-empty">No matches found.</div>';
          return;
        }
        results.forEach((result) => {
          const card = document.createElement('div');
          card.className = 'diagnostic-result';
          const header = document.createElement('div');
          header.className = 'diagnostic-result-header';
          const title = document.createElement('div');
          title.className = 'diagnostic-result-title';
          title.textContent = `${result.surface || ''} → ${result.reading || ''}`;
          header.appendChild(title);
          const meta = document.createElement('div');
          meta.className = 'diagnostic-result-meta';
          if (result.chapter_path) {
            const chapter = document.createElement('span');
            chapter.textContent = result.chapter_path;
            meta.appendChild(chapter);
          }
          if (Array.isArray(result.sources) && result.sources.length) {
            const sources = document.createElement('span');
            sources.textContent = `Sources: ${result.sources.join(', ')}`;
            meta.appendChild(sources);
          }
          if (typeof result.index === 'number') {
            const idx = document.createElement('span');
            idx.textContent = `Token #${result.index}`;
            meta.appendChild(idx);
          }
          header.appendChild(meta);
          card.appendChild(header);

          const contextLine = document.createElement('div');
          contextLine.className = 'diagnostic-result-context';
          contextLine.textContent = formatContextSnippet(result);
          card.appendChild(contextLine);

          const actions = document.createElement('div');
          actions.className = 'diagnostic-result-actions';
          const openBtn = document.createElement('button');
          openBtn.textContent = result.chapter_path === state.selectedPath ? 'Highlight' : 'Open';
          openBtn.addEventListener('click', () => {
            if (!result.chapter_path) return;
            if (result.chapter_path === state.selectedPath) {
              setHighlighted(result.index);
              scrollTokenIntoView(result.index);
            } else {
              state.pendingHighlightIndex = typeof result.index === 'number' ? result.index : null;
              openChapter(result.chapter_path, { autoCollapse: false, preserveHighlight: true });
            }
          });
          actions.appendChild(openBtn);
          card.appendChild(actions);
          diagnosticSearchResults.appendChild(card);
        });
      }

      function tokenSearchPayloadFromChapter(tokens, path) {
        if (!Array.isArray(tokens)) return [];
        return tokens.map((token, index) => ({
          surface: token.surface || '',
          reading: token.reading || '',
          sources: Array.isArray(token.sources) ? token.sources : [],
          pos: token.pos,
          accent: token.accent,
          connection: token.connection,
          context_prefix: token.context_prefix || '',
          context_suffix: token.context_suffix || '',
          index,
          chapter_path: path || state.selectedPath,
        }));
      }

      async function runDiagnosticSearch() {
        if (!diagnosticSearchInput || !diagnosticSearchScope) return;
        const query = (diagnosticSearchInput.value || '').trim();
        const scope = diagnosticSearchScope.value === 'chapter' ? 'chapter' : 'book';
        if (!query) {
          renderSearchResults([], query, scope);
          return;
        }
        if (scope === 'book' && !state.selectedPath) {
          diagnosticSearchResults.innerHTML = '<div class="diagnostic-empty">Select a chapter to search its book.</div>';
          return;
        }
        if (scope === 'chapter') {
          const matches = tokenSearchPayloadFromChapter(state.tokens, state.selectedPath).filter(
            (entry) => entry.surface === query
          );
          renderSearchResults(matches, query, scope);
          return;
        }
        if (!state.selectedPath) {
          renderSearchResults([], query, scope);
          return;
        }
        try {
          const params = new URLSearchParams();
          params.set('path', state.selectedPath);
          params.set('surface', query);
          params.set('scope', scope);
          const payload = await fetchJSON(`/api/token-search?${params.toString()}`);
          const results = Array.isArray(payload.results) ? payload.results : [];
          renderSearchResults(results, query, scope);
        } catch (error) {
          const message = error && error.message ? error.message : 'Search failed.';
          diagnosticSearchResults.innerHTML = `<div class="diagnostic-empty">${message}</div>`;
        }
      }

      function applyFilter() {
        const query = state.filterValue.trim().toLowerCase();
        if (!query) {
          state.filtered = state.chapters.slice();
        } else {
          state.filtered = state.chapters.filter((chapter) => {
            const book = (chapter.book || '').toLowerCase();
            return chapter.path.toLowerCase().includes(query) || book.includes(query);
          });
        }
        renderChapterList();
      }

      function buildChapterTree(chapters) {
        const createDirNode = (name, path, depth, order = Number.POSITIVE_INFINITY) => ({
          type: 'dir',
          name,
          path,
          depth,
          children: [],
          dirs: new Map(),
          chapterCount: 0,
          order,
        });
        const root = createDirNode('', '', 0, -1);
        const ensureDir = (node, name, orderHint) => {
          if (node.dirs.has(name)) {
            const existing = node.dirs.get(name);
            if (existing && typeof orderHint === 'number') {
              existing.order = Math.min(existing.order, orderHint);
            }
            return existing;
          }
          const dirPath = node.path ? `${node.path}/${name}` : name;
          const child = createDirNode(name, dirPath, node.depth + 1, orderHint);
          node.dirs.set(name, child);
          node.children.push(child);
          return child;
        };

        chapters.forEach((chapter, index) => {
          const parts = chapter.path.split('/');
          const fileName = parts.pop() || chapter.name;
          let cursor = root;
          parts.forEach((part) => {
            if (part) {
              cursor = ensureDir(cursor, part, index);
            }
          });
          cursor.children.push({
            type: 'file',
            name: fileName,
            path: chapter.path,
            depth: cursor.depth + 1,
            chapter,
            order: index,
          });
        });

        const finalize = (node) => {
          let total = 0;
          node.children.sort((a, b) => {
            const orderA = typeof a.order === 'number' ? a.order : Number.POSITIVE_INFINITY;
            const orderB = typeof b.order === 'number' ? b.order : Number.POSITIVE_INFINITY;
            if (orderA !== orderB) {
              return orderA - orderB;
            }
            if (a.type !== b.type) {
              return a.type === 'dir' ? -1 : 1;
            }
            return a.name.localeCompare(b.name, 'ja', { numeric: true, sensitivity: 'base' });
          });
          node.children.forEach((child) => {
            if (child.type === 'dir') {
              total += finalize(child);
            } else {
              total += 1;
            }
          });
          node.chapterCount = total;
          node.dirs = undefined;
          return total;
        };

        finalize(root);
        return root;
      }

      function renderTreeNode(node) {
        if (node.type === 'dir') {
          return renderFolderNode(node);
        }
        return renderFileNode(node);
      }

      function renderFolderNode(node) {
        const searchActive = Boolean(state.filterValue.trim());
        const li = document.createElement('li');
        li.className = 'chapter-folder';
        const expanded = searchActive || isFolderExpanded(node.path);
        li.classList.toggle('collapsed', !expanded);
        li.classList.toggle('expanded', expanded);

        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'chapter folder-toggle';
        const indentLevel = Math.max(0, node.depth - 1);
        button.style.setProperty('--indent', `${indentLevel * 1.2}rem`);
        button.dataset.folderPath = node.path;
        const name = document.createElement('div');
        name.className = 'name';
        name.textContent = node.name || node.path || 'root';
        const meta = document.createElement('div');
        meta.className = 'meta';
        const label = node.chapterCount === 1 ? 'chapter' : 'chapters';
        meta.textContent = `${node.chapterCount} ${label}`;
        button.appendChild(name);
        button.appendChild(meta);
        button.setAttribute('aria-expanded', String(expanded));

        const children = document.createElement('ul');
        children.className = 'chapter-children';
        children.classList.toggle('collapsed', !expanded);
        node.children.forEach((child) => {
          children.appendChild(renderTreeNode(child));
        });
        if (searchActive) {
          button.disabled = true;
          button.title = 'Folders stay expanded while search is active';
        } else {
          button.addEventListener('click', () => {
            const currentlyExpanded = !children.classList.contains('collapsed');
            const nextState = !currentlyExpanded;
            setFolderExpanded(node.path, nextState);
            li.classList.toggle('collapsed', !nextState);
            li.classList.toggle('expanded', nextState);
            children.classList.toggle('collapsed', !nextState);
            button.setAttribute('aria-expanded', String(nextState));
          });
        }
        li.appendChild(button);
        li.appendChild(children);
        return li;
      }

      function renderFileNode(node) {
        const li = document.createElement('li');
        li.className = 'chapter-leaf';
        const button = document.createElement('button');
        button.type = 'button';
        const isActive = state.selectedPath === node.chapter.path;
        button.className = 'chapter' + (isActive ? ' active' : '');
        button.dataset.path = node.chapter.path;
        const indentLevel = Math.max(0, node.depth - 1);
        button.style.setProperty('--indent', `${indentLevel * 1.2}rem`);
        const name = document.createElement('div');
        name.className = 'name';
        name.textContent = node.chapter.name;
        const meta = document.createElement('div');
        meta.className = 'meta';
        const tokenFlag = node.chapter.has_token ? 'token✓' : 'token×';
        const origFlag = node.chapter.has_original ? 'orig✓' : 'orig×';
        meta.textContent = `${tokenFlag} · ${origFlag} · ${formatBytes(node.chapter.size)} · ${formatDate(node.chapter.modified)}`;
        button.appendChild(name);
        button.appendChild(meta);
        button.addEventListener('click', () => {
          openChapter(node.chapter.path, { autoCollapse: true });
        });
        li.appendChild(button);
        return li;
      }

      function renderChapterList() {
        listEl.innerHTML = '';
        if (!state.filtered.length) {
          const empty = document.createElement('li');
          empty.textContent = state.chapters.length ? 'No matches.' : 'No chapters found.';
          empty.style.color = 'var(--muted)';
          listEl.appendChild(empty);
          return;
        }
        const tree = buildChapterTree(state.filtered);
        const fragment = document.createDocumentFragment();
        tree.children.forEach((child) => {
          fragment.appendChild(renderTreeNode(child));
        });
        listEl.appendChild(fragment);
      }

      function formatUploadStatus(status) {
        if (!status) return 'Pending';
        const label = String(status);
        return label.charAt(0).toUpperCase() + label.slice(1);
      }

      function setUploadError(message) {
        if (!uploadErrorEl) return;
        uploadErrorEl.textContent = message || '';
      }

      function renderUploadJobs() {
        if (!uploadJobsEl) return;
        uploadJobsEl.innerHTML = '';
        if (!state.uploadJobs.length) {
          const empty = document.createElement('div');
          empty.className = 'upload-empty';
          empty.textContent = 'No uploads yet.';
          uploadJobsEl.appendChild(empty);
          return;
        }
        state.uploadJobs.forEach((job) => {
          const item = document.createElement('div');
          item.className = 'upload-job';
          if (job.status) {
            item.dataset.status = job.status;
          }
          const header = document.createElement('div');
          header.className = 'upload-job-header';
          const title = document.createElement('div');
          title.className = 'upload-job-title';
          title.textContent = job.filename || 'Upload';
          const statusLabel = document.createElement('span');
          statusLabel.className = 'upload-job-status';
          statusLabel.textContent = formatUploadStatus(job.status);
          header.appendChild(title);
          header.appendChild(statusLabel);
          item.appendChild(header);
          const message = document.createElement('div');
          message.className = 'upload-job-message';
          const progressLabel = job.progress && job.progress.label ? job.progress.label : null;
          const errorMessage = job.error || null;
          message.textContent = errorMessage || job.message || progressLabel || 'Pending…';
          item.appendChild(message);
          const targetText = job.book_dir || job.target_name;
          if (targetText) {
            const target = document.createElement('div');
            target.className = 'upload-target';
            target.textContent = `→ ${targetText}`;
            item.appendChild(target);
          }
          const progress = job.progress;
          if (
            progress
            && typeof progress.index === 'number'
            && typeof progress.total === 'number'
            && progress.total > 0
          ) {
            const percent = Math.max(0, Math.min(100, (progress.index / progress.total) * 100));
            const barWrap = document.createElement('div');
            barWrap.className = 'upload-progress';
            const bar = document.createElement('div');
            bar.className = 'upload-progress-bar';
            bar.style.width = `${percent}%`;
            barWrap.appendChild(bar);
            item.appendChild(barWrap);
          }
          uploadJobsEl.appendChild(item);
        });
      }

      function applyUploadJobs(jobs) {
        if (!Array.isArray(jobs)) {
          if (!state.uploadJobs.length) {
            renderUploadJobs();
          }
          return;
        }
        const prevStatuses = new Map(state.uploadJobs.map((job) => [job.id, job.status]));
        const normalized = jobs
          .filter((job) => job && typeof job === 'object')
          .map((job) => job);
        normalized.sort((a, b) => {
          const aTime = new Date(a.updated || a.created || 0).getTime();
          const bTime = new Date(b.updated || b.created || 0).getTime();
          return bTime - aTime;
        });
        state.uploadJobs = normalized;
        renderUploadJobs();
        let shouldReload = false;
        normalized.forEach((job) => {
          if (job.status === 'success' && prevStatuses.get(job.id) !== 'success') {
            shouldReload = true;
          }
        });
        if (shouldReload) {
          loadChapters();
        }
      }

      function loadUploads() {
        fetchJSON('/api/uploads')
          .then((payload) => {
            if (payload && Array.isArray(payload.jobs)) {
              applyUploadJobs(payload.jobs);
            } else if (!state.uploadJobs.length) {
              renderUploadJobs();
            }
          })
          .catch((err) => {
            console.warn('Failed to load uploads', err);
          });
      }

      function startUploadPolling() {
        if (uploadPollTimer !== null) {
          return;
        }
        uploadPollTimer = window.setInterval(() => {
          loadUploads();
        }, UPLOAD_POLL_INTERVAL);
      }

      function handleUploadFiles(fileList) {
        const files = [];
        if (!fileList) {
          // no-op
        } else if (typeof fileList.length === 'number') {
          for (let i = 0; i < fileList.length; i += 1) {
            const entry = fileList[i];
            if (entry) {
              files.push(entry);
            }
          }
        } else if (fileList && fileList.name) {
          files.push(fileList);
        }
        const file = files.find((candidate) => {
          if (!candidate || !candidate.name) {
            return false;
          }
          return candidate.name.toLowerCase().endsWith('.epub');
        });
        if (!file) {
          setUploadError('Please choose an .epub file.');
          return;
        }
        setUploadError('');
        if (uploadDrop) {
          uploadDrop.classList.remove('dragging');
          uploadDrop.classList.add('upload-busy');
        }
        const submitUpload = (force = false) => {
          const formData = new FormData();
          formData.append('file', file, file.name);
          if (force) {
            formData.set('force', '1');
          }
          fetch('/api/uploads', {
            method: 'POST',
            body: formData,
          })
            .then(async (res) => {
              let payload = null;
              try {
                payload = await res.json();
              } catch (error) {
                payload = null;
              }
              if (!res.ok) {
                const detail = payload && payload.detail ? payload.detail : null;
                if (res.status === 409 && detail) {
                  const confirmText = `${detail}\n\nProceed and overwrite existing chapterized files?`;
                  if (window.confirm(confirmText)) {
                    return submitUpload(true);
                  }
                }
                throw new Error(detail || `Upload failed (${res.status})`);
              }
              return payload;
            })
            .then(() => {
              loadUploads();
              startUploadPolling();
            })
            .catch((err) => {
              console.error(err);
              setUploadError(err.message || 'Upload failed.');
            })
            .finally(() => {
              if (uploadInput) {
                uploadInput.value = '';
              }
              if (uploadDrop) {
                uploadDrop.classList.remove('upload-busy');
                uploadDrop.classList.remove('dragging');
              }
              uploadDragDepth = 0;
            });
        };
        submitUpload(false);
      }

      function clearSelection() {
        metaPanel.hidden = true;
        clearDiagnostics();
        transformedMeta.textContent = '—';
        transformedMeta.className = 'pill';
        originalMeta.textContent = '—';
        originalMeta.className = 'pill';
        transformedText.classList.add('empty');
        transformedText.textContent = 'Select a chapter to preview.';
        originalText.classList.add('empty');
        originalText.textContent = 'Original text unavailable.';
        resetScrollPositions();
        setLineRegistry('transformed', []);
        setLineRegistry('original', []);
        scheduleAlignLines();
        setDocumentTitle(null);
        state.chapterPayload = null;
        state.tokens = [];
        state.pendingHighlightIndex = null;
        selectionRange = null;
        selectionViewportRect = null;
        hideSelectionToolbar();
        closeRefineModal();
        setHighlighted(null);
        setChapterNavLabel('');
        updateChapterNavButtons();
        updateOverridesButton();
        if (diagnosticSearchResults) {
          diagnosticSearchResults.innerHTML = '<div class="diagnostic-empty">Enter a surface to search.</div>';
        }
      }

      function buildChapterUrl(path, includeTransformed) {
        const params = new URLSearchParams({ path });
        if (includeTransformed) {
          params.set('include_transformed', '1');
        }
        return `/api/chapter?${params.toString()}`;
      }

      function renderOriginalText(payload, tokens) {
        const hasOriginalFile = Boolean(payload.original_path);
        const originalLoaded = typeof payload.original_text === 'string';
        updateTextMeta(originalMeta, payload.original_text, hasOriginalFile, originalLoaded);
        if (originalLoaded) {
          const originalLines = renderTextView(originalText, payload.original_text, tokens, 'original');
          setLineRegistry('original', originalLines);
          return;
        }
        originalText.classList.add('empty');
        originalText.textContent = hasOriginalFile
          ? 'Original text unavailable.'
          : 'Missing original text.';
        setLineRegistry('original', []);
      }

      function renderTransformedText(payload, tokens) {
        const textLoaded = typeof payload.text === 'string';
        updateTextMeta(transformedMeta, payload.text, true, textLoaded);
        if (textLoaded) {
          const transformedLines = renderTextView(transformedText, payload.text, tokens, 'transformed');
          setLineRegistry('transformed', transformedLines);
          return;
        }
        transformedText.classList.add('empty');
        transformedText.textContent = toggleTransformed && toggleTransformed.checked
          ? 'Loading transformed text…'
          : 'Enable transformed text to load.';
        setLineRegistry('transformed', []);
      }

      function applyChapterPayload(payload, options = {}) {
        const tokens = Array.isArray(payload.tokens) ? payload.tokens : [];
        state.tokens = tokens;
        state.chapterPayload = payload;
        if (!options.preserveHighlight) {
          setHighlighted(null);
        }
        const chapter = payload.chapter || {};
        const chapterName = chapter.name || null;
        const displayName = payload.name || chapterName || state.selectedPath || '';
        renderStatus(`Loaded ${displayName} (${tokens.length} tokens)`);
        setDocumentTitle(displayName);
        setChapterNavLabel(displayName);
        updateChapterNavButtons();
        updateMetaPanel(payload);
        renderOriginalText(payload, tokens);
        renderTransformedText(payload, tokens);
        renderDiagnostics(tokens);
        if (diagnosticSearchInput && diagnosticSearchInput.value.trim()) {
          runDiagnosticSearch();
        }
        scheduleAlignLines();
        updateOverridesButton();
        if (state.pendingHighlightIndex !== null) {
          const highlightIndex = state.pendingHighlightIndex;
          state.pendingHighlightIndex = null;
          requestAnimationFrame(() => {
            setHighlighted(highlightIndex);
            scrollTokenIntoView(highlightIndex);
          });
        }
        if (options.preserveScroll && options.scrollSnapshot) {
          restoreScrollPositions(options.scrollSnapshot);
        } else {
          resetScrollPositions();
        }
      }

      function openChapter(path, options = {}) {
        if (!path) {
          return;
        }
        state.selectedPath = path;
        const pendingLabel = chapterLabelFromPath(path) || path;
        setChapterNavLabel(pendingLabel);
        updateChapterNavButtons();
        expandFoldersForPath(path);
        if (!options.preservePayload) {
          state.chapterPayload = null;
        }
        if (state.chapters.length) {
          renderChapterList();
        }
        updateChapterHash(path);
        const includeTransformed =
          options.includeTransformed ?? Boolean(toggleTransformed && toggleTransformed.checked);
        const preserveScroll = Boolean(options.preserveScroll);
        const scrollSnapshot = preserveScroll ? snapshotScrollPositions() : null;
        renderStatus(`Loading ${path}…`);
        fetchJSON(buildChapterUrl(path, includeTransformed))
          .then((payload) => {
            if (state.selectedPath !== path) {
              return;
            }
            applyChapterPayload(payload, {
              includeTransformed,
              preserveScroll,
              scrollSnapshot,
              preserveHighlight: Boolean(options.preserveHighlight),
            });
            if (options.autoCollapse && mobileQuery.matches) {
              setSidebarCollapsed(true);
            }
          })
          .catch((err) => {
            if (state.selectedPath !== path) {
              return;
            }
            console.error(err);
            renderStatus(`Failed to load ${path}: ${err.message}`);
            if (!options.keepStateOnError) {
              state.selectedPath = null;
              renderChapterList();
              clearSelection();
            } else if (toggleTransformed && toggleTransformed.checked) {
              transformedText.classList.add('empty');
              transformedText.textContent = 'Failed to load transformed text.';
              setLineRegistry('transformed', []);
              scheduleAlignLines();
            }
          });
      }

      function requestTransformedLoad() {
        if (!toggleTransformed || !toggleTransformed.checked) {
          return;
        }
        if (!state.selectedPath) {
          return;
        }
        if (state.chapterPayload && typeof state.chapterPayload.text === 'string') {
          renderTransformedText(state.chapterPayload, state.tokens);
          scheduleAlignLines();
          return;
        }
        transformedText.classList.add('empty');
        transformedText.textContent = 'Loading transformed text…';
        setLineRegistry('transformed', []);
        scheduleAlignLines();
        openChapter(state.selectedPath, {
          includeTransformed: true,
          preserveScroll: true,
          preserveHighlight: true,
          preservePayload: true,
          keepStateOnError: true,
          autoCollapse: false,
        });
      }

      function handleHashNavigation() {
        const targetPath = getChapterPathFromHash();
        if (!targetPath) {
          if (state.selectedPath) {
            state.selectedPath = null;
            renderChapterList();
            clearSelection();
          }
          return;
        }
        if (targetPath === state.selectedPath) {
          return;
        }
        openChapter(targetPath, { autoCollapse: false });
      }

      function loadChapters() {
        renderStatus('Loading chapters…');
        const params = new URLSearchParams();
        params.set('sort', state.sortOrder || 'author');
        fetchJSON(`/api/chapters?${params.toString()}`)
          .then((data) => {
            state.chapters = data.chapters || [];
            if (!state.chapters.some((chapter) => chapter.path === state.selectedPath)) {
              state.selectedPath = null;
              clearSelection();
            }
            applyFilter();
            if (state.selectedPath) {
              setChapterNavLabel(chapterLabelFromPath(state.selectedPath));
            }
            updateChapterNavButtons();
            renderStatus(`Found ${state.chapters.length} chapter(s) under ${data.root}`);
          })
          .catch((err) => {
            console.error(err);
            renderStatus(`Failed to load chapters: ${err.message}`);
          });
      }

      if (uploadDrop) {
        uploadDrop.addEventListener('click', (event) => {
          event.preventDefault();
          if (uploadInput) {
            uploadInput.click();
          }
        });
        uploadDrop.addEventListener('keydown', (event) => {
          if (event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            if (uploadInput) {
              uploadInput.click();
            }
          }
        });
        uploadDrop.addEventListener('dragenter', (event) => {
          event.preventDefault();
          uploadDragDepth += 1;
          uploadDrop.classList.add('dragging');
        });
        uploadDrop.addEventListener('dragover', (event) => {
          event.preventDefault();
        });
        uploadDrop.addEventListener('dragleave', (event) => {
          event.preventDefault();
          uploadDragDepth = Math.max(0, uploadDragDepth - 1);
          if (uploadDragDepth === 0) {
            uploadDrop.classList.remove('dragging');
          }
        });
        uploadDrop.addEventListener('drop', (event) => {
          event.preventDefault();
          const files = event.dataTransfer ? event.dataTransfer.files : null;
          uploadDragDepth = 0;
          uploadDrop.classList.remove('dragging');
          handleUploadFiles(files);
        });
      }
      if (uploadInput) {
        uploadInput.addEventListener('change', () => {
          handleUploadFiles(uploadInput.files);
        });
      }

      renderUploadJobs();
      loadUploads();
      startUploadPolling();

      filterEl.addEventListener('input', (event) => {
        state.filterValue = event.target.value;
        applyFilter();
      });

      refreshBtn.addEventListener('click', () => {
        loadChapters();
      });

      if (diagnosticSearchInput) {
        diagnosticSearchInput.addEventListener('input', () => {
          if (diagnosticSearchTimer) {
            window.clearTimeout(diagnosticSearchTimer);
          }
          diagnosticSearchTimer = window.setTimeout(() => {
            runDiagnosticSearch();
          }, 220);
        });
      }
      if (diagnosticSearchScope) {
        diagnosticSearchScope.addEventListener('change', () => {
          runDiagnosticSearch();
        });
      }

      function openSelectionAsToken() {
        if (!selectionRange || !selectionViewportRect) {
          hideSelectionToolbar();
          return;
        }
        const selection = selectionTextAndOffsets();
        if (!selection || !selection.text) {
          hideSelectionToolbar();
          return;
        }
        const selectionContext = {
          selection: { start: selection.start, end: selection.end },
          text: selection.text,
          token: { surface: selection.text, reading: selection.text, sources: ['selection'] },
          view: 'transformed',
        };
        hideSelectionToolbar();
        openRefineModal(selectionContext);
      }

      function handleSelectionChange() {
        const info = selectionTextAndOffsets();
        if (!info || !info.text || info.text.trim().length === 0) {
          hideSelectionToolbar();
          return;
        }
        selectionRange = info.range;
        selectionViewportRect = info.rect;
        selectionAddButton.disabled = false;
        selectionToolbar.classList.remove('hidden');
        positionSelectionToolbar();
      }

      selectionAddButton.addEventListener('click', () => {
        openSelectionAsToken();
      });
      selectionCancelButton.addEventListener('click', () => {
        hideSelectionToolbar();
        const sel = window.getSelection ? window.getSelection() : null;
        if (sel) sel.removeAllRanges();
      });

      if (transformedText) {
        transformedText.addEventListener('mouseup', () => {
          setTimeout(handleSelectionChange, 0);
        });
        transformedText.addEventListener('keyup', (event) => {
          if (event.key === 'Shift') return;
          setTimeout(handleSelectionChange, 0);
        });
      }
      document.addEventListener('scroll', () => {
        if (!selectionToolbar.classList.contains('hidden')) {
          positionSelectionToolbar();
        }
      });

      window.addEventListener('hashchange', handleHashNavigation);

      clearSelection();
      updateOverridesButton();
      loadChapters();
      if (initialHashPath) {
        openChapter(initialHashPath, { autoCollapse: false, preservePayload: false });
      }
    })();
  </script>
</body>
</html>
"""


_SORT_MODES = {"author", "recent", "played"}


def _normalize_sort_mode(value: str | None) -> str:
    if not value:
        return "author"
    normalized = value.strip().lower()
    if normalized in _SORT_MODES:
        return normalized
    raise HTTPException(status_code=400, detail="Invalid sort mode.")


def _iter_chapter_files(root: Path, sort_mode: str) -> Iterable[Path]:
    for book in list_books_sorted(root, mode=sort_mode):
        book_path = book.path
        for path in sorted(book_path.rglob("*.txt")):
            if not path.is_file():
                continue
            if is_original_text_file(path) or path.name.endswith(".partial.txt"):
                continue
            yield path
    # Include any loose .txt files directly under the root (rare).
    for path in sorted(root.glob("*.txt")):
        if (
            not path.is_file()
            or is_original_text_file(path)
            or path.name.endswith(".partial.txt")
        ):
            continue
        yield path


def _relative_to_root(root: Path, path: Path) -> Path:
    try:
        return path.resolve().relative_to(root)
    except ValueError as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=400, detail="Invalid chapter path") from exc


def _resolve_book_dir(root: Path, book_id: str) -> Path:
    if not isinstance(book_id, str) or not book_id.strip():
        raise HTTPException(status_code=400, detail="Book id is required.")
    candidate = (root / book_id).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Book not found.") from exc
    if not candidate.exists() or not candidate.is_dir():
        raise HTTPException(status_code=404, detail="Book not found.")
    return candidate


def _chapter_entry(root: Path, path: Path) -> dict[str, object]:
    rel = _relative_to_root(root, path)
    stat = path.stat()
    token_path = path.with_name(path.name + ".token.json")
    original_path = path.with_name(f"{path.stem}.original.txt")
    modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
    entry = {
        "path": rel.as_posix(),
        "name": path.name,
        "book": rel.parts[0] if len(rel.parts) > 1 else None,
        "size": stat.st_size,
        "modified": modified,
        "has_token": token_path.exists(),
        "has_original": original_path.exists(),
    }
    return entry


def _list_chapters(root: Path, sort_mode: str) -> list[dict[str, object]]:
    return [_chapter_entry(root, path) for path in _iter_chapter_files(root, sort_mode)]


def _safe_read_text(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="ignore")


def _convert_token_entry(entry: Mapping[str, object]) -> dict[str, object]:
    surface = entry.get("surface")
    if not isinstance(surface, str):
        surface = ""
    reading = entry.get("reading")
    if not isinstance(reading, str):
        reading = ""
    pos = entry.get("pos")
    if not isinstance(pos, str):
        pos = None
    accent = entry.get("accent")
    if not isinstance(accent, int):
        accent = entry.get("accent_type")
        if not isinstance(accent, int):
            accent = None
    connection = entry.get("connection")
    if not isinstance(connection, str):
        connection = entry.get("accent_connection")
        if not isinstance(connection, str):
            connection = None
    start_original = entry.get("start")
    if isinstance(start_original, Mapping):
        start_original_value = start_original.get("original")
    else:
        start_original_value = (
            start_original if isinstance(start_original, int) else None
        )
    end_original = entry.get("end")
    if isinstance(end_original, Mapping):
        end_original_value = end_original.get("original")
    else:
        end_original_value = end_original if isinstance(end_original, int) else None
    transformed_start = entry.get("transformed_start")
    transformed_end = entry.get("transformed_end")
    if isinstance(entry.get("start"), Mapping):
        transformed_start = entry["start"].get("transformed")
    if isinstance(entry.get("end"), Mapping):
        transformed_end = entry["end"].get("transformed")
    sources = entry.get("sources")
    normalized_sources: list[str] = []
    if isinstance(sources, list):
        normalized_sources = [
            str(source) for source in sources if isinstance(source, str) and source
        ]
    else:
        reading_source = entry.get("reading_source")
        if isinstance(reading_source, str) and reading_source:
            normalized_sources = [reading_source]
    context_prefix = entry.get("context_prefix")
    if not isinstance(context_prefix, str):
        context_prefix = ""
    context_suffix = entry.get("context_suffix")
    if not isinstance(context_suffix, str):
        context_suffix = ""
    return {
        "surface": surface,
        "reading": reading,
        "pos": pos,
        "accent": accent,
        "connection": connection,
        "sources": normalized_sources,
        "context_prefix": context_prefix,
        "context_suffix": context_suffix,
        "start": {
            "original": start_original_value,
            "transformed": transformed_start
            if isinstance(transformed_start, int)
            else None,
        },
        "end": {
            "original": end_original_value,
            "transformed": transformed_end
            if isinstance(transformed_end, int)
            else None,
        },
    }


def _load_token_payload(
    path: Path,
) -> tuple[list[dict[str, object]], dict[str, object] | None, str | None]:
    if not path.exists():
        return [], None, None
    try:
        raw_payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [], None, f"Failed to parse {path.name}: {exc}"
    tokens_data = raw_payload.get("tokens")
    if not isinstance(tokens_data, list):
        return [], raw_payload, "Token file missing 'tokens' array."
    converted: list[dict[str, object]] = []
    for entry in tokens_data:
        if isinstance(entry, Mapping):
            converted.append(_convert_token_entry(entry))
    return converted, raw_payload, None


def create_reader_app(root: Path) -> FastAPI:
    resolved_root = root.expanduser().resolve()
    if not resolved_root.exists() or not resolved_root.is_dir():
        raise FileNotFoundError(f"Root not found: {resolved_root}")

    app = FastAPI(title="nk Reader")
    app.state.root = resolved_root
    upload_manager = UploadManager(resolved_root)
    app.state.upload_manager = upload_manager
    app.add_event_handler("shutdown", upload_manager.shutdown)

    def _chapter_text_path(chapter_path: Path) -> Path:
        if not chapter_path.exists():
            raise HTTPException(status_code=404, detail="Chapter not found")
        if chapter_path.name.endswith(".partial.txt"):
            raise HTTPException(
                status_code=400, detail="Partial text files are no longer supported."
            )
        return chapter_path

    def _read_overrides(
        book_dir: Path,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]], bool, float | None]:
        overrides_path = book_dir / "custom_token.json"
        if not overrides_path.exists():
            return [], [], False, None
        try:
            raw = json.loads(overrides_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to parse overrides file: {exc}",
            ) from exc
        payload = raw.get("overrides")
        if payload is None:
            payload = raw.get("rules")
        if payload is None:
            payload = raw.get("tokens")
        if not isinstance(payload, list):
            raise HTTPException(
                status_code=400,
                detail="Overrides file must contain an 'overrides' array.",
            )
        remove_payload = raw.get("remove")
        if remove_payload is None:
            remove_payload = raw.get("removals")
        if remove_payload is None:
            remove_payload = []
        if remove_payload is not None and not isinstance(remove_payload, list):
            raise HTTPException(
                status_code=400,
                detail="Overrides file must contain a 'remove' array if provided.",
            )
        try:
            modified = overrides_path.stat().st_mtime
        except OSError:
            modified = None
        normalized: list[dict[str, object]] = []
        for entry in payload:
            if isinstance(entry, dict):
                normalized.append(entry)
        normalized_remove = _normalize_remove_payload(raw)
        return normalized, normalized_remove, True, modified

    def _normalize_override_entry(entry: Mapping[str, object]) -> dict[str, object]:
        pattern = entry.get("pattern")
        if not isinstance(pattern, str) or not pattern.strip():
            raise HTTPException(
                status_code=400, detail="pattern is required for each override."
            )
        regex = bool(entry.get("regex"))
        replacement = entry.get("replacement")
        if replacement is not None and not isinstance(replacement, str):
            replacement = None
        reading = entry.get("reading")
        if reading is not None and not isinstance(reading, str):
            reading = None
        surface = entry.get("surface")
        if surface is not None and not isinstance(surface, str):
            surface = None
        match_surface = entry.get("match_surface")
        if match_surface is not None and not isinstance(match_surface, str):
            match_surface = None
        pos = entry.get("pos")
        if pos is not None and not isinstance(pos, str):
            pos = None
        accent_val = entry.get("accent")
        accent = None
        if isinstance(accent_val, int):
            accent = accent_val
        elif isinstance(accent_val, str) and accent_val.strip().isdigit():
            accent = int(accent_val.strip())
        return {
            "pattern": pattern.strip(),
            "regex": regex,
            **({"replacement": replacement} if replacement else {}),
            **({"reading": reading} if reading else {}),
            **({"surface": surface} if surface else {}),
            **({"match_surface": match_surface} if match_surface else {}),
            **({"pos": pos} if pos else {}),
            **({"accent": accent} if accent is not None else {}),
        }

    def _normalize_remove_entry(entry: Mapping[str, object]) -> dict[str, object]:
        reading = entry.get("reading")
        surface = entry.get("surface")
        normalized: dict[str, object] = {}
        if isinstance(reading, str) and reading.strip():
            normalized["reading"] = reading.strip()
        if isinstance(surface, str) and surface.strip():
            normalized["surface"] = surface.strip()
        return normalized

    def _normalize_remove_payload(payload: object) -> list[dict[str, object]]:
        if not isinstance(payload, Mapping):
            return []
        raw_remove = payload.get("remove")
        if raw_remove is None:
            raw_remove = payload.get("removals")
        if not isinstance(raw_remove, list):
            return []
        cleaned: list[dict[str, object]] = []
        for entry in raw_remove:
            if not isinstance(entry, Mapping):
                continue
            normalized = _normalize_remove_entry(entry)
            if normalized:
                cleaned.append(normalized)
        return cleaned

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        return HTMLResponse(INDEX_HTML.replace("__NK_FAVICON__", NK_FAVICON_URL))

    @app.get("/apple-touch-icon.png")
    def apple_touch_icon() -> Response:
        return Response(content=NK_APPLE_TOUCH_ICON_PNG, media_type="image/png")

    @app.get("/api/chapters")
    def api_chapters(
        sort: str | None = Query(
            None, description="Sort order: author, recent, or played"
        ),
    ) -> JSONResponse:
        sort_mode = _normalize_sort_mode(sort)
        chapters = _list_chapters(resolved_root, sort_mode)
        return JSONResponse({"root": resolved_root.as_posix(), "chapters": chapters})

    @app.get("/api/uploads")
    def api_uploads() -> JSONResponse:
        jobs = upload_manager.list_jobs()
        return JSONResponse({"jobs": jobs})

    @app.post("/api/uploads")
    async def api_upload_epub(
        file: UploadFile = File(...),
        force: bool = Form(False),
    ) -> JSONResponse:
        filename = file.filename or "upload.epub"
        suffix = Path(filename).suffix.lower()
        if suffix != ".epub":
            raise HTTPException(
                status_code=400, detail="Only .epub files are supported."
            )
        job = UploadJob(resolved_root, filename, force=bool(force))
        if job.output_dir.exists() and not job.force:
            job.cleanup()
            detail = (
                f"Book already exists at {job.target_rel}. Uploading will overwrite transformed text, "
                "token metadata, cover, and manifests (bookmarks/custom_token.json are left as-is)."
            )
            raise HTTPException(status_code=409, detail=detail)
        try:
            with job.temp_path.open("wb") as destination:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    destination.write(chunk)
        except Exception as exc:
            job.cleanup()
            raise HTTPException(
                status_code=500, detail=f"Failed to save upload: {exc}"
            ) from exc
        finally:
            await file.close()
        upload_manager.enqueue(job)
        return JSONResponse({"job": job.to_payload()})

    @app.get("/api/books/{book_id:path}/overrides")
    def api_get_overrides(book_id: str) -> JSONResponse:
        book_dir = _resolve_book_dir(resolved_root, book_id)
        overrides, remove_rules, exists, modified = _read_overrides(book_dir)
        return JSONResponse(
            {
                "book": book_id,
                "path": _relative_to_root(resolved_root, book_dir).as_posix(),
                "overrides": overrides,
                "remove": remove_rules,
                "exists": exists,
                "modified": modified,
            }
        )

    @app.put("/api/books/{book_id:path}/overrides")
    def api_update_overrides(
        book_id: str, payload: dict[str, object] = Body(...)
    ) -> JSONResponse:
        book_dir = _resolve_book_dir(resolved_root, book_id)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid payload.")
        overrides_payload = payload.get("overrides")
        if not isinstance(overrides_payload, list):
            raise HTTPException(status_code=400, detail="'overrides' must be an array.")
        remove_payload = payload.get("remove")
        if remove_payload is not None and not isinstance(remove_payload, list):
            raise HTTPException(status_code=400, detail="'remove' must be an array when provided.")
        normalized: list[dict[str, object]] = []
        normalized_remove: list[dict[str, object]] = []
        for entry in overrides_payload:
            if not isinstance(entry, Mapping):
                continue
            normalized.append(_normalize_override_entry(entry))
        if remove_payload is not None:
            for entry in remove_payload:
                if not isinstance(entry, Mapping):
                    continue
                cleaned = _normalize_remove_entry(entry)
                if cleaned:
                    normalized_remove.append(cleaned)
        overrides_path = book_dir / "custom_token.json"
        if remove_payload is None:
            try:
                existing_raw = json.loads(overrides_path.read_text(encoding="utf-8"))
                existing_remove = _normalize_remove_payload(existing_raw)
                if existing_remove:
                    normalized_remove = existing_remove
            except Exception:
                normalized_remove = normalized_remove
        overrides_path.write_text(
            json.dumps({"overrides": normalized, "remove": normalized_remove}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        try:
            modified = overrides_path.stat().st_mtime
        except OSError:
            modified = None
        return JSONResponse(
            {
                "book": book_id,
                "path": _relative_to_root(resolved_root, book_dir).as_posix(),
                "overrides": normalized,
                "remove": normalized_remove,
                "exists": True,
                "modified": modified,
            }
        )

    @app.get("/api/chapter")
    def api_chapter(
        path: str = Query(..., description="Relative path to a .txt file"),
        include_transformed: bool = Query(
            False,
            description="Include transformed text in the response (default: false).",
        ),
    ) -> JSONResponse:
        if not path:
            raise HTTPException(status_code=400, detail="Path is required")
        rel_path = Path(path)
        if rel_path.is_absolute():
            raise HTTPException(
                status_code=400, detail="Path must be relative to the root"
            )
        chapter_path = (resolved_root / rel_path).resolve()
        try:
            chapter_path.relative_to(resolved_root)
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Path escapes the root directory"
            )
        if not chapter_path.exists() or not chapter_path.is_file():
            raise HTTPException(status_code=404, detail="Chapter not found")
        if chapter_path.suffix.lower() != ".txt":
            raise HTTPException(status_code=400, detail="Only .txt files are supported")

        variant_path = _chapter_text_path(chapter_path)

        text = _safe_read_text(variant_path) if include_transformed else None
        text_length = len(text) if text is not None else None
        original_path = chapter_path.with_name(f"{chapter_path.stem}.original.txt")
        original_text = _safe_read_text(original_path)
        original_length = len(original_text) if original_text is not None else None

        token_path = variant_path.with_name(variant_path.name + ".token.json")
        tokens_list, token_payload, token_error = _load_token_payload(token_path)

        chapter_entry = _chapter_entry(resolved_root, chapter_path)
        response = {
            "chapter": chapter_entry,
            "name": chapter_path.name,
            "text": text,
            "text_length": text_length,
            "original_text": original_text,
            "original_length": original_length,
            "tokens": tokens_list,
            "token_version": token_payload.get("version")
            if isinstance(token_payload, Mapping)
            else None,
            "token_sha1": token_payload.get("text_sha1")
            if isinstance(token_payload, Mapping)
            else None,
            "token_path": _relative_to_root(resolved_root, token_path).as_posix()
            if token_path.exists()
            else None,
            "token_error": token_error,
            "original_path": _relative_to_root(resolved_root, original_path).as_posix()
            if original_path.exists()
            else None,
        }
        return JSONResponse(response)

    @app.post("/api/refine")
    def api_refine(payload: dict[str, object] = Body(...)) -> JSONResponse:
        if not isinstance(payload, Mapping):
            raise HTTPException(status_code=400, detail="Invalid payload.")
        path_value = payload.get("path")
        if not isinstance(path_value, str) or not path_value.strip():
            raise HTTPException(status_code=400, detail="path is required.")
        rel_path = Path(path_value)
        if rel_path.is_absolute():
            raise HTTPException(status_code=400, detail="path must be relative.")
        chapter_path = (resolved_root / rel_path).resolve()
        try:
            chapter_path.relative_to(resolved_root)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Path escapes root.") from exc
        if (
            not chapter_path.exists()
            or not chapter_path.is_file()
            or chapter_path.suffix.lower() != ".txt"
        ):
            raise HTTPException(status_code=404, detail="Chapter not found.")
        book_dir = chapter_path.parent

        def _trim(value: object) -> str | None:
            if isinstance(value, str):
                stripped = value.strip()
                if stripped:
                    return stripped
            return None

        scope_value = payload.get("scope")
        scope = "book"
        if isinstance(scope_value, str):
            normalized_scope = scope_value.strip().lower()
            if normalized_scope in {"book", "chapter", "token"}:
                scope = normalized_scope

        pattern = _trim(payload.get("pattern"))
        replacement = _trim(payload.get("replacement"))
        reading = _trim(payload.get("reading"))
        surface = _trim(payload.get("surface"))
        match_surface = _trim(payload.get("match_surface"))
        pos = _trim(payload.get("pos"))
        regex_flag = bool(payload.get("regex"))

        accent_value = payload.get("accent")
        accent: int | None = None
        if isinstance(accent_value, int):
            accent = accent_value
        elif isinstance(accent_value, str) and accent_value.strip():
            try:
                accent = int(accent_value)
            except ValueError:
                raise HTTPException(
                    status_code=400, detail="accent must be an integer."
                ) from None

        if scope == "token":
            token_index = payload.get("token_index")
            if not isinstance(token_index, int) or token_index < 0:
                raise HTTPException(
                    status_code=400, detail="token_index is required for token updates."
                )
            if not any(value is not None for value in (reading, surface, pos, accent)):
                raise HTTPException(
                    status_code=400,
                    detail="Provide at least one editable field (reading, surface, pos, accent).",
                )
            try:
                changed = edit_single_token(
                    chapter_path,
                    token_index,
                    reading=reading,
                    surface=surface,
                    pos=pos,
                    accent=accent,
                    replacement=replacement,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            book_rel = _relative_to_root(resolved_root, book_dir)
            return JSONResponse(
                {
                    "updated": 1 if changed else 0,
                    "scope": "token",
                    "chapter": rel_path.as_posix(),
                    "book": book_rel.as_posix(),
                    "token_index": token_index,
                }
            )

        if not pattern:
            raise HTTPException(status_code=400, detail="pattern is required.")
        entry: dict[str, object] = {"pattern": pattern}
        if regex_flag:
            entry["regex"] = True
        if replacement:
            entry["replacement"] = replacement
        if reading:
            entry["reading"] = reading
        if surface:
            entry["surface"] = surface
        if match_surface:
            entry["match_surface"] = match_surface
        if pos:
            entry["pos"] = pos
        if accent is not None:
            entry["accent"] = accent
        if scope == "chapter":
            scope = "chapter"
        try:
            override_path = append_override_entry(book_dir, entry)
            overrides, removals = load_refine_config(book_dir)
            if scope == "chapter":
                refined_value = refine_chapter(chapter_path, overrides, removals=removals)
                updated = 1 if refined_value else 0
            else:
                updated = refine_book(book_dir, overrides, removals=removals)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        override_rel = _relative_to_root(resolved_root, override_path)
        book_rel = _relative_to_root(resolved_root, book_dir)
        return JSONResponse(
            {
                "updated": updated,
                "override_path": override_rel.as_posix(),
                "chapter": rel_path.as_posix(),
                "book": book_rel.as_posix(),
                "scope": scope,
            }
        )

    @app.post("/api/remove-token")
    def api_remove_token(payload: dict[str, object] = Body(...)) -> JSONResponse:
        if not isinstance(payload, Mapping):
            raise HTTPException(status_code=400, detail="Invalid payload.")
        path_value = payload.get("path")
        if not isinstance(path_value, str) or not path_value.strip():
            raise HTTPException(status_code=400, detail="path is required.")
        rel_path = Path(path_value)
        if rel_path.is_absolute():
            raise HTTPException(status_code=400, detail="path must be relative.")
        chapter_path = (resolved_root / rel_path).resolve()
        try:
            chapter_path.relative_to(resolved_root)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Path escapes root.") from exc
        if (
            not chapter_path.exists()
            or not chapter_path.is_file()
            or chapter_path.suffix.lower() != ".txt"
        ):
            raise HTTPException(status_code=404, detail="Chapter not found.")
        token_index = payload.get("token_index")
        if not isinstance(token_index, int) or token_index < 0:
            raise HTTPException(
                status_code=400,
                detail="token_index is required and must be non-negative.",
            )
        try:
            removed = remove_token(chapter_path, token_index)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        book_dir = chapter_path.parent
        book_rel = _relative_to_root(resolved_root, book_dir)
        return JSONResponse(
            {
                "removed": bool(removed),
                "chapter": rel_path.as_posix(),
                "book": book_rel.as_posix(),
                "token_index": token_index,
            }
        )

    @app.get("/api/token-search")
    def api_token_search(
        path: str = Query(
            ..., description="Relative path to a chapter within the book"
        ),
        surface: str = Query(..., description="Exact surface to search for"),
        scope: str = Query("book", description="Scope: 'book' or 'chapter'"),
    ) -> JSONResponse:
        if not path or not surface:
            raise HTTPException(status_code=400, detail="path and surface are required")
        rel_path = Path(path)
        if rel_path.is_absolute():
            raise HTTPException(
                status_code=400, detail="Path must be relative to the root"
            )
        chapter_path = (resolved_root / rel_path).resolve()
        try:
            chapter_path.relative_to(resolved_root)
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Path escapes the root directory"
            )
        if not chapter_path.exists() or not chapter_path.is_file():
            raise HTTPException(status_code=404, detail="Chapter not found")
        normalized_surface = surface.strip()
        if not normalized_surface:
            raise HTTPException(status_code=400, detail="surface cannot be empty")
        results: list[dict[str, object]] = []

        def _append_matches(txt_path: Path) -> None:
            token_path = txt_path.with_name(txt_path.name + ".token.json")
            tokens, _, _ = _load_token_payload(token_path)
            for idx, token in enumerate(tokens):
                if token.get("surface") != normalized_surface:
                    continue
                entry = {
                    "surface": token.get("surface"),
                    "reading": token.get("reading"),
                    "sources": token.get("sources") or [],
                    "pos": token.get("pos"),
                    "accent": token.get("accent"),
                    "connection": token.get("connection"),
                    "context_prefix": token.get("context_prefix"),
                    "context_suffix": token.get("context_suffix"),
                    "index": idx,
                    "chapter_path": _relative_to_root(
                        resolved_root, txt_path
                    ).as_posix(),
                    "chapter_name": txt_path.name,
                }
                results.append(entry)

        scope_value = (scope or "book").strip().lower()
        if scope_value not in {"book", "chapter"}:
            raise HTTPException(
                status_code=400, detail="scope must be 'book' or 'chapter'"
            )

        if scope_value == "chapter":
            _append_matches(chapter_path)
        else:
            book_dir = chapter_path.parent
            if not book_dir.exists():
                raise HTTPException(status_code=404, detail="Book not found for path")
            for candidate in sorted(book_dir.glob("*.txt")):
                if (
                    not candidate.is_file()
                    or is_original_text_file(candidate)
                    or candidate.name.endswith(".partial.txt")
                ):
                    continue
                _append_matches(candidate)

        return JSONResponse({"results": results})

    @app.post("/api/create-token")
    def api_create_token(payload: dict[str, object] = Body(...)) -> JSONResponse:
        if not isinstance(payload, Mapping):
            raise HTTPException(status_code=400, detail="Invalid payload.")
        path_value = payload.get("path")
        if not isinstance(path_value, str) or not path_value.strip():
            raise HTTPException(status_code=400, detail="path is required.")
        rel_path = Path(path_value)
        if rel_path.is_absolute():
            raise HTTPException(status_code=400, detail="path must be relative.")
        chapter_path = (resolved_root / rel_path).resolve()
        try:
            chapter_path.relative_to(resolved_root)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Path escapes root.") from exc
        if (
            not chapter_path.exists()
            or not chapter_path.is_file()
            or chapter_path.suffix.lower() != ".txt"
        ):
            raise HTTPException(status_code=404, detail="Chapter not found.")
        start = payload.get("start")
        end = payload.get("end")
        if not isinstance(start, int) or not isinstance(end, int):
            raise HTTPException(
                status_code=400, detail="start and end must be integers."
            )
        if end <= start or start < 0:
            raise HTTPException(status_code=400, detail="Invalid selection range.")
        replacement = payload.get("replacement")
        reading_val = payload.get("reading")
        surface_val = payload.get("surface")
        pos_val = payload.get("pos")
        accent_val = payload.get("accent")
        if replacement is not None and not isinstance(replacement, str):
            replacement = None
        if reading_val is not None and not isinstance(reading_val, str):
            reading_val = None
        if surface_val is not None and not isinstance(surface_val, str):
            surface_val = None
        if pos_val is not None and not isinstance(pos_val, str):
            pos_val = None
        if accent_val is not None and not isinstance(accent_val, int):
            accent_val = None
        try:
            updated = create_token_from_selection(
                chapter_path,
                start,
                end,
                replacement=replacement,
                reading=reading_val,
                surface=surface_val,
                pos=pos_val,
                accent=accent_val,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse({"updated": 1 if updated else 0})

    return app
