from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from fastapi import Body, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse

from .library import list_books_sorted
from .refine import append_override_entry, load_override_config, refine_book
from .uploads import UploadJob, UploadManager

INDEX_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <title>nk Reader</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
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
      font-family: "SFMono-Regular", Consolas, "Hiragino Sans", monospace;
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
      font-family: "SFMono-Regular", Consolas, "Hiragino Sans", monospace;
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
      box-shadow: 0 25px 60px rgba(0,0,0,0.45);
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
      justify-content: space-between;
      align-items: center;
      gap: 0.8rem;
      flex-wrap: wrap;
    }
    .modal-button-group {
      display: flex;
      gap: 0.5rem;
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
      </section>
      <section class="panel" id="meta-panel" hidden>
        <h2>chapter info</h2>
        <div class="meta-grid" id="meta-grid"></div>
      </section>
    </main>
  </div>
  <div id="refine-modal" class="modal hidden" aria-hidden="true">
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="refine-title">
      <h3 id="refine-title">Refine token</h3>
      <p id="refine-context">Choose a token to begin.</p>
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
          <div class="modal-button-group">
            <button type="button" class="secondary" id="refine-cancel">Cancel</button>
            <button type="submit" id="refine-submit">Apply override</button>
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
      };
      const baseTitle = document.title || 'nk Reader';
      const CHAPTER_HASH_PREFIX = '#chapter=';

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

      const listEl = document.getElementById('chapter-list');
      const homeLink = document.getElementById('home-link');
      const filterEl = document.getElementById('chapter-filter');
      const refreshBtn = document.getElementById('refresh');
      const sortSelect = document.getElementById('sort-order');
      const statusEl = document.getElementById('status');
      const metaPanel = document.getElementById('meta-panel');
      const textGrid = document.getElementById('text-grid');
      const metaGrid = document.getElementById('meta-grid');
      const transformedMeta = document.getElementById('transformed-meta');
      const originalMeta = document.getElementById('original-meta');
      const transformedText = document.getElementById('transformed-text');
      const originalText = document.getElementById('original-text');
      const transformedPanel = document.getElementById('transformed-panel');
      const originalPanel = document.getElementById('original-panel');
      const toggleTransformed = document.getElementById('toggle-transformed');
      const toggleOriginal = document.getElementById('toggle-original');
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
      const refinePosInput = document.getElementById('refine-pos');
      const refineRegexInput = document.getElementById('refine-regex');
      const refineSubmit = document.getElementById('refine-submit');
      const refineCancel = document.getElementById('refine-cancel');
      const refineError = document.getElementById('refine-error');
      const refineContextLabel = document.getElementById('refine-context');
      let alignFrame = null;
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
        if (event.key === 'Escape' && refineModal && !refineModal.classList.contains('hidden')) {
          closeRefineModal();
        }
      });
      if (refineForm) {
        refineForm.addEventListener('submit', (event) => {
          event.preventDefault();
          if (!state.selectedPath) {
            setRefineError('Select a chapter before applying overrides.');
            return;
          }
          const pattern = refinePatternInput ? refinePatternInput.value.trim() : '';
          if (!pattern) {
            setRefineError('Pattern is required.');
            return;
          }
          const replacement = refineReplacementInput ? refineReplacementInput.value.trim() : '';
          const reading = refineReadingInput ? refineReadingInput.value.trim() : '';
          const surface = refineSurfaceInput ? refineSurfaceInput.value.trim() : '';
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
          const payload = {
            path: state.selectedPath,
            pattern,
            regex: Boolean(refineRegexInput && refineRegexInput.checked),
          };
          if (replacement) payload.replacement = replacement;
          if (reading) payload.reading = reading;
          if (surface) payload.surface = surface;
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
              const chapterLabel = result && result.chapter ? result.chapter : state.selectedPath;
              renderStatus(`Refined ${updated} chapter(s) for ${chapterLabel}.`);
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

      function renderStatus(text) {
        statusEl.textContent = text;
      }

      function setRefineError(message) {
        if (refineError) {
          refineError.textContent = message || '';
        }
      }

      function setRefineBusy(busy) {
        refineBusy = busy;
        if (!refineForm) return;
        const controls = refineForm.querySelectorAll('input, button, textarea');
        controls.forEach((control) => {
          if (control === refineCancel) {
            control.disabled = false;
          } else {
            control.disabled = busy;
          }
        });
        if (refineSubmit) {
          refineSubmit.textContent = busy ? 'Saving…' : 'Apply override';
        }
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
        const defaultPattern =
          view === 'transformed' && chunk
            ? chunk
            : (token.reading || chunk || token.surface || '');
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
          refineSurfaceInput.value = token.surface || '';
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
          const surfaceLabel = token.surface || chunk || '';
          const readingLabel = token.reading || '';
          if (surfaceLabel || readingLabel) {
            parts.push(`${surfaceLabel || '—'} → ${readingLabel || '—'}`);
          }
          refineContextLabel.textContent = parts.join(' · ');
        }
        refineContext = { token, view, text: chunk };
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

      function attachHighlightHandlers(element, index) {
        element.addEventListener('mouseenter', () => setHighlighted(index));
        element.addEventListener('mouseleave', () => setHighlighted(null));
        element.addEventListener('focus', () => setHighlighted(index));
        element.addEventListener('blur', () => setHighlighted(null));
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
        let cursor = 0;
        const segments = [];
        const ordered = tokens
          .map((token, index) => ({
            token,
            index,
            start: offsetValue(token.start, key),
            end: offsetValue(token.end, key),
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

        const pushTextSegment = (value) => {
          if (value) {
            segments.push({ type: 'text', text: value });
          }
        };
        const pushTokenSegment = (value, entry) => {
          if (value) {
            segments.push({
              type: 'token',
              text: value,
              token: entry.token,
              index: entry.index,
            });
          }
        };

        ordered.forEach((entry) => {
          const start = Math.max(entry.start, cursor);
          if (start > cursor) {
            pushTextSegment(text.slice(cursor, start));
          }
          const end = Math.max(start, entry.end);
          if (end > start) {
            pushTokenSegment(text.slice(start, end), entry);
          }
          cursor = Math.max(cursor, entry.end);
        });

        if (cursor < length) {
          pushTextSegment(text.slice(cursor));
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

        const appendSegmentContent = (segment, chunk) => {
          if (!chunk) {
            return;
          }
          if (segment.type === 'token') {
            const span = document.createElement('span');
            span.className = 'token-chunk';
            span.dataset.tokenIndex = String(segment.index);
            span.title = `${segment.token.surface || ''} → ${segment.token.reading || ''}`;
             span.tabIndex = 0;
            const annotation = isOriginalView ? (segment.token.reading || '') : (segment.token.surface || '');
            const trimmedAnnotation = annotation && annotation.trim();
            if (trimmedAnnotation) {
              const ruby = document.createElement('ruby');
              const rb = document.createElement('span');
              rb.textContent = chunk;
              ruby.appendChild(rb);
              const rt = document.createElement('rt');
              rt.textContent = annotation;
              ruby.appendChild(rt);
              span.appendChild(ruby);
            } else {
              span.textContent = chunk;
            }
            attachHighlightHandlers(span, segment.index);
            span.addEventListener('click', (event) => {
              event.preventDefault();
              openRefineModal({
                token: segment.token,
                text: chunk,
                view: isOriginalView ? 'original' : 'transformed',
              });
            });
            span.addEventListener('keydown', (event) => {
              if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                openRefineModal({
                  token: segment.token,
                  text: chunk,
                  view: isOriginalView ? 'original' : 'transformed',
                });
              }
            });
            currentLineNodes.push(span);
          } else {
            currentLineNodes.push(document.createTextNode(chunk));
          }
        };

        segments.forEach((segment) => {
          let start = 0;
          while (start <= segment.text.length) {
            const newlineIndex = segment.text.indexOf('\\n', start);
            if (newlineIndex === -1) {
              const finalChunk = segment.text.slice(start);
              if (finalChunk) {
                appendSegmentContent(segment, finalChunk);
              }
              break;
            }
            const chunk = segment.text.slice(start, newlineIndex);
            if (chunk) {
              appendSegmentContent(segment, chunk);
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
        const formData = new FormData();
        formData.append('file', file, file.name);
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
              const detail = payload && payload.detail;
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
      }

      function clearSelection() {
        metaPanel.hidden = true;
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
        closeRefineModal();
        setHighlighted(null);
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
        updateMetaPanel(payload);
        renderOriginalText(payload, tokens);
        renderTransformedText(payload, tokens);
        scheduleAlignLines();
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

      window.addEventListener('hashchange', handleHashNavigation);

      clearSelection();
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
            if path.name.endswith(".original.txt"):
                continue
            yield path
    # Include any loose .txt files directly under the root (rare).
    for path in sorted(root.glob("*.txt")):
        if not path.is_file() or path.name.endswith(".original.txt"):
            continue
        yield path


def _relative_to_root(root: Path, path: Path) -> Path:
    try:
        return path.resolve().relative_to(root)
    except ValueError as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=400, detail="Invalid chapter path") from exc


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
    return {
        "surface": surface,
        "reading": reading,
        "pos": pos,
        "accent": accent,
        "connection": connection,
        "sources": normalized_sources,
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

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        return HTMLResponse(INDEX_HTML)

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
    async def api_upload_epub(file: UploadFile = File(...)) -> JSONResponse:
        filename = file.filename or "upload.epub"
        suffix = Path(filename).suffix.lower()
        if suffix != ".epub":
            raise HTTPException(
                status_code=400, detail="Only .epub files are supported."
            )
        job = UploadJob(resolved_root, filename)
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

        text = _safe_read_text(chapter_path) if include_transformed else None
        text_length = len(text) if text is not None else None
        original_path = chapter_path.with_name(f"{chapter_path.stem}.original.txt")
        original_text = _safe_read_text(original_path)
        original_length = len(original_text) if original_text is not None else None

        token_path = chapter_path.with_name(chapter_path.name + ".token.json")
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
        pattern = payload.get("pattern")
        if not isinstance(pattern, str) or not pattern.strip():
            raise HTTPException(status_code=400, detail="pattern is required.")
        entry: dict[str, object] = {"pattern": pattern.strip()}
        if payload.get("regex"):
            entry["regex"] = True
        replacement = payload.get("replacement")
        if isinstance(replacement, str) and replacement.strip():
            entry["replacement"] = replacement.strip()
        reading = payload.get("reading")
        if isinstance(reading, str) and reading.strip():
            entry["reading"] = reading.strip()
        surface = payload.get("surface")
        if isinstance(surface, str) and surface.strip():
            entry["surface"] = surface.strip()
        pos = payload.get("pos")
        if isinstance(pos, str) and pos.strip():
            entry["pos"] = pos.strip()
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
        if accent is not None:
            entry["accent"] = accent
        try:
            override_path = append_override_entry(book_dir, entry)
            overrides = load_override_config(book_dir)
            updated = refine_book(book_dir, overrides)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        override_rel = _relative_to_root(resolved_root, override_path)
        return JSONResponse(
            {
                "updated": updated,
                "override_path": override_rel.as_posix(),
                "chapter": rel_path.as_posix(),
            }
        )

    return app
