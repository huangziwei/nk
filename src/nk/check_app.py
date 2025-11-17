from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse

INDEX_HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <title>nk Token Inspector</title>
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
    }
    @media (max-width: 900px) {
      .app {
        grid-template-columns: 1fr;
      }
      aside {
        position: sticky;
        top: 0;
        z-index: 20;
      }
    }
    aside {
      background: var(--sidebar);
      border-right: 1px solid var(--outline);
      padding: 1.2rem 1rem;
      display: flex;
      flex-direction: column;
      gap: 1rem;
      min-height: 100vh;
    }
    aside h1 {
      margin: 0;
      font-size: 1.25rem;
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
      text-align: left;
      color: var(--text);
      cursor: pointer;
      transition: background 0.15s ease, border 0.15s ease;
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
    main {
      padding: 1.4rem;
      display: flex;
      flex-direction: column;
      gap: 1.2rem;
    }
    .status {
      color: var(--muted);
      font-size: 0.95rem;
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
      grid-template-columns: 3rem 1fr;
      gap: 0.75rem;
      align-items: flex-start;
      font-size: 0.9rem;
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
  </style>
</head>
<body>
  <div class="app">
    <aside>
      <div>
        <h1>nk token check</h1>
        <p style="margin:0.35rem 0 0;font-size:0.85rem;color:var(--muted);">
          Select a chapter and hover tokens to compare transformed vs original offsets.
        </p>
      </div>
      <div class="filter">
        <input type="search" placeholder="Filter chapters…" id="chapter-filter">
        <button id="refresh">↻</button>
      </div>
      <ul class="chapter-list" id="chapter-list"></ul>
    </aside>
    <main id="details">
      <div class="status" id="status">Loading chapters…</div>
      <section class="panel text-grid" id="text-grid">
        <div class="text-controls">
          <label class="toggle">
            <input type="checkbox" id="toggle-original" checked>
            <span>Original text</span>
          </label>
          <label class="toggle">
            <input type="checkbox" id="toggle-transformed" checked>
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
  <script>
    (() => {
      const state = {
        chapters: [],
        filtered: [],
        selectedPath: null,
        tokens: [],
        filterValue: '',
        activeToken: null,
      };
      const baseTitle = document.title || 'nk Token Inspector';

      const listEl = document.getElementById('chapter-list');
      const filterEl = document.getElementById('chapter-filter');
      const refreshBtn = document.getElementById('refresh');
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
      const lineRegistry = {
        transformed: [],
        original: [],
      };
      let alignFrame = null;

      function setDocumentTitle(label) {
        document.title = label ? `${label} – ${baseTitle}` : baseTitle;
      }

      function renderStatus(text) {
        statusEl.textContent = text;
      }

      function fetchJSON(url) {
        return fetch(url).then((res) => {
          if (!res.ok) {
            throw new Error(`Request failed: ${res.status}`);
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

      function bindPanelToggle(panel, toggle) {
        if (!panel || !toggle) return;
        updatePanelVisibility(panel, toggle);
        toggle.addEventListener('change', () => updatePanelVisibility(panel, toggle));
      }

      function resetScrollPositions() {
        if (transformedText) transformedText.scrollTop = 0;
        if (originalText) originalText.scrollTop = 0;
      }

      let scrollSyncLock = false;
      function syncScroll(source, target) {
        if (!source || !target) return;
        if (scrollSyncLock) return;
        scrollSyncLock = true;
        target.scrollTop = source.scrollTop;
        scrollSyncLock = false;
      }

      bindPanelToggle(transformedPanel, toggleTransformed);
      bindPanelToggle(originalPanel, toggleOriginal);

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

      function describeSpan(token, key) {
        const start = offsetValue(token.start, key);
        const end = offsetValue(token.end, key);
        if (!Number.isFinite(start) || !Number.isFinite(end)) {
          return '—';
        }
        return `${start} – ${end}`;
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

      function updateTextMeta(element, textValue, hasFile) {
        if (!textValue) {
          element.textContent = hasFile ? 'Empty text' : 'Missing';
          element.className = hasFile ? 'pill warn' : 'pill missing';
          return;
        }
        element.textContent = `${textValue.length.toLocaleString()} chars`;
        element.className = 'pill ok';
      }

      function updateMetaPanel(payload) {
        const chapter = payload.chapter || {};
        const tokenVersion = payload.token_version ?? '—';
        const textLength = typeof payload.text_length === 'number' ? payload.text_length : (payload.text ? payload.text.length : 0);
        const originalLength = typeof payload.original_length === 'number' ? payload.original_length : (payload.original_text ? payload.original_text.length : 0);
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

      function renderChapterList() {
        listEl.innerHTML = '';
        if (!state.filtered.length) {
          const empty = document.createElement('li');
          empty.textContent = state.chapters.length ? 'No matches.' : 'No chapters found.';
          empty.style.color = 'var(--muted)';
          listEl.appendChild(empty);
          return;
        }
        state.filtered.forEach((chapter) => {
          const item = document.createElement('li');
          const button = document.createElement('button');
          button.type = 'button';
          button.className = 'chapter' + (state.selectedPath === chapter.path ? ' active' : '');
          button.dataset.path = chapter.path;
          const name = document.createElement('div');
          name.className = 'name';
          name.textContent = chapter.book ? `${chapter.book} / ${chapter.name}` : chapter.name;
          const meta = document.createElement('div');
          meta.className = 'meta';
          const tokenFlag = chapter.has_token ? 'token✓' : 'token×';
          const origFlag = chapter.has_original ? 'orig✓' : 'orig×';
          meta.textContent = `${tokenFlag} · ${origFlag} · ${formatBytes(chapter.size)} · ${formatDate(chapter.modified)}`;
          button.appendChild(name);
          button.appendChild(meta);
          button.addEventListener('click', () => {
            openChapter(chapter.path);
          });
          item.appendChild(button);
          listEl.appendChild(item);
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
        state.tokens = [];
        setHighlighted(null);
      }

      function openChapter(path) {
        state.selectedPath = path;
        renderChapterList();
        renderStatus(`Loading ${path}…`);
        fetchJSON(`/api/chapter?path=${encodeURIComponent(path)}`)
          .then((payload) => {
            const tokens = Array.isArray(payload.tokens) ? payload.tokens : [];
            state.tokens = tokens;
            setHighlighted(null);
            const chapter = payload.chapter || {};
            const chapterName = chapter.name || null;
            const displayName = payload.name || chapterName || path;
            renderStatus(`Loaded ${displayName} (${tokens.length} tokens)`);
            setDocumentTitle(displayName);
            updateMetaPanel(payload);
            updateTextMeta(transformedMeta, payload.text, true);
            const transformedLines = renderTextView(transformedText, payload.text, tokens, 'transformed');
            const hasOriginalFile = Boolean(payload.original_path);
            updateTextMeta(originalMeta, payload.original_text, hasOriginalFile);
            const originalLines = renderTextView(originalText, payload.original_text, tokens, 'original');
            setLineRegistry('transformed', transformedLines);
            setLineRegistry('original', originalLines);
            scheduleAlignLines();
            resetScrollPositions();
          })
          .catch((err) => {
            console.error(err);
            state.selectedPath = null;
            renderChapterList();
            renderStatus(`Failed to load ${path}: ${err.message}`);
            clearSelection();
          });
      }

      function loadChapters() {
        renderStatus('Loading chapters…');
        fetchJSON('/api/chapters')
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

      filterEl.addEventListener('input', (event) => {
        state.filterValue = event.target.value;
        applyFilter();
      });

      refreshBtn.addEventListener('click', () => {
        loadChapters();
      });

      clearSelection();
      loadChapters();
    })();
  </script>
</body>
</html>
"""


def _iter_chapter_files(root: Path) -> Iterable[Path]:
    for path in sorted(root.rglob("*.txt")):
        if not path.is_file():
            continue
        if path.name.endswith(".original.txt"):
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


def _list_chapters(root: Path) -> list[dict[str, object]]:
    return [_chapter_entry(root, path) for path in _iter_chapter_files(root)]


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
        start_original_value = start_original if isinstance(start_original, int) else None
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
        normalized_sources = [str(source) for source in sources if isinstance(source, str) and source]
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
            "transformed": transformed_start if isinstance(transformed_start, int) else None,
        },
        "end": {
            "original": end_original_value,
            "transformed": transformed_end if isinstance(transformed_end, int) else None,
        },
    }


def _load_token_payload(path: Path) -> tuple[list[dict[str, object]], dict[str, object] | None, str | None]:
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


def create_check_app(root: Path) -> FastAPI:
    resolved_root = root.expanduser().resolve()
    if not resolved_root.exists() or not resolved_root.is_dir():
        raise FileNotFoundError(f"Root not found: {resolved_root}")

    app = FastAPI(title="nk Token Inspector")
    app.state.root = resolved_root

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        return HTMLResponse(INDEX_HTML)

    @app.get("/api/chapters")
    def api_chapters() -> JSONResponse:
        chapters = _list_chapters(resolved_root)
        return JSONResponse({"root": resolved_root.as_posix(), "chapters": chapters})

    @app.get("/api/chapter")
    def api_chapter(path: str = Query(..., description="Relative path to a .txt file")) -> JSONResponse:
        if not path:
            raise HTTPException(status_code=400, detail="Path is required")
        rel_path = Path(path)
        if rel_path.is_absolute():
            raise HTTPException(status_code=400, detail="Path must be relative to the root")
        chapter_path = (resolved_root / rel_path).resolve()
        try:
            chapter_path.relative_to(resolved_root)
        except ValueError:
            raise HTTPException(status_code=400, detail="Path escapes the root directory")
        if not chapter_path.exists() or not chapter_path.is_file():
            raise HTTPException(status_code=404, detail="Chapter not found")
        if chapter_path.suffix.lower() != ".txt":
            raise HTTPException(status_code=400, detail="Only .txt files are supported")

        text = _safe_read_text(chapter_path)
        original_path = chapter_path.with_name(f"{chapter_path.stem}.original.txt")
        original_text = _safe_read_text(original_path)

        token_path = chapter_path.with_name(chapter_path.name + ".token.json")
        tokens_list, token_payload, token_error = _load_token_payload(token_path)

        chapter_entry = _chapter_entry(resolved_root, chapter_path)
        response = {
            "chapter": chapter_entry,
            "name": chapter_path.name,
            "text": text,
            "text_length": len(text) if text else 0,
            "original_text": original_text,
            "original_length": len(original_text) if original_text else 0,
            "tokens": tokens_list,
            "token_version": token_payload.get("version") if isinstance(token_payload, Mapping) else None,
            "token_sha1": token_payload.get("text_sha1") if isinstance(token_payload, Mapping) else None,
            "token_path": _relative_to_root(resolved_root, token_path).as_posix() if token_path.exists() else None,
            "token_error": token_error,
            "original_path": _relative_to_root(resolved_root, original_path).as_posix() if original_path.exists() else None,
        }
        return JSONResponse(response)

    return app
