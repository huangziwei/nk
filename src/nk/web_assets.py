from __future__ import annotations

from urllib.parse import quote


def build_favicon_svg(
    label: str = "nk",
    *,
    background: str = "#080b16",
    text_color: str = "#f8fafc",
    border_color: str | None = "#ffffff1a",
) -> str:
    """Return a minimalist square SVG badge."""
    normalized = (label or "nk").strip() or "nk"
    normalized = normalized[:2]
    font_size = "26" if len(normalized) > 1 else "32"
    text_y = "40"
    border_markup = (
        f'<rect x="1.5" y="1.5" width="61" height="61" rx="12" ry="12" fill="none" '
        f'stroke="{border_color}" stroke-width="1.5" />'
        if border_color
        else ""
    )
    return f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" role="img" aria-label="{normalized} icon">
  <rect width="64" height="64" rx="14" ry="14" fill="{background}" />
  {border_markup}
  <text x="32" y="{text_y}" text-anchor="middle" font-family="Inter, 'Hiragino Sans', 'Segoe UI', sans-serif"
        font-size="{font_size}" font-weight="700" fill="{text_color}">{normalized}</text>
</svg>"""


def favicon_data_url(
    label: str = "nk",
    *,
    background: str = "#080b16",
    text_color: str = "#f8fafc",
    border_color: str | None = "#ffffff1a",
) -> str:
    """Build a favicon SVG and wrap it in a data URL for inline use."""
    svg = build_favicon_svg(
        label=label,
        background=background,
        text_color=text_color,
        border_color=border_color,
    )
    return "data:image/svg+xml," + quote(svg)


NK_FAVICON_URL = favicon_data_url()


__all__ = ["build_favicon_svg", "favicon_data_url", "NK_FAVICON_URL"]
