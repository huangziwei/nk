#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VOICEVOX_ROOT="${VOICEVOX_ROOT:-"$HOME/opt/voicevox"}"

UNAME_OUT="$(uname -s)"
case "$UNAME_OUT" in
  Darwin)
    DEFAULT_VOICEVOX_TARGET="macos-x64"
    ;;
  Linux)
    DEFAULT_VOICEVOX_TARGET="linux-x64"
    ;;
  *)
    echo "Unsupported operating system: $UNAME_OUT" >&2
    exit 1
    ;;
esac

VOICEVOX_TARGET="${VOICEVOX_TARGET:-$DEFAULT_VOICEVOX_TARGET}"
VOICEVOX_INSTALL_DIR="$VOICEVOX_ROOT/$VOICEVOX_TARGET"
VOICEVOX_ASSET_PATTERN="${VOICEVOX_ASSET_PATTERN:-voicevox_engine-${VOICEVOX_TARGET}.*\\.7z\\.001$}"
VOICEVOX_VERSION="${VOICEVOX_VERSION:-latest}"
VOICEVOX_FORCE="${VOICEVOX_FORCE:-0}"
NK_SKIP_VOICEVOX="${NK_SKIP_VOICEVOX:-0}"
VOICEVOX_API="${VOICEVOX_API:-https://api.github.com/repos/VOICEVOX/voicevox_engine/releases}"
VOICEVOX_URL="${VOICEVOX_URL:-}"
VOICEVOX_RELEASE_TAG=""
VOICEVOX_ARCHIVE_PATH=""

BREW_DEPS=(curl ffmpeg jq p7zip uv)
REQUIRED_COMMANDS=(curl ffmpeg jq 7z uv)

log() {
  echo "[nk install] $*" >&2
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: $1 is required but not installed. Please install it and re-run install.sh." >&2
    exit 1
  fi
}

install_brew_deps() {
  for dep in "${BREW_DEPS[@]}"; do
    if brew ls --versions "$dep" >/dev/null 2>&1; then
      local outdated
      outdated="$(brew outdated "$dep" 2>/dev/null || true)"
      if [[ -n "$outdated" ]]; then
        log "Upgrading Homebrew package: $dep"
        brew upgrade "$dep"
      else
        log "Homebrew package already up to date: $dep"
      fi
    else
      log "Installing Homebrew package: $dep"
      brew install "$dep"
    fi
  done
}

verify_required_commands() {
  local missing=()

  for cmd in "$@"; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      missing+=("$cmd")
    fi
  done

  if ((${#missing[@]} > 0)); then
    echo "Error: Missing required commands after Homebrew install: ${missing[*]}" >&2
    echo "Please ensure Homebrew packages are on your PATH and rerun install.sh." >&2
    exit 1
  fi
}

sync_python_dependencies() {
  log "Syncing Python environment with uv"
  (cd "$ROOT_DIR" && uv sync)
}

install_unidic() {
  log "Ensuring UniDic 3.1.1 is installed"
  (
    cd "$ROOT_DIR"
    uv run nk tools install-unidic
  ) | while IFS= read -r line || [[ -n "$line" ]]; do
    log "$line"
  done
}

download_voicevox_release() {
  local target_api asset_url asset_name release_json tag

  if [[ -n "$VOICEVOX_URL" ]]; then
    asset_url="$VOICEVOX_URL"
    asset_name="$(basename "$VOICEVOX_URL")"
    tag="${VOICEVOX_VERSION:-custom}"
  else
    if [[ "$VOICEVOX_VERSION" == "latest" ]]; then
      target_api="$VOICEVOX_API/latest"
    else
      target_api="$VOICEVOX_API/tags/$VOICEVOX_VERSION"
    fi

    log "Fetching VoiceVox metadata from $target_api"
    release_json="$(curl -fsSL "$target_api")"
    tag="$(echo "$release_json" | jq -r '.tag_name')"
    if [[ -z "$tag" || "$tag" == "null" ]]; then
      echo "Unable to determine VoiceVox release tag (API: $target_api)" >&2
      exit 1
    fi
    asset_url="$(echo "$release_json" | jq -r --arg pattern "$VOICEVOX_ASSET_PATTERN" '.assets[] | select(.name | test($pattern)) | .browser_download_url' | head -n 1)"
    asset_name="$(echo "$release_json" | jq -r --arg pattern "$VOICEVOX_ASSET_PATTERN" '.assets[] | select(.name | test($pattern)) | .name' | head -n 1)"
    if [[ -z "$asset_url" || "$asset_url" == "null" ]]; then
      echo "Could not find a VoiceVox engine asset matching pattern '$VOICEVOX_ASSET_PATTERN' in release $tag" >&2
      exit 1
    fi
  fi

  local temp_dir download_path default_asset_name
  temp_dir="$(mktemp -d)"
  default_asset_name="voicevox_engine-${VOICEVOX_TARGET}.7z.001"
  download_path="$temp_dir/${asset_name:-$default_asset_name}"

  VOICEVOX_RELEASE_TAG="$tag"
  local display_name="${asset_name:-$default_asset_name}"
  log "Downloading VoiceVox engine release: $tag ($display_name)"
  curl -fL "$asset_url" -o "$download_path"

  VOICEVOX_ARCHIVE_PATH="$download_path"
}

extract_voicevox_archive() {
  local archive_path="$1"
  local temp_extract
  temp_extract="$(mktemp -d)"

  log "Extracting VoiceVox archive"
  7z x "$archive_path" -o"$temp_extract" >/dev/null
  echo "$temp_extract"
}

finalize_voicevox_install() {
  local extract_dir="$1"
  local install_dir="$VOICEVOX_INSTALL_DIR"

  mkdir -p "$VOICEVOX_ROOT"
  rm -rf "$install_dir"

  local candidate=""
  if [[ -d "$extract_dir/$VOICEVOX_TARGET" ]]; then
    candidate="$extract_dir/$VOICEVOX_TARGET"
  else
    candidate="$(find "$extract_dir" -maxdepth 1 -type d -name "voicevox_engine-${VOICEVOX_TARGET}*" | head -n 1 || true)"
  fi

  if [[ -z "$candidate" ]]; then
    local run_file
    run_file="$(find "$extract_dir" -type f -name run | head -n 1 || true)"
    if [[ -n "$run_file" ]]; then
      candidate="$(dirname "$run_file")"
    fi
  fi

  if [[ -z "$candidate" || ! -d "$candidate" ]]; then
    echo "Failed to locate VoiceVox engine directory inside archive" >&2
    exit 1
  fi

  mv "$candidate" "$install_dir"
  chmod +x "$install_dir/run"

  if [[ -n "${VOICEVOX_RELEASE_TAG:-}" && "$VOICEVOX_RELEASE_TAG" != "null" ]]; then
    echo "$VOICEVOX_RELEASE_TAG" > "$install_dir/.nk-voicevox-version"
  fi

  local tag_note="${VOICEVOX_RELEASE_TAG:-unknown}"
  log "VoiceVox runtime installed at $install_dir (version: $tag_note, run binary: $install_dir/run)"
}

install_voicevox() {
  if [[ "$NK_SKIP_VOICEVOX" == "1" ]]; then
    log "Skipping VoiceVox download (NK_SKIP_VOICEVOX=1)"
    return
  fi

  if [[ -x "$VOICEVOX_INSTALL_DIR/run" && "$VOICEVOX_FORCE" != "1" ]]; then
    log "VoiceVox already installed at $VOICEVOX_INSTALL_DIR (set VOICEVOX_FORCE=1 to reinstall)"
    return
  fi

  local archive_path extract_dir
  download_voicevox_release
  archive_path="$VOICEVOX_ARCHIVE_PATH"
  if [[ -z "$archive_path" || ! -f "$archive_path" ]]; then
    echo "VoiceVox archive download failed (archive path not found)" >&2
    exit 1
  fi
  extract_dir="$(extract_voicevox_archive "$archive_path")"
  if [[ -z "$extract_dir" || ! -d "$extract_dir" ]]; then
    echo "VoiceVox extraction failed (no directory created)" >&2
    exit 1
  fi
  finalize_voicevox_install "$extract_dir"
  rm -rf "$(dirname "$archive_path")" "$extract_dir"
}

main() {
  require_cmd brew

  install_brew_deps
  verify_required_commands "${REQUIRED_COMMANDS[@]}"
  sync_python_dependencies
  install_unidic
  install_voicevox

  log "All dependencies installed. Activate the virtualenv with 'source .venv/bin/activate' or run commands via 'uv run nk ...'."
}

main "$@"
