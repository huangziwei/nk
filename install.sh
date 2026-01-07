#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VOICEVOX_ROOT="${VOICEVOX_ROOT:-"$HOME/opt/voicevox"}"

UNAME_OUT="$(uname -s)"
LINUX_DISTRO=""
case "$UNAME_OUT" in
  Darwin)
    DEFAULT_VOICEVOX_TARGET="macos-x64"
    ;;
  Linux)
    if [[ -r /etc/os-release ]]; then
      # shellcheck disable=SC1091
      . /etc/os-release
    fi
    if [[ "${ID:-}" == "ubuntu" || "${ID_LIKE:-}" == *"ubuntu"* ]]; then
      LINUX_DISTRO="ubuntu"
      DEFAULT_VOICEVOX_TARGET="linux-cpu-x64"
    else
      echo "Unsupported Linux distribution. Only Ubuntu is supported by install.sh." >&2
      exit 1
    fi
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
VOICEVOX_RELEASE_API=""
VOICEVOX_RELEASE_JSON=""
VOICEVOX_ARCHIVE_PATH=""
UNIDIC_VERSION="${UNIDIC_VERSION:-3.1.1}"
UNIDIC_URL="${UNIDIC_URL:-https://clrd.ninjal.ac.jp/unidic_archive/cwj/3.1.1/unidic-cwj-3.1.1-full.zip}"
UNIDIC_ROOT="${UNIDIC_ROOT:-"$HOME/opt/unidic"}"
UNIDIC_INSTALL_DIR="$UNIDIC_ROOT/$UNIDIC_VERSION"
UNIDIC_FORCE="${UNIDIC_FORCE:-0}"
UNIDIC_ZIP="${UNIDIC_ZIP:-}"
NK_SKIP_UNIDIC="${NK_SKIP_UNIDIC:-0}"
UNIDIC_DOWNLOAD_TMP=""

NK_STATE_DIR="${NK_STATE_DIR:-"$HOME/.local/share/nk"}"
NK_STATE_FILE="$NK_STATE_DIR/deps-manifest.json"
NK_OPT_ROOT="${NK_OPT_ROOT:-"$HOME/opt"}"

BREW_DEPS=(curl ffmpeg jq p7zip uv)
APT_PACKAGES=(curl ffmpeg jq p7zip-full libasound2-dev)
REQUIRED_COMMANDS=(curl ffmpeg jq 7z uv)

PACKAGE_MANAGER=""
SUDO_CMD=""
LOCAL_BIN="$HOME/.local/bin"

UNIDIC_INSTALLED_BY_NK=0
UNIDIC_SYMLINK_PATH="$UNIDIC_ROOT/current"
UNIDIC_PATH="$UNIDIC_INSTALL_DIR"
UNIDIC_ROOT_CREATED=0

VOICEVOX_INSTALLED_BY_NK=0
VOICEVOX_PATH="$VOICEVOX_INSTALL_DIR"
VOICEVOX_VERSION_NOTE=""
VOICEVOX_ROOT_CREATED=0

NK_OPT_ROOT_CREATED=0

if [[ -d "$LOCAL_BIN" && ":$PATH:" != *":$LOCAL_BIN:"* ]]; then
  export PATH="$LOCAL_BIN:$PATH"
fi

log() {
  echo "[nk install] $*" >&2
}

maybe_mark_opt_root_created() {
  local target="$1"
  if [[ "$NK_OPT_ROOT_CREATED" == "1" ]]; then
    return
  fi
  case "$target" in
    "$NK_OPT_ROOT"|"$NK_OPT_ROOT"/*)
      if [[ ! -d "$NK_OPT_ROOT" ]]; then
        NK_OPT_ROOT_CREATED=1
      fi
      ;;
  esac
}

run_with_sudo() {
  if [[ -n "$SUDO_CMD" ]]; then
    "$SUDO_CMD" "$@"
  else
    "$@"
  fi
}

detect_package_manager() {
  if command -v brew >/dev/null 2>&1; then
    PACKAGE_MANAGER="brew"
    return
  fi

  if [[ "$UNAME_OUT" == "Darwin" ]]; then
    echo "Homebrew is required on macOS. Install it from https://brew.sh/ and re-run install.sh." >&2
    exit 1
  fi

  if [[ "$LINUX_DISTRO" == "ubuntu" && -x /usr/bin/apt-get ]]; then
    PACKAGE_MANAGER="apt-get"
    return
  fi

  echo "Could not detect a supported package manager. On macOS install Homebrew; on Ubuntu ensure apt-get is available." >&2
  exit 1
}

configure_sudo_helper() {
  if [[ "$PACKAGE_MANAGER" == "brew" ]]; then
    SUDO_CMD=""
    return
  fi

  if [[ "$EUID" -eq 0 ]]; then
    SUDO_CMD=""
    return
  fi

  if command -v sudo >/dev/null 2>&1; then
    SUDO_CMD="sudo"
    return
  fi

  echo "Installing packages with apt-get requires root privileges. Re-run install.sh as root or ensure sudo is available." >&2
  exit 1
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

install_apt_deps() {
  log "Installing packages via apt-get: ${APT_PACKAGES[*]}"
  run_with_sudo apt-get update
  run_with_sudo apt-get install -y "${APT_PACKAGES[@]}"
}

install_system_deps() {
  case "$PACKAGE_MANAGER" in
    brew)
      install_brew_deps
      ;;
    apt-get)
      install_apt_deps
      ;;
    *)
      echo "Unsupported package manager: $PACKAGE_MANAGER" >&2
      exit 1
      ;;
  esac
}

verify_required_commands() {
  local missing=()

  for cmd in "$@"; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      missing+=("$cmd")
    fi
  done

  if ((${#missing[@]} > 0)); then
    echo "Error: Missing required commands after the dependency install step: ${missing[*]}" >&2
    echo "Ensure they are on your PATH (or install them manually) and rerun install.sh." >&2
    exit 1
  fi
}

install_uv_if_missing() {
  if command -v uv >/dev/null 2>&1; then
    return
  fi

  if [[ "$PACKAGE_MANAGER" == "brew" ]]; then
    echo "uv should have been installed via Homebrew but is still missing from PATH." >&2
    exit 1
  fi

  local installer_url="${UV_INSTALLER_URL:-https://astral.sh/uv/install.sh}"
  log "uv not found. Installing via upstream installer ($installer_url)"
  curl -LsSf "$installer_url" | sh

  if [[ -d "$LOCAL_BIN" && ":$PATH:" != *":$LOCAL_BIN:"* ]]; then
    export PATH="$LOCAL_BIN:$PATH"
  fi

  if ! command -v uv >/dev/null 2>&1; then
    echo "uv installation completed but the command is still unavailable. Add $LOCAL_BIN to your PATH or install uv manually." >&2
    exit 1
  fi
}

read_voicevox_version_marker() {
  local version_file="$VOICEVOX_INSTALL_DIR/.nk-voicevox-version"
  if [[ -f "$version_file" ]]; then
    head -n 1 "$version_file"
  fi
}

fetch_voicevox_release_json() {
  if [[ -n "$VOICEVOX_RELEASE_JSON" ]]; then
    echo "$VOICEVOX_RELEASE_JSON"
    return
  fi

  if [[ "$VOICEVOX_VERSION" == "latest" ]]; then
    VOICEVOX_RELEASE_API="$VOICEVOX_API/latest"
  else
    VOICEVOX_RELEASE_API="$VOICEVOX_API/tags/$VOICEVOX_VERSION"
  fi

  log "Fetching VoiceVox metadata from $VOICEVOX_RELEASE_API"
  VOICEVOX_RELEASE_JSON="$(curl -fsSL "$VOICEVOX_RELEASE_API")"
  if [[ -z "$VOICEVOX_RELEASE_JSON" ]]; then
    echo "Unable to fetch VoiceVox release metadata (API: $VOICEVOX_RELEASE_API)" >&2
    exit 1
  fi

  echo "$VOICEVOX_RELEASE_JSON"
}

resolve_voicevox_release_tag() {
  local release_json tag
  release_json="$(fetch_voicevox_release_json)"
  tag="$(printf '%s' "$release_json" | jq -r '.tag_name')"
  if [[ -z "$tag" || "$tag" == "null" ]]; then
    echo "Unable to determine VoiceVox release tag (API: $VOICEVOX_RELEASE_API)" >&2
    exit 1
  fi
  echo "$tag"
}

sync_python_dependencies() {
  if [[ ! -f "$ROOT_DIR/pyproject.toml" ]]; then
    log "Skipping uv sync (pyproject.toml not found at $ROOT_DIR)"
    return
  fi

  log "Syncing Python environment with uv"
  (cd "$ROOT_DIR" && uv sync)
}

download_unidic_archive() {
  UNIDIC_DOWNLOAD_TMP="$(mktemp -d)"
  local archive_path="$UNIDIC_DOWNLOAD_TMP/unidic-${UNIDIC_VERSION}.zip"
  log "Downloading UniDic $UNIDIC_VERSION"
  curl -fL "$UNIDIC_URL" -o "$archive_path"
  echo "$archive_path"
}

extract_unidic_archive() {
  local archive_path="$1"
  local extract_dir
  extract_dir="$(mktemp -d)"
  log "Extracting UniDic archive"
  7z x "$archive_path" -o"$extract_dir" >/dev/null
  echo "$extract_dir"
}

find_dic_root() {
  local base="$1"
  local dic_file
  dic_file="$(find "$base" -type f -name dicrc -print -quit 2>/dev/null)"
  if [[ -z "$dic_file" ]]; then
    return 1
  fi
  dirname "$dic_file"
}

install_unidic() {
  if [[ "$NK_SKIP_UNIDIC" == "1" ]]; then
    log "Skipping UniDic install (NK_SKIP_UNIDIC=1)"
    return
  fi

  if [[ -f "$UNIDIC_INSTALL_DIR/dicrc" && "$UNIDIC_FORCE" != "1" ]]; then
    log "UniDic already installed at $UNIDIC_INSTALL_DIR (set UNIDIC_FORCE=1 to reinstall)"
    ln -sfn "$UNIDIC_INSTALL_DIR" "$UNIDIC_ROOT/current"
    return
  fi

  maybe_mark_opt_root_created "$UNIDIC_ROOT"
  if [[ ! -d "$UNIDIC_ROOT" ]]; then
    UNIDIC_ROOT_CREATED=1
  fi
  mkdir -p "$UNIDIC_ROOT"

  local archive_path download_cleanup="0"
  if [[ -n "$UNIDIC_ZIP" ]]; then
    if [[ ! -f "$UNIDIC_ZIP" ]]; then
      echo "Specified UNIDIC_ZIP not found: $UNIDIC_ZIP" >&2
      exit 1
    fi
    archive_path="$UNIDIC_ZIP"
  else
    archive_path="$(download_unidic_archive)"
    download_cleanup="1"
  fi

  local extract_dir
  extract_dir="$(extract_unidic_archive "$archive_path")"
  local dic_root=""
  if ! dic_root="$(find_dic_root "$extract_dir")"; then
    echo "Failed to locate dicrc inside UniDic archive." >&2
    rm -rf "$extract_dir"
    if [[ "$download_cleanup" == "1" && -n "$UNIDIC_DOWNLOAD_TMP" ]]; then
      rm -rf "$UNIDIC_DOWNLOAD_TMP"
    fi
    exit 1
  fi

  rm -rf "$UNIDIC_INSTALL_DIR"
  mv "$dic_root" "$UNIDIC_INSTALL_DIR"
  ln -sfn "$UNIDIC_INSTALL_DIR" "$UNIDIC_ROOT/current"
  log "UniDic $UNIDIC_VERSION installed at $UNIDIC_INSTALL_DIR"

  if [[ "$download_cleanup" == "1" && -n "$UNIDIC_DOWNLOAD_TMP" ]]; then
    rm -rf "$UNIDIC_DOWNLOAD_TMP"
  fi
  rm -rf "$extract_dir"

  UNIDIC_INSTALLED_BY_NK=1
}

download_voicevox_release() {
  local asset_url asset_name release_json tag

  if [[ -n "$VOICEVOX_URL" ]]; then
    asset_url="$VOICEVOX_URL"
    asset_name="$(basename "$VOICEVOX_URL")"
    tag="${VOICEVOX_VERSION:-custom}"
  else
    release_json="$(fetch_voicevox_release_json)"
    tag="$(resolve_voicevox_release_tag)"
    asset_url="$(printf '%s' "$release_json" | jq -r --arg pattern "$VOICEVOX_ASSET_PATTERN" '.assets[] | select(.name | test($pattern)) | .browser_download_url' | head -n 1)"
    asset_name="$(printf '%s' "$release_json" | jq -r --arg pattern "$VOICEVOX_ASSET_PATTERN" '.assets[] | select(.name | test($pattern)) | .name' | head -n 1)"
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

  maybe_mark_opt_root_created "$VOICEVOX_ROOT"
  if [[ ! -d "$VOICEVOX_ROOT" ]]; then
    VOICEVOX_ROOT_CREATED=1
  fi
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
    if [[ -n "$VOICEVOX_URL" ]]; then
      log "VoiceVox already installed at $VOICEVOX_INSTALL_DIR (set VOICEVOX_FORCE=1 to reinstall)"
      VOICEVOX_VERSION_NOTE="$(read_voicevox_version_marker || true)"
      return
    fi

    local installed_tag desired_tag
    installed_tag="$(read_voicevox_version_marker || true)"
    desired_tag="$(resolve_voicevox_release_tag)"
    if [[ -n "$installed_tag" && "$installed_tag" == "$desired_tag" ]]; then
      log "VoiceVox already up to date at $VOICEVOX_INSTALL_DIR (version: $installed_tag)"
      VOICEVOX_VERSION_NOTE="$installed_tag"
      return
    fi

    local from_note="${installed_tag:-unknown}"
    log "Upgrading VoiceVox from $from_note to $desired_tag"
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

  VOICEVOX_INSTALLED_BY_NK=1
  VOICEVOX_VERSION_NOTE="${VOICEVOX_RELEASE_TAG:-}"
}

write_install_manifest() {
  mkdir -p "$NK_STATE_DIR"
  NK_STATE_FILE="$NK_STATE_FILE" \
  UNIDIC_INSTALLED_BY_NK="$UNIDIC_INSTALLED_BY_NK" \
  UNIDIC_PATH="$UNIDIC_PATH" \
  UNIDIC_SYMLINK_PATH="$UNIDIC_SYMLINK_PATH" \
  UNIDIC_VERSION="$UNIDIC_VERSION" \
  UNIDIC_ROOT_CREATED="$UNIDIC_ROOT_CREATED" \
  UNIDIC_ROOT="$UNIDIC_ROOT" \
  NK_OPT_ROOT="$NK_OPT_ROOT" \
  NK_OPT_ROOT_CREATED="$NK_OPT_ROOT_CREATED" \
  VOICEVOX_INSTALLED_BY_NK="$VOICEVOX_INSTALLED_BY_NK" \
  VOICEVOX_PATH="$VOICEVOX_PATH" \
  VOICEVOX_TARGET="$VOICEVOX_TARGET" \
  VOICEVOX_VERSION_NOTE="$VOICEVOX_VERSION_NOTE" \
  VOICEVOX_ROOT_CREATED="$VOICEVOX_ROOT_CREATED" \
  VOICEVOX_ROOT="$VOICEVOX_ROOT" \
  python3 - <<'PY'
import datetime
import json
import os
from pathlib import Path

state_file = Path(os.environ["NK_STATE_FILE"])
state_file.parent.mkdir(parents=True, exist_ok=True)

def _bool_env(name: str) -> bool:
    return os.environ.get(name) == "1"

manifest = {
    "version": 1,
    "generated_at": datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z"),
    "components": {
        "unidic": {
            "installed_by_nk": _bool_env("UNIDIC_INSTALLED_BY_NK"),
            "path": os.environ.get("UNIDIC_PATH", ""),
            "symlink": os.environ.get("UNIDIC_SYMLINK_PATH", ""),
            "version": os.environ.get("UNIDIC_VERSION", ""),
            "root_path": os.environ.get("UNIDIC_ROOT", ""),
            "root_created_by_nk": _bool_env("UNIDIC_ROOT_CREATED"),
        },
        "voicevox": {
            "installed_by_nk": _bool_env("VOICEVOX_INSTALLED_BY_NK"),
            "path": os.environ.get("VOICEVOX_PATH", ""),
            "target": os.environ.get("VOICEVOX_TARGET", ""),
            "version": os.environ.get("VOICEVOX_VERSION_NOTE", ""),
            "root_path": os.environ.get("VOICEVOX_ROOT", ""),
            "root_created_by_nk": _bool_env("VOICEVOX_ROOT_CREATED"),
        },
        "opt_root": {
            "path": os.environ.get("NK_OPT_ROOT", ""),
            "root_created_by_nk": _bool_env("NK_OPT_ROOT_CREATED"),
        },
        "system": {
            "installed_by_nk": False,
            "note": "nk does not remove system packages; uninstall manually if desired.",
        },
    },
}

with state_file.open("w", encoding="utf-8") as fh:
    json.dump(manifest, fh, indent=2)
PY
}

main() {
  detect_package_manager
  configure_sudo_helper
  install_system_deps
  install_uv_if_missing
  verify_required_commands "${REQUIRED_COMMANDS[@]}"
  sync_python_dependencies
  install_unidic
  install_voicevox
  if ! write_install_manifest; then
    log "Warning: failed to write install manifest to $NK_STATE_FILE"
  else
    log "Wrote install manifest to $NK_STATE_FILE"
  fi

  log "All dependencies installed. Activate the virtualenv with 'source .venv/bin/activate' or run commands via 'uv run nk ...'."
}

main "$@"
