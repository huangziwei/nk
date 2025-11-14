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
VOICEVOX_ARCHIVE_PATH=""

BREW_DEPS=(curl ffmpeg jq p7zip uv)
APT_PACKAGES=(curl ffmpeg jq p7zip-full libasound2-dev)
REQUIRED_COMMANDS=(curl ffmpeg jq 7z uv)

PACKAGE_MANAGER=""
SUDO_CMD=""
LOCAL_BIN="$HOME/.local/bin"

if [[ -d "$LOCAL_BIN" && ":$PATH:" != *":$LOCAL_BIN:"* ]]; then
  export PATH="$LOCAL_BIN:$PATH"
fi

log() {
  echo "[nk install] $*" >&2
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

sync_python_dependencies() {
  log "Syncing Python environment with uv"
  (cd "$ROOT_DIR" && uv sync)
}

install_unidic() {
  log "Ensuring UniDic 3.1.1 is installed"
  (
    cd "$ROOT_DIR"
    uv run nk tools install-unidic
  )
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
  detect_package_manager
  configure_sudo_helper
  install_system_deps
  install_uv_if_missing
  verify_required_commands "${REQUIRED_COMMANDS[@]}"
  sync_python_dependencies
  install_unidic
  install_voicevox

  log "All dependencies installed. Activate the virtualenv with 'source .venv/bin/activate' or run commands via 'uv run nk ...'."
}

main "$@"
