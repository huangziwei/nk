from __future__ import annotations

import hashlib
import json
import math
import os
import wave
from io import BytesIO
from pathlib import Path

import pytest

from nk.book_io import TOKEN_METADATA_VERSION
from nk.tts import (
    TTSTarget,
    VoiceProfile,
    VoiceVoxClient,
    VoiceVoxRuntimeError,
    _prepare_voicevox_endpoint,
    _resolve_runtime_executable,
    _synthesize_target_with_client,
    _target_cache_dir,
    discover_voicevox_runtime,
    managed_voicevox_runtime,
    synthesize_texts_to_mp3,
)

_ARTIFACT_DIR = os.environ.get("NK_TEST_ARTIFACTS_DIR")


def _save_artifact(name: str, data: bytes) -> None:
    if not _ARTIFACT_DIR:
        return
    root = Path(_ARTIFACT_DIR)
    root.mkdir(parents=True, exist_ok=True)
    safe_name = name.replace("/", "_")
    (root / safe_name).write_bytes(data)


def _tone_wav_bytes(
    frequency: float = 440.0, duration: float = 0.25, sample_rate: int = 16000
) -> bytes:
    total_frames = int(duration * sample_rate)
    amplitude = 12000
    frames = bytearray()
    for idx in range(total_frames):
        value = int(amplitude * math.sin(2 * math.pi * frequency * (idx / sample_rate)))
        frames.extend(value.to_bytes(2, byteorder="little", signed=True))
    buffer = BytesIO()
    with wave.open(buffer, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        wav_file.writeframes(bytes(frames))
    return buffer.getvalue()


def test_prepare_voicevox_endpoint_defaults() -> None:
    normalized, host, port = _prepare_voicevox_endpoint("127.0.0.1")
    assert normalized == "http://127.0.0.1:50021"
    assert host == "127.0.0.1"
    assert port == 50021


def test_prepare_voicevox_endpoint_preserves_port() -> None:
    normalized, host, port = _prepare_voicevox_endpoint("http://0.0.0.0:12345/")
    assert normalized == "http://0.0.0.0:12345"
    assert host == "0.0.0.0"
    assert port == 12345


def test_prepare_voicevox_endpoint_rejects_empty() -> None:
    with pytest.raises(ValueError):
        _prepare_voicevox_endpoint("  ")


def test_resolve_runtime_executable_from_directory(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "voicevox"
    runtime_dir.mkdir()
    run = runtime_dir / "run"
    run.write_text("", encoding="utf-8")

    resolved = _resolve_runtime_executable(runtime_dir)
    assert resolved == run


def test_resolve_runtime_executable_missing(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "voicevox"
    runtime_dir.mkdir()

    with pytest.raises(FileNotFoundError):
        _resolve_runtime_executable(runtime_dir)


def test_managed_runtime_skips_when_ready(monkeypatch, tmp_path: Path) -> None:
    runtime_dir = tmp_path / "voicevox"
    runtime_dir.mkdir()
    (runtime_dir / "run").write_text("", encoding="utf-8")

    monkeypatch.setattr("nk.tts._voicevox_is_ready", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        "nk.tts.subprocess.Popen",
        lambda *args, **kwargs: pytest.fail("VoiceVox runtime should not launch"),
    )

    with managed_voicevox_runtime(runtime_dir, "http://127.0.0.1:50021") as process:
        assert process is None


def test_managed_runtime_launches_and_cleans_up(monkeypatch, tmp_path: Path) -> None:
    runtime_dir = tmp_path / "voicevox"
    runtime_dir.mkdir()
    run = runtime_dir / "run"
    run.write_text("", encoding="utf-8")

    class DummyProcess:
        def __init__(self) -> None:
            self.terminated = False
            self.killed = False
            self.wait_called = False

        def poll(self) -> int | None:
            return None

        def terminate(self) -> None:
            self.terminated = True

        def wait(self, timeout: float | None = None) -> int:
            self.wait_called = True
            return 0

        def kill(self) -> None:
            self.killed = True

    dummy = DummyProcess()
    popen_args: dict[str, object] = {}
    wait_args: dict[str, object] = {}

    def fake_popen(cmd, cwd, stdout, stderr, env):
        popen_args["cmd"] = cmd
        popen_args["cwd"] = cwd
        popen_args["stdout"] = stdout
        popen_args["stderr"] = stderr
        popen_args["env"] = env
        return dummy

    def fake_wait_for(process, normalized_base_url, readiness_timeout, poll_interval):
        wait_args["process"] = process
        wait_args["normalized_base_url"] = normalized_base_url
        wait_args["readiness_timeout"] = readiness_timeout
        wait_args["poll_interval"] = poll_interval

    monkeypatch.setattr("nk.tts._voicevox_is_ready", lambda *args, **kwargs: False)
    monkeypatch.setattr("nk.tts._wait_for_voicevox_ready", fake_wait_for)
    monkeypatch.setattr("nk.tts.subprocess.Popen", fake_popen)

    with managed_voicevox_runtime(runtime_dir, "http://127.0.0.1:50021") as process:
        assert process is dummy

    assert popen_args["cmd"] == [str(run), "--host", "127.0.0.1", "--port", "50021"]
    assert popen_args["cwd"] == str(runtime_dir)
    assert dummy.terminated is True
    assert dummy.wait_called is True
    assert dummy.killed is False
    assert wait_args["process"] is dummy
    assert wait_args["normalized_base_url"] == "http://127.0.0.1:50021"


def test_managed_runtime_applies_extra_env(monkeypatch, tmp_path: Path) -> None:
    runtime_dir = tmp_path / "voicevox"
    runtime_dir.mkdir()
    run = runtime_dir / "run"
    run.write_text("", encoding="utf-8")

    class DummyProcess:
        def poll(self) -> None:
            return None

        def terminate(self) -> None:
            pass

        def wait(self, timeout: float | None = None) -> int:
            return 0

    dummy = DummyProcess()
    captured_env: dict[str, str] = {}

    def fake_popen(cmd, cwd, stdout, stderr, env):
        captured_env.update(env)
        return dummy

    monkeypatch.setattr("nk.tts._voicevox_is_ready", lambda *args, **kwargs: False)
    monkeypatch.setattr("nk.tts._wait_for_voicevox_ready", lambda *args, **kwargs: None)
    monkeypatch.setattr("nk.tts.subprocess.Popen", fake_popen)

    extra_env = {"VOICEVOX_CPU_NUM_THREADS": "16", "RAYON_NUM_THREADS": "16"}
    with managed_voicevox_runtime(
        runtime_dir, "http://127.0.0.1:50021", extra_env=extra_env
    ):
        pass

    assert captured_env["VOICEVOX_CPU_NUM_THREADS"] == "16"
    assert captured_env["RAYON_NUM_THREADS"] == "16"


def test_managed_runtime_adds_cpu_thread_flag(monkeypatch, tmp_path: Path) -> None:
    runtime_dir = tmp_path / "voicevox"
    runtime_dir.mkdir()
    run = runtime_dir / "run"
    run.write_text("", encoding="utf-8")

    class DummyProcess:
        def poll(self) -> None:
            return None

        def terminate(self) -> None:
            pass

        def wait(self, timeout: float | None = None) -> int:
            return 0

    captured_cmd: list[str] = []

    def fake_popen(cmd, cwd, stdout, stderr, env):
        captured_cmd[:] = cmd
        return DummyProcess()

    monkeypatch.setattr("nk.tts._voicevox_is_ready", lambda *args, **kwargs: False)
    monkeypatch.setattr("nk.tts._wait_for_voicevox_ready", lambda *args, **kwargs: None)
    monkeypatch.setattr("nk.tts.subprocess.Popen", fake_popen)

    with managed_voicevox_runtime(
        runtime_dir,
        "http://127.0.0.1:50021",
        extra_env=None,
        cpu_threads=12,
    ):
        pass

    assert "--cpu_num_threads" in captured_cmd
    idx = captured_cmd.index("--cpu_num_threads")
    assert captured_cmd[idx + 1] == "12"


def test_managed_runtime_reports_launch_failure(monkeypatch, tmp_path: Path) -> None:
    runtime_dir = tmp_path / "voicevox"
    runtime_dir.mkdir()
    (runtime_dir / "run").write_text("", encoding="utf-8")

    monkeypatch.setattr("nk.tts._voicevox_is_ready", lambda *args, **kwargs: False)

    def fake_popen(*args, **kwargs):
        raise OSError("boom")

    monkeypatch.setattr("nk.tts.subprocess.Popen", fake_popen)

    with pytest.raises(VoiceVoxRuntimeError):
        with managed_voicevox_runtime(runtime_dir, "http://127.0.0.1:50021"):
            pass


def test_discover_runtime_ignores_remote() -> None:
    found = discover_voicevox_runtime("https://example.com:50021", env={})
    assert found is None


def test_discover_runtime_env_hint(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "voicevox"
    runtime_dir.mkdir()
    run = runtime_dir / "run"
    run.write_text("", encoding="utf-8")

    env = {"NK_VOICEVOX_RUNTIME": str(runtime_dir)}
    found = discover_voicevox_runtime("http://127.0.0.1:50021", env=env)
    assert found == run


def test_discover_runtime_default_home(monkeypatch, tmp_path: Path) -> None:
    runtime_dir = tmp_path / "opt" / "voicevox" / "macos-x64"
    runtime_dir.mkdir(parents=True)
    run = runtime_dir / "run"
    run.write_text("", encoding="utf-8")

    monkeypatch.setattr(
        "nk.tts.Path.home",
        classmethod(lambda cls: tmp_path),
    )

    found = discover_voicevox_runtime("http://127.0.0.1:50021", env={})
    assert found == run


def test_synthesize_texts_progress(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "input.txt"
    src.write_text("Line one\n\nLine two", encoding="utf-8")
    target = TTSTarget(source=src, output=tmp_path / "out.mp3")

    class DummyClient:
        def __init__(self, *args, **kwargs) -> None:
            self.calls: list[str] = []

        def synthesize_wav(self, text: str, modify_query=None) -> bytes:
            self.calls.append(text)
            return b"wav"

        def close(self) -> None:
            pass

        def recalculate_mora_pitch(self, accent_phrases):
            return accent_phrases

    monkeypatch.setattr("nk.tts.VoiceVoxClient", DummyClient)
    monkeypatch.setattr("nk.tts.wav_bytes_to_mp3", lambda *args, **kwargs: None)
    monkeypatch.setattr("nk.tts._merge_wavs_to_mp3", lambda *args, **kwargs: None)

    events: list[dict[str, object]] = []
    generated = synthesize_texts_to_mp3([target], progress=events.append)

    assert generated == [target.output]
    assert [event["event"] for event in events] == [
        "target_start",
        "chunk_start",
        "chunk_start",
        "target_done",
    ]
    assert events[0]["chunk_count"] == 2


def test_voicevox_client_post_phoneme_override(monkeypatch) -> None:
    class DummyResponse:
        def __init__(self, status_code: int, payload: object) -> None:
            self.status_code = status_code
            self._payload = payload

        def json(self) -> object:
            if isinstance(self._payload, Exception):
                raise self._payload
            return self._payload

        @property
        def text(self) -> str:
            return str(self._payload)

        @property
        def content(self) -> bytes:
            if isinstance(self._payload, bytes):
                return self._payload
            raise TypeError("Payload is not bytes")

    class DummySession:
        def __init__(self) -> None:
            self.calls: list[
                tuple[str, dict[str, object] | None, dict[str, object] | None]
            ] = []

        def post(self, url, *, params=None, json=None, timeout=None):
            if url.endswith("/audio_query"):
                self.calls.append(("audio_query", params, json))
                return DummyResponse(
                    200,
                    {
                        "postPhonemeLength": 0.1,
                        "speedScale": 1.0,
                        "pitchScale": 0.0,
                        "intonationScale": 1.0,
                    },
                )
            if url.endswith("/synthesis"):
                self.calls.append(("synthesis", params, json))
                return DummyResponse(200, b"wav")
            raise AssertionError(f"Unexpected URL: {url}")

        def close(self) -> None:
            pass

    dummy_session = DummySession()
    monkeypatch.setattr("nk.tts.requests.Session", lambda: dummy_session)

    observed_defaults: list[dict[str, float]] = []

    client = VoiceVoxClient(
        post_phoneme_length=0.6,
        speed_scale=1.2,
        pitch_scale=-0.1,
        intonation_scale=0.8,
        engine_defaults_callback=lambda defaults: observed_defaults.append(defaults),
    )
    try:
        wav = client.synthesize_wav("テスト")
        assert wav == b"wav"
    finally:
        client.close()

    assert dummy_session.calls[0][0] == "audio_query"
    assert dummy_session.calls[1][0] == "synthesis"
    synthesis_payload = dummy_session.calls[1][2]
    assert isinstance(synthesis_payload, dict)
    assert synthesis_payload["postPhonemeLength"] == 0.6
    assert observed_defaults
    assert observed_defaults[0]["speed"] == 1.0
    assert observed_defaults[0]["intonation"] == 1.0
    assert synthesis_payload["speedScale"] == 1.2
    assert synthesis_payload["pitchScale"] == -0.1
    assert synthesis_payload["intonationScale"] == 0.8


def test_parallel_jobs_uses_multiple_clients(monkeypatch, tmp_path: Path) -> None:
    files: list[TTSTarget] = []
    for idx in range(3):
        src = tmp_path / f"chapter{idx}.txt"
        src.write_text(f"Line {idx}\n\nLine {idx}b", encoding="utf-8")
        files.append(TTSTarget(source=src, output=tmp_path / f"chapter{idx}.mp3"))

    created_clients: list[int] = []

    class DummyClient:
        def __init__(self, *args, **kwargs) -> None:
            created_clients.append(os.getpid())

        def synthesize_wav(self, text: str, modify_query=None) -> bytes:
            return text.encode("utf-8")

        def close(self) -> None:
            pass

        def recalculate_mora_pitch(self, accent_phrases):
            return accent_phrases

    def fake_wav_to_mp3(wav_bytes: bytes, output_path: Path, **kwargs) -> None:
        output_path.write_bytes(wav_bytes)

    def fake_merge_wavs_to_mp3(
        wav_paths: list[Path], output_path: Path, **kwargs
    ) -> None:
        combined = b"".join(path.read_bytes() for path in wav_paths)
        output_path.write_bytes(combined)

    monkeypatch.setattr("nk.tts.VoiceVoxClient", DummyClient)
    monkeypatch.setattr("nk.tts.wav_bytes_to_mp3", fake_wav_to_mp3)
    monkeypatch.setattr("nk.tts._merge_wavs_to_mp3", fake_merge_wavs_to_mp3)

    outputs = synthesize_texts_to_mp3(files, jobs=2, cache_dir=tmp_path / "cache")

    assert set(outputs) == {target.output for target in files}
    # Each target should have required its own client instance.
    assert len(created_clients) == len(files)


def test_resume_reuses_cached_chunks(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "chapter.txt"
    # Two chunks separated by blank line.
    src.write_text("ChunkA\n\nChunkB", encoding="utf-8")
    target = TTSTarget(source=src, output=tmp_path / "chapter.mp3")
    cache_root = tmp_path / "cache"

    # First run: create cached chunks but fail during merge.
    first_calls: list[str] = []

    class FirstClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def synthesize_wav(self, text: str, modify_query=None) -> bytes:
            first_calls.append(text)
            return text.encode("utf-8")

        def close(self) -> None:
            pass

        def recalculate_mora_pitch(self, accent_phrases):
            return accent_phrases

    monkeypatch.setattr("nk.tts.VoiceVoxClient", FirstClient)
    monkeypatch.setattr("nk.tts.wav_bytes_to_mp3", lambda *a, **k: None)

    def failing_merge(wav_paths: list[Path], output_path: Path, **kwargs) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr("nk.tts._merge_wavs_to_mp3", failing_merge)

    with pytest.raises(RuntimeError):
        synthesize_texts_to_mp3([target], cache_dir=cache_root)

    # Cached files should exist after the failure.
    cached_files = list(cache_root.glob("**/*.wav"))
    assert len(cached_files) == 2
    assert first_calls == ["ChunkA", "ChunkB"]
    assert target.output.exists() is False

    # Second run: ensure cached chunks are reused (no additional synth calls).
    target.output.write_bytes(b"partial")
    second_calls: list[str] = []

    class SecondClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def synthesize_wav(self, text: str, modify_query=None) -> bytes:
            second_calls.append(text)
            return text.encode("utf-8")

        def close(self) -> None:
            pass

        def recalculate_mora_pitch(self, accent_phrases):
            return accent_phrases

    def merging_ok(wav_paths: list[Path], output_path: Path, **kwargs) -> None:
        combined = b"".join(path.read_bytes() for path in wav_paths)
        output_path.write_bytes(combined)

    monkeypatch.setattr("nk.tts.VoiceVoxClient", SecondClient)
    monkeypatch.setattr("nk.tts._merge_wavs_to_mp3", merging_ok)

    outputs = synthesize_texts_to_mp3([target], cache_dir=cache_root)

    assert outputs == [target.output]
    assert second_calls == []  # all chunks served from cache
    assert target.output.read_bytes() == b"ChunkAChunkB"
    # Cache directory is cleaned after successful synthesis by default.
    assert not list(cache_root.glob("**/*.wav"))


def test_keep_cache_directory_persists(monkeypatch, tmp_path: Path) -> None:
    src = tmp_path / "chapter.txt"
    src.write_text("Foo\n\nBar", encoding="utf-8")
    target = TTSTarget(source=src, output=tmp_path / "chapter.mp3")
    cache_root = tmp_path / "cache"

    class DummyClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def synthesize_wav(self, text: str, modify_query=None) -> bytes:
            return text.encode("utf-8")

        def close(self) -> None:
            pass

        def recalculate_mora_pitch(self, accent_phrases):
            return accent_phrases

        def recalculate_mora_pitch(self, accent_phrases):
            return accent_phrases

    monkeypatch.setattr("nk.tts.VoiceVoxClient", DummyClient)
    monkeypatch.setattr(
        "nk.tts.wav_bytes_to_mp3", lambda wav, out, **kwargs: out.write_bytes(wav)
    )

    def merge(wav_paths: list[Path], output_path: Path, **kwargs) -> None:
        combined = b"".join(path.read_bytes() for path in wav_paths)
        output_path.write_bytes(combined)

    monkeypatch.setattr("nk.tts._merge_wavs_to_mp3", merge)

    synthesize_texts_to_mp3([target], cache_dir=cache_root, keep_cache=True)

    cache_dir = _target_cache_dir(cache_root, target)
    assert cache_dir.exists()
    assert (cache_dir / ".complete").exists()
    wavs = list(cache_dir.glob("*.wav"))
    assert wavs

    reuse_calls: list[str] = []

    class ReuseClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def synthesize_wav(self, text: str, modify_query=None) -> bytes:
            reuse_calls.append(text)
            return text.encode("utf-8")

        def close(self) -> None:
            pass

        def recalculate_mora_pitch(self, accent_phrases):
            return accent_phrases

    monkeypatch.setattr("nk.tts.VoiceVoxClient", ReuseClient)
    synthesize_texts_to_mp3([target], cache_dir=cache_root, keep_cache=True)
    assert reuse_calls == []


def _write_token_payload(
    chapter_path: Path, tokens: list[dict[str, object]], text: str
) -> None:
    normalized: list[dict[str, object]] = []
    for token in tokens:
        start = int(token.get("transformed_start", token.get("start", 0)))
        end = int(token.get("transformed_end", token.get("end", start)))
        normalized.append(
            {
                "surface": token.get("surface"),
                "start": token.get("start", start),
                "end": token.get("end", end),
                "reading": token.get("reading"),
                "reading_source": token.get("reading_source"),
                "accent": token.get("accent"),
                "pos": token.get("pos"),
                "transformed_start": start,
                "transformed_end": end,
            }
        )
    payload = {
        "version": TOKEN_METADATA_VERSION,
        "text_sha1": hashlib.sha1(text.encode("utf-8")).hexdigest(),
        "tokens": normalized,
    }
    token_path = chapter_path.with_name(chapter_path.name + ".token.json")
    token_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_token_metadata_overrides_voicevox(monkeypatch, tmp_path: Path) -> None:
    chapter_path = tmp_path / "011.txt"
    chapter_text = "アメヲ\n\nアメヲ"
    chapter_path.write_text(chapter_text, encoding="utf-8")
    tokens = [
        {
            "surface": "雨",
            "reading": "アメ",
            "accent": 1,
            "start": 0,
            "end": 2,
            "pos": "名詞",
        },
        {
            "surface": "飴",
            "reading": "アメ",
            "accent": 0,
            "start": 5,
            "end": 7,
            "pos": "名詞",
        },
    ]
    _write_token_payload(chapter_path, tokens, chapter_text)

    target = TTSTarget(source=chapter_path, output=tmp_path / "out.mp3")

    class CaptureClient:
        def __init__(self) -> None:
            self.payloads: list[dict[str, object]] = []
            self.counter = 0

        def synthesize_wav(self, text: str, modify_query=None) -> bytes:
            moras = [{"text": ch} for ch in text]
            payload = {"accent_phrases": [{"moras": moras, "accent": len(moras)}]}
            if modify_query is not None:
                modify_query(payload)
            accent = payload["accent_phrases"][0]["accent"]
            self.payloads.append(payload)
            self.counter += 1
            return _tone_wav_bytes(frequency=380 + accent * 40)

        def close(self) -> None:
            pass

    client = CaptureClient()

    monkeypatch.setattr(
        "nk.tts.wav_bytes_to_mp3",
        lambda wav_bytes, output, **_: output.write_bytes(wav_bytes),
    )
    monkeypatch.setattr(
        "nk.tts._merge_wavs_to_mp3",
        lambda wav_paths, output_path, **_: output_path.write_bytes(
            b"".join(path.read_bytes() for path in wav_paths)
        ),
    )

    result = _synthesize_target_with_client(
        target,
        client,
        index=1,
        total=1,
        ffmpeg_path="ffmpeg",
        overwrite=True,
        progress=None,
        cache_base=tmp_path / "cache",
        keep_cache=False,
        narrator_profile=VoiceProfile(speaker=2),
        voice_overlays=None,
    )
    assert result == target.output
    assert len(client.payloads) == 2  # two chunks (split by blank line)
    first_accent = client.payloads[0]["accent_phrases"][0]["accent"]
    second_accent = client.payloads[1]["accent_phrases"][0]["accent"]
    assert first_accent == 1  # enforced atamadaka
    assert second_accent == 3  # heiban -> mora count (アメ + ヲ)


def test_token_metadata_handles_stripped_punctuation(
    monkeypatch, tmp_path: Path
) -> None:
    chapter_path = tmp_path / "punct.txt"
    chapter_text = "『アメ』ヲ\n\n．アメ．ヲ"
    chapter_path.write_text(chapter_text, encoding="utf-8")
    tokens = [
        {
            "surface": "雨",
            "reading": "アメ",
            "accent": 1,
            "start": 1,
            "end": 3,
            "pos": "名詞",
        },
        {
            "surface": "飴",
            "reading": "アメ",
            "accent": 0,
            "start": 8,
            "end": 10,
            "pos": "名詞",
        },
    ]
    _write_token_payload(chapter_path, tokens, chapter_text)

    target = TTSTarget(source=chapter_path, output=tmp_path / "punct.mp3")

    class CaptureClient:
        def __init__(self) -> None:
            self.payloads: list[dict[str, object]] = []
            self.texts: list[str] = []

        def synthesize_wav(self, text: str, modify_query=None) -> bytes:
            self.texts.append(text)
            moras = [{"text": ch} for ch in text]
            payload = {"accent_phrases": [{"moras": moras, "accent": len(moras)}]}
            if modify_query is not None:
                modify_query(payload)
            self.payloads.append(payload)
            accent = payload["accent_phrases"][0]["accent"]
            return _tone_wav_bytes(frequency=360 + accent * 30)

        def recalculate_mora_pitch(self, accent_phrases):
            return accent_phrases

        def close(self) -> None:
            pass

    client = CaptureClient()
    monkeypatch.setattr(
        "nk.tts.wav_bytes_to_mp3",
        lambda wav_bytes, output, **_: output.write_bytes(wav_bytes),
    )
    monkeypatch.setattr(
        "nk.tts._merge_wavs_to_mp3",
        lambda wav_paths, output_path, **_: output_path.write_bytes(
            b"".join(path.read_bytes() for path in wav_paths)
        ),
    )

    result = _synthesize_target_with_client(
        target,
        client,
        index=1,
        total=1,
        ffmpeg_path="ffmpeg",
        overwrite=True,
        progress=None,
        cache_base=tmp_path / "cache",
        keep_cache=False,
        narrator_profile=VoiceProfile(speaker=2),
        voice_overlays=None,
    )
    assert result == target.output
    assert len(client.payloads) == 2
    # assert client.texts[0] == "アメヲ"  # 『』 stripped, tokens still align
    assert client.texts[1] == "．アメ．ヲ"
    first_accent = client.payloads[0]["accent_phrases"][0]["accent"]
    second_accent = client.payloads[1]["accent_phrases"][0]["accent"]
    assert first_accent == 1
    assert second_accent == len(client.texts[1])


def test_token_metadata_skipped_when_hash_mismatch(monkeypatch, tmp_path: Path) -> None:
    chapter_path = tmp_path / "chapter.txt"
    text = "アメ\n\nアメ"
    chapter_path.write_text(text, encoding="utf-8")
    payload = {
        "version": TOKEN_METADATA_VERSION,
        "text_sha1": "deadbeef",
        "tokens": [
            {
                "surface": "雨",
                "reading": "アメ",
                "accent": 1,
                "start": 0,
                "end": 2,
                "transformed_start": 0,
                "transformed_end": 2,
            }
        ],
    }
    chapter_path.with_name(chapter_path.name + ".token.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )

    target = TTSTarget(source=chapter_path, output=tmp_path / "out.mp3")

    class CaptureClient:
        def __init__(self) -> None:
            self.payloads: list[dict[str, object]] = []

        def synthesize_wav(self, text: str, modify_query=None) -> bytes:
            moras = [{"text": ch} for ch in text]
            payload = {"accent_phrases": [{"moras": moras, "accent": len(moras)}]}
            if modify_query is not None:
                modify_query(payload)
            self.payloads.append(payload)
            return _tone_wav_bytes(frequency=420 + len(moras) * 20)

        def close(self) -> None:
            pass

    client = CaptureClient()
    monkeypatch.setattr(
        "nk.tts.wav_bytes_to_mp3",
        lambda wav_bytes, output, **_: output.write_bytes(wav_bytes),
    )
    monkeypatch.setattr(
        "nk.tts._merge_wavs_to_mp3",
        lambda wav_paths, output_path, **_: output_path.write_bytes(
            b"".join(path.read_bytes() for path in wav_paths)
        ),
    )

    _synthesize_target_with_client(
        target,
        client,
        index=1,
        total=1,
        ffmpeg_path="ffmpeg",
        overwrite=True,
        progress=None,
        cache_base=tmp_path / "cache",
        keep_cache=False,
        narrator_profile=VoiceProfile(speaker=2),
        voice_overlays=None,
    )
    assert client.payloads
    assert all(
        phrase["accent"] == len(phrase["moras"])
        for payload in client.payloads
        for phrase in payload["accent_phrases"]
    )


@pytest.mark.skipif(
    not os.environ.get("NK_VOICEVOX_URL"),
    reason="Set NK_VOICEVOX_URL (and optional NK_VOICEVOX_SPEAKER) to exercise the real VoiceVox engine.",
)
def test_voicevox_pitch_artifacts(monkeypatch, tmp_path: Path) -> None:
    base_url = os.environ.get("NK_VOICEVOX_URL")
    speaker = int(os.environ.get("NK_VOICEVOX_SPEAKER", "2"))
    chapter_path = tmp_path / "voice.txt"
    chapter_text = "アメヲ\n\nアメヲ"
    chapter_path.write_text(chapter_text, encoding="utf-8")
    tokens = [
        {
            "surface": "雨",
            "reading": "アメ",
            "accent": 1,
            "start": 0,
            "end": 2,
            "pos": "名詞",
        },
        {
            "surface": "飴",
            "reading": "アメ",
            "accent": 0,
            "start": 5,
            "end": 7,
            "pos": "名詞",
        },
    ]
    _write_token_payload(chapter_path, tokens, chapter_text)

    target = TTSTarget(source=chapter_path, output=tmp_path / "out.mp3")
    cache_root = tmp_path / "cache"

    monkeypatch.setattr(
        "nk.tts.wav_bytes_to_mp3",
        lambda wav_bytes, output, **_: output.write_bytes(wav_bytes),
    )
    monkeypatch.setattr(
        "nk.tts._merge_wavs_to_mp3",
        lambda wav_paths, output_path, **_: output_path.write_bytes(
            b"".join(path.read_bytes() for path in wav_paths)
        ),
    )

    client = VoiceVoxClient(base_url=base_url, speaker_id=speaker)
    try:
        result = _synthesize_target_with_client(
            target,
            client,
            index=1,
            total=1,
            ffmpeg_path="ffmpeg",
            overwrite=True,
            progress=None,
            cache_base=cache_root,
            keep_cache=True,
            narrator_profile=VoiceProfile(speaker=speaker),
            voice_overlays=None,
        )
        assert result == target.output
        cache_dir = _target_cache_dir(cache_root, target)
        chunk_paths = sorted(cache_dir.glob("*.wav"))
        assert len(chunk_paths) == 2
        for idx, chunk_path in enumerate(chunk_paths, start=1):
            _save_artifact(f"voicevox_base_chunk{idx}.wav", chunk_path.read_bytes())

        # Capture a reference artifact where the same text stays in a single chunk.
        single_chunk_text = chapter_text.replace("\n\n", "\n")
        single_chunk_path = tmp_path / "voice_single_chunk.txt"
        single_chunk_path.write_text(single_chunk_text, encoding="utf-8")
        single_tokens = [
            {
                "surface": "雨",
                "reading": "アメ",
                "accent": 1,
                "start": 0,
                "end": 2,
                "pos": "名詞",
            },
            {
                "surface": "飴",
                "reading": "アメ",
                "accent": 0,
                "start": 4,
                "end": 6,
                "pos": "名詞",
            },
        ]
        _write_token_payload(single_chunk_path, single_tokens, single_chunk_text)

        single_target = TTSTarget(
            source=single_chunk_path, output=tmp_path / "single_chunk.mp3"
        )
        single_cache_root = tmp_path / "cache_single"
        single_result = _synthesize_target_with_client(
            single_target,
            client,
            index=1,
            total=1,
            ffmpeg_path="ffmpeg",
            overwrite=True,
            progress=None,
            cache_base=single_cache_root,
            keep_cache=True,
            narrator_profile=VoiceProfile(speaker=client.speaker_id),
            voice_overlays=None,
        )
        assert single_result == single_target.output
        single_cache_dir = _target_cache_dir(single_cache_root, single_target)
        single_chunk_paths = sorted(single_cache_dir.glob("*.wav"))
        assert len(single_chunk_paths) == 1
        _save_artifact(
            "voicevox_base_single_chunk.wav", single_chunk_paths[0].read_bytes()
        )

        # Capture a punctuation-heavy example to ensure offset remapping survives stripping.
        punct_path = tmp_path / "voice_punct.txt"
        punct_text = "『アメ』ヲ\n\n．アメ．ヲ"
        punct_path.write_text(punct_text, encoding="utf-8")
        punct_tokens = [
            {
                "surface": "雨",
                "reading": "アメ",
                "accent": 1,
                "start": 1,
                "end": 3,
                "pos": "名詞",
            },
            {
                "surface": "飴",
                "reading": "アメ",
                "accent": 0,
                "start": 8,
                "end": 10,
                "pos": "名詞",
            },
        ]
        _write_token_payload(punct_path, punct_tokens, punct_text)
        punct_target = TTSTarget(source=punct_path, output=tmp_path / "punct.mp3")
        punct_cache_root = tmp_path / "cache_punct"
        punct_result = _synthesize_target_with_client(
            punct_target,
            client,
            index=1,
            total=1,
            ffmpeg_path="ffmpeg",
            overwrite=True,
            progress=None,
            cache_base=punct_cache_root,
            keep_cache=True,
            narrator_profile=VoiceProfile(speaker=client.speaker_id),
            voice_overlays=None,
        )
        assert punct_result == punct_target.output
        punct_cache_dir = _target_cache_dir(punct_cache_root, punct_target)
        punct_chunk_paths = sorted(punct_cache_dir.glob("*.wav"))
        assert len(punct_chunk_paths) == 2
        for idx, chunk_path in enumerate(punct_chunk_paths, start=1):
            _save_artifact(f"voicevox_punct_chunk{idx}.wav", chunk_path.read_bytes())

        punct_single_text = punct_text.replace("\n\n", "\n")
        punct_single_path = tmp_path / "voice_punct_single.txt"
        punct_single_path.write_text(punct_single_text, encoding="utf-8")
        punct_single_tokens = [
            {
                "surface": "雨",
                "reading": "アメ",
                "accent": 1,
                "start": 1,
                "end": 3,
                "pos": "名詞",
            },
            {
                "surface": "飴",
                "reading": "アメ",
                "accent": 0,
                "start": 7,
                "end": 9,
                "pos": "名詞",
            },
        ]
        _write_token_payload(punct_single_path, punct_single_tokens, punct_single_text)
        punct_single_target = TTSTarget(
            source=punct_single_path, output=tmp_path / "punct_single.mp3"
        )
        punct_single_cache_root = tmp_path / "cache_punct_single"
        punct_single_result = _synthesize_target_with_client(
            punct_single_target,
            client,
            index=1,
            total=1,
            ffmpeg_path="ffmpeg",
            overwrite=True,
            progress=None,
            cache_base=punct_single_cache_root,
            keep_cache=True,
            narrator_profile=VoiceProfile(speaker=client.speaker_id),
            voice_overlays=None,
        )
        assert punct_single_result == punct_single_target.output
        punct_single_cache_dir = _target_cache_dir(
            punct_single_cache_root, punct_single_target
        )
        punct_single_chunk_paths = sorted(punct_single_cache_dir.glob("*.wav"))
        assert len(punct_single_chunk_paths) == 1
        _save_artifact(
            "voicevox_punct_single_chunk.wav",
            punct_single_chunk_paths[0].read_bytes(),
        )
    finally:
        client.close()
