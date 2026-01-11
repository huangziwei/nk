# Character Attribution Guide

Purpose: keep speaker labels in .tts.json consistent and avoid false positives.

## Decision rules
- Default all chunks to narrator unless there is clear evidence of a specific
  character voice for the chunk.
- Relabel a chunk when it contains explicit direct speech (quote punctuation
  or a clear speaker tag like "Name: ...").
- Relabel narration too when the POV is clearly tied to a character (first-person
  narration within an established POV block, diary/log format, or explicit
  section headers naming the speaker). In such cases, use the same character
  as the nearby dialogue.
- Use full Japanese character names for speaker labels (avoid romaji/English
  aliases).
- Pull the canonical kanji spellings from the matching `.original.txt` file
  for that chapter.
- Do not infer a speaker from reported speech or paraphrase (e.g., "X said
  that ...", "X asked ...") unless quoted speech appears.
- If the chunk mixes narration and direct speech, either split the chunk
  before relabeling or keep it as narrator and flag for review.
- When assigning a non-narrator label, use an existing key in tts_voices
  or add the new label to .nk-book.json under tts_voices first (include at
  least a speaker id so the entry is recognized).
- If the speaker is ambiguous, keep narrator and ask for confirmation.

## Quick checklist
- Is there explicit quoted text, or a clear POV tag for this block?
- Is the speaker identity unambiguous from context?
- Is the label the full Japanese name for that character?
- Does the label match the kanji form in the `.original.txt` file or `.txt.token.json` file?
- Does the label exist in tts_voices? If not, add it to .nk-book.json or
  pick an existing one.

## Notes
- When in doubt, preserve narrator and request clarification.
