# nk data

This directory holds the pretrained assets that ship with nk, including
`nhk_easy_readings.json`. The file is generated from the NHK Easier readings
corpus published at [https://nhkeasier.com](https://nhkeasier.com).

To rebuild the dictionary:

1. Download the latest `nhkeasier.epub` bundle from the site above and place it
   inside the repo (the default path is `dev/nhkeasier.epub`).
2. Run `python src/nk/data/build_corpus_readings.py` to aggregate the readings
   into `src/nk/data/nhk_easy_readings.json`.

The `build_corpus_readings.py` script accepts `--epub` and `--output` arguments
if you keep the corpus in a different location or want to write the JSON
elsewhere.
