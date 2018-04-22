# reddit-json-dump-parser

A parser for the reddit data dump that can be found here: [reddit](https://www.reddit.com/r/datasets/comments/3bxlg7/i_have_every_publicly_available_reddit_comment/)

## How does the loader work?

1. You edit the config to include the path where the uncompressed dump files can be found.
2. You run `python3 run.py`
3. You wait for it to complete (Took a few hours)
4. The sqlite3 database file is now ready to be queried!

## How does the sanitizer work?

1. Create a task for each batch of 10000 comments for preprocessing.
2. Preprocess the string using the following technique (tunable via config.json)
    1. Replace names by the tag \<name>
    2. Replace numbers by the tag \<number>
    3. Remove ponctuation
    4. Replace words not part of the provided dictionary by the tag \<unk>
3. Save the resulting text as sanitized_body in the sqlite3 db
