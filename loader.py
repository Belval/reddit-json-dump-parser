import os
import time
import json
import sqlite3

from multiprocessing import Pool

def create_parent_id_index(conf):
    """
        Creates the parent_id index
    """

    conn = sqlite3.connect(conf["sqlite_db_path"])
    c = conn.cursor()
    c.execute("CREATE INDEX comments_parent_id ON comments (parent_id)")
    conn.commit()
    conn.close()

def create_name_index(conf):
    """
        Creates the name index
    """

    conn = sqlite3.connect(conf["sqlite_db_path"])
    c = conn.cursor()
    c.execute("CREATE INDEX comments_name ON comments (name)")
    conn.commit()
    conn.close()

def create_is_locked_index(conf):
    """
        Creates the is_locked index
    """

    conn = sqlite3.connect(conf["sqlite_db_path"])
    c = conn.cursor()
    c.execute("CREATE INDEX comments_is_locked ON comments (is_locked)")
    conn.commit()
    conn.close()

def create_database(conf):
    """
        Create the database if required
    """

    conn = sqlite3.connect(conf["sqlite_db_path"])
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS comments (score_hidden INT, name TEXT, link_id TEXT, body TEXT, sanitized_body TEXT, downs INT, created_utc INT, score INT, author TEXT, distinguished TEXT, id TEXT, archived INT, parent_id TEXT, subreddit TEXT, author_flair_css_class TEXT, author_flair_text TEXT, gilded INT, retrieved_on INT, ups INT, controversiality INT, subreddit_id TEXT, edited INT, is_locked INT)")
    conn.commit()
    conn.close()

def save_batch(conn, batch):
    for _ in range(50):
        try:
            conn.cursor().executemany(
                "INSERT INTO comments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch
            )
            conn.commit()
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e):
                print('Database was locked, retrying in 1 second.')
                time.sleep(1)
            else:
                raise

def fill_database(conf, file_path):
    """
        Parse the given file
    """

    conn = sqlite3.connect(conf["sqlite_db_path"])
    save_step = 100000
    i = 0

    with open(file_path) as f:
        batch = []
        for l in f:
            if i % save_step == 0 and i != 0:
                save_batch(conn, batch)
                batch = []        
            
            comment_dict = json.loads(l)
            # We will use the whitelist if not empty, else the blacklist
            if True or comment_dict.get('subreddit', '') not in conf.get('blacklist', '') and (len(conf.get('whitelist', '') == 0) or comment_dict.get('subreddit', '') in conf.get('whitelist', '')):
                batch.append((
                    int(comment_dict.get('score_hidden', '0')),
                    comment_dict.get('name', ''),
                    comment_dict.get('link_id', ''),
                    comment_dict.get('body', ''),
                    '', # We'll parse it afterwards
                    int(comment_dict.get('downs', '0')),
                    int(comment_dict.get('created_utc', '0')),
                    int(comment_dict.get('score', '0')),
                    comment_dict.get('author', ''),
                    comment_dict.get('distinguished', ''),
                    comment_dict.get('id', ''),
                    int(comment_dict.get('archived', '0')),
                    comment_dict.get('parent_id', ''),
                    comment_dict.get('subreddit', ''),
                    comment_dict.get('author_flair_css_class', ''),
                    comment_dict.get('author_flair_text', ''),
                    int(comment_dict.get('gilded', '0')),
                    int(comment_dict.get('retrieved_on', '0')),
                    int(comment_dict.get('ups', '0')),
                    int(comment_dict.get('controversiality', '0')),
                    comment_dict.get('subreddit_id', ''),
                    int(comment_dict.get('edited', '0')),
                    0
                ))
                i += 1
        save_batch(conn, batch)
    conn.close()

def load_from_folder(conf):
    """
        Instead of loading from a single file, we will load everything from a folder.
        This is useful is we are processing more than one dump file.
    """

    pool = Pool(conf['thread_count'])
    pool.starmap(
        fill_database,
        [
            (conf, os.path.join(conf['input_folder_path'], f))
            for f in os.listdir(conf['input_folder_path'])
        ]
    )
    pool.close()
    pool.join()

    # Finally we all the required indexes otherwise search will be O(n) and that's a long time with 1.7 billions records.
    create_parent_id_index(conf)
    create_name_index(conf)
    create_is_locked_index(conf)
