import time
import sys
import re
import json
import string
import sqlite3

from multiprocessing import Process, Pool

import nltk
nltk.download('punkt')
nltk.download('words')
nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')

def process_string(s, conf):
    """
        Remove special char transform words, etc...
    """

    final_string = s

    # We remove the ponctuation
    if conf['punctuation_removal']:
        final_string = ''.join([c for c in final_string if c not in conf['punctuation_string']])
    
    # We replace the name entity in the string
    if conf['name_entity_removal']:
        final_string = ' '.join([
            w[0] if type(w[0]) == str else conf['name_entity_placeholder']
            for w in nltk.ne_chunk(
                        nltk.pos_tag(
                                nltk.word_tokenize(s)
                        ), binary=True)
        ])

    # We replace numbers in the string
    if conf['number_removal']:
        final_string = re.sub(r'\d+', conf['number_placeholder'], final_string)

    # We lowercase the string
    if conf['lower_case_string']:
        final_string = final_string.lower()
    
    # We remove unknown token if a wordlist was defined
    if len(conf['wordlist']) > 3:
        final_string = ' '.join(
            [
                w if w in conf['wordlist'] else conf['unknown_word_placeholder']
                for w in final_string
            ]
        )
    
    if conf['add_end_of_utterance_token']:
        final_string = final_string + ' ' + conf['end_of_utterance_token']

    return final_string

def process_rows(rows, conf):
    """
        Calls the process_string() function and saves the result to the disk
    """
    
    # Processing the records
    processed_records = []
    for i, n, b in rows:
        processed_records.append((process_string(b, conf['sanitize_comments_parameters']), n))

    # Saving resulting strings
    for i in range(50):
        try:
            conn = sqlite3.connect(conf['sqlite_db_path'])
            c = conn.cursor()
            c.executemany('UPDATE comments SET sanitized_body = ? WHERE name = ?', processed_records)
            conn.commit()
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e):
                print('Database was locked, retrying in 1 second {}'.format(i))
                time.sleep(1)
            else:
                raise

def can_start_new_task(results, max_count):
    """
        Takes a list of AsyncResults and return True or False based on how many are still running
    """

    awaiting_tasks = 0

    for r in results:
        try:
            r.successful()
        except AssertionError:
            awaiting_tasks += 1

    return awaiting_tasks < max_count

def sanitize_db_comments(conf):
    """
        Creates a sanitized version of the comments in the database
    """

    # We add the special tokens so they don't get removed
    conf['sanitize_comments_parameters']['wordlist'].append(conf['sanitize_comments_parameters']['name_entity_placeholder'])
    conf['sanitize_comments_parameters']['wordlist'].append(conf['sanitize_comments_parameters']['number_placeholder'])
    conf['sanitize_comments_parameters']['wordlist'].append(conf['sanitize_comments_parameters']['unknown_word_placeholder'])
    # Converting the wordlist to a set for faster lookup
    conf['wordlist'] = set(conf['wordlist'])
    # Converting the punctuation string to a set for faster lookup
    conf['punctuation_string'] = set(conf['punctuation_string'])

    conn = sqlite3.connect(conf["sqlite_db_path"])
    c = conn.cursor()
    data_to_process_left = True
    loop_count = 0
    results = []
    with Pool(conf['thread_count']) as pool:
        while data_to_process_left:
            if not can_start_new_task(results, 20):
                print('Too many tasks in queue. Sleeping!')
                time.sleep(5)
                continue
            print(loop_count)
            rows = c.execute('SELECT link_id, body, name, parent_id FROM comments WHERE is_locked = 0 LIMIT 10000')
            values = [(i, r[2], r[1]) for i, r in enumerate(rows)]
            for i in range(50):
                try:
                    update_conn = sqlite3.connect(conf["sqlite_db_path"])
                    update_cursor = update_conn.cursor()
                    update_cursor.executemany('UPDATE comments SET is_locked = 1 WHERE name = ?', [(n,) for _, n, _ in values])
                    update_conn.commit()
                    break
                except sqlite3.OperationalError as e:
                    if "locked" in str(e):
                        print('Database was locked, retrying in 1 second {}'.format(i))
                        time.sleep(1)
                    else:
                        raise

            data_to_process_left = len(values) == 10000
            results.append(pool.apply_async(process_rows, (values, conf)))
            loop_count += 1
        pool.close()
        pool.join()

if __name__=='__main__':
    conf = json.loads(open("./config.json").read())
    # We add the special tokens so they don't get removed
    conf['sanitize_comments_parameters']['wordlist'].append(conf['sanitize_comments_parameters']['name_entity_placeholder'])
    conf['sanitize_comments_parameters']['wordlist'].append(conf['sanitize_comments_parameters']['number_placeholder'])
    conf['sanitize_comments_parameters']['wordlist'].append(conf['sanitize_comments_parameters']['unknown_word_placeholder'])
    # Converting the wordlist to a set for faster lookup
    conf['sanitize_comments_parameters']['wordlist'] = set(conf['sanitize_comments_parameters']['wordlist'])
    # Converting the punctuation string to a set for faster lookup
    conf['sanitize_comments_parameters']['punctuation_string'] = set(conf['sanitize_comments_parameters']['punctuation_string'])

    print(process_string(sys.argv[1], conf['sanitize_comments_parameters']))