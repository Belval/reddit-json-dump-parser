import json

from loader import create_database, load_from_folder
from sanitizer import sanitize_db_comments

def main():
    print("Loading config")
    conf = json.loads(open("./config.json").read())
    # This only creates a database if it didn't exist
    print("Creating database")
    create_database(conf)
    # Fills the database using the data file
    if conf["fill_database"]:
        print("Filling database")
        load_from_folder(conf)
    # Creates the CSV file from the database
    if conf["sanitize_comments"]:
        print("Sanitizing comments")
        sanitize_db_comments(conf)

if __name__=='__main__':
    main()