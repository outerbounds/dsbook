import sqlite3

def recs_key(movie_set):
    return '%s,%s' % (min(movie_set), max(movie_set))

def dbname(run_id):
    return 'movie_recs_%s.db' % run_id

def save(run_id, recs, names):
    NAMES_TABLE = "CREATE TABLE movies_%s("\
                  "   movie_id INTEGER PRIMARY KEY,"\
                  "   is_top INTEGER, name TEXT)" % run_id
    NAMES_INSERT = "INSERT INTO movies_%s "\
                   "VALUES (?, ?, ?)" % run_id
    RECS_TABLE = "CREATE TABLE recs_%s(recs_key TEXT, "\
                 "    movie_id INTEGER)" % run_id
    RECS_INSERT = "INSERT INTO recs_%s VALUES (?, ?)" % run_id
    RECS_INDEX = "CREATE INDEX index_recs ON recs_%s(recs_key)" % run_id

    def db_recs(recs):
        for movie_set, user_recs in recs:
            key = recs_key(movie_set)
            for rec in user_recs:
                yield key, int(rec)

    name = dbname(run_id)
    with sqlite3.connect(name) as con:
        cur = con.cursor()
        cur.execute(NAMES_TABLE)
        cur.execute(RECS_TABLE)
        cur.executemany(NAMES_INSERT, names)
        cur.executemany(RECS_INSERT, db_recs(recs))
        cur.execute(RECS_INDEX)
    return name

class MovieRecsDB():
    def __init__(self, run_id):
        self.run_id = run_id
        self.name = dbname(run_id)
        self.con = sqlite3.connect(self.name)

    def get_recs(self, movie_id1, movie_id2):
        SQL = "SELECT name FROM movies_{run_id} AS movies "\
              "JOIN recs_{run_id} AS recs "\
              "ON recs.movie_id = movies.movie_id "\
              "WHERE recs.recs_key = ?".format(run_id=self.run_id)
        cur = self.con.cursor()
        cur.execute(SQL, [recs_key((movie_id1, movie_id2))])
        return [k[0] for k in cur]

    def get_top_movies(self):
        SQL = "SELECT movie_id, name FROM movies_%s "\
              "WHERE is_top=1" % self.run_id
        cur = self.con.cursor()
        cur.execute(SQL)
        return list(cur)

if __name__ == '__main__':
    import sys
    db = MovieRecsDB(sys.argv[1])
    print(db.get_top_movies())
    print(db.get_recs(int(sys.argv[2]), int(sys.argv[3])))