import duckdb
from .article import Article

class Wikipedia():
    def __init__(self):
        self.create_database_connection()
        
    def create_database_connection(self):
        self.con = duckdb.connect(":memory:")
        self.con.execute("CREATE VIEW wiki AS SELECT * FROM parquet_scan('*.parquet')")        
        
    def get_article(self, title):
        title, text = self.con.query(f"SELECT titles, text FROM wiki WHERE titles='{title}'").fetchone()
        return Article(title, text)
    
    def iter_over_query(self, query):
        result_set = self.con.cursor().execute(query)
        while True:
            title, text = result_set.fetchone()
            yield Article(title, text)
            
    def __iter__(self):
        for article in self.iter_over_query("SELECT titles, text FROM wiki"):
            yield article

    def text_article(self, query):
        for article in self.iter_over_query(f"SELECT titles, text FROM wiki WHERE text LIKE '%query%'"):
            yield article