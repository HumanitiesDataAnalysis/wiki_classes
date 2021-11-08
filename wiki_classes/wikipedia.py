import duckdb
from pyarrow import parquet as pq
import pyarrow as pa
from pathlib import Path

from .article import Article

class ArticleCollection():
  def __init__(self, dir):
    # dir: where the files for this are stored
    self.dir = dir
    

  def fill_from_articles(self, articles):
    self.titles = []
    self.texts = []
    for article in articles:
      self.titles.append(article.title)
      self.texts.append(article.text)

  def save_to_disk(self):
    dir = self.dir
    # Create the directory.
    Path(dir).mkdir(exist_ok = True)
    # Build a table of the articles:
    table = pa.table({
      'titles': pa.array(self.titles),
      'text': pa.array(self.texts)
    })
    # write to disk.
    pq.write_parquet(table, self.dir / "contents.parquet")
      
  def create_database_connection(self):
      self.con = duckdb.connect(":memory:")
      self.con.execute(f"CREATE VIEW wiki AS SELECT * FROM parquet_scan('{self.dir}/*.parquet')")        
      
  def get_article(self, title):
      title, text = self.con.query(f"SELECT titles, text FROM wiki WHERE titles='{title}'").fetchone()
      return Article(title, text)
  
  def iter_over_query(self, query):
      result_set = self.con.cursor().execute(query)
      while True:
          try:
              title, text = result_set.fetchone()
              yield Article(title, text)
          except:
              break
          
  def __iter__(self):
      for article in self.iter_over_query("SELECT titles, text FROM wiki"):
          yield article
  
  def text_article(self, query, limit = 10_000):
      """
      Returns an iterator over articles containing the string 'query'.

      query: a string to search for across wikipedia.
      limit: maximum number of articles to return.
      """
      for article in self.iter_over_query(f"SELECT titles, text FROM wiki WHERE text LIKE '%{query}%' LIMIT {limit}"):
          yield article
  
  def make_collection(self, words = ["environment", "global warming", "climate change", "conservation"]):
    collection = []
    seen_articles = {}
    for word in words:
      for article in self.text_article(word):
        if not article.title in seen_articles:
          collection.append(article)
          seen_articles[article.title] = True
    return collection

class Wikipedia(ArticleCollection):
    def __init__(self, dir = "."):
        self.dir = dir
        self.create_database_connection()
