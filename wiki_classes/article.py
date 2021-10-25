class Article():
    def __init__(self, title, text = None):
        self.text = text
        self.title = title
    def fill_text(self, wikipedia):
        if self.text is None:
            self.text = wikipedia.get_text(self.title)