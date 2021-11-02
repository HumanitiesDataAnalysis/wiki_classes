import re

class Article():

    def __init__(self, title, text = None):
        self.text = text
        self.title = title

    def fill_text(self, wikipedia):
        if self.text is None:
            self.text = wikipedia.get_text(self.title)
    
    def links(self):
        #returns number of links to other wiki articles in article
        links_list = re.findall(r'\[{2}[^\]]*\]{2}', self.text)
        return links_list

    def cites(self):
      #returns number of citations in article
      cites_list = re.findall(r'\{{2}cite[^\}]*\}{2}', self.text)
      return cites_list

    def links_no_regex(self):
      matches = []
      for match in self.text.split("[[")[1:]:
        for splitter in ["]]", "|", "#"]:
          match = match.split(splitter)[0]
        matches.append(match)
      return matches
    
    def all_infoboxes(self):
      # Filter links to infoboxes like Template:Infobox sport
      pass
      raise NotImplementedError("Not implemented!")
