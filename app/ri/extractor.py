import os
import zipfile

from parsing import preprocess_string

class DocumentExtractor(object):
    def __init__(self, path):
        self.path = path

    def get_words(self):
        with zipfile.ZipFile(self.path, 'r') as archive:
            index = 0
            total = len(archive.infolist())

            for member in archive.infolist():
                index += 1
                doc_id = int(member.filename.split("-", 2)[1])
                text = archive.read(member)

                perc = float(index) / float(total)

                for line in text.splitlines():
                    for word in preprocess_string(line):
                        yield (perc, word, doc_id)

if __name__ == "__main__":
    import sys
    for word in DocumentExtractor(sys.argv[1]).get_words():
        pass