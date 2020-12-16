
from mrjob.job import MRJob

class TextStats(MRJob):
    def mapper(self, _, line):
        import os
        file_name = os.getenv('mapreduce_map_input_file')
        wordCount = len(line.lower().split())
        charCount = len(line)
        lineCount = 1
        yield "w", wordCount
        yield "c", charCount
        yield "l", lineCount
        yield "fn", file_name
            
    def combiner(self, key, counts_iter):
        if key != "fn":
            yield key, sum(counts_iter)
        else:
            yield key, list(set(counts_iter))
    
    def reducer(self, key, counts_iter):
        if key == "w":
            yield "words", sum(counts_iter)
        elif key == "c":
            yield "chars", sum(counts_iter)
        elif key == "l":
            yield "lines", sum(counts_iter)
        else:
            yield "files", list(counts_iter)

if __name__ == '__main__':
    TextStats.run()
