cimport cpython
cimport libc.stdlib

cimport cstemmer

cdef class Stemmer:
    cdef cstemmer.sb_stemmer *_c_stemmer

    def __cinit__(self, lang="english", encoding="UTF_8"):
        self._c_stemmer = cstemmer.sb_stemmer_new(lang, encoding)
        if self._c_stemmer is NULL:
          raise MemoryError()

    def __dealloc__(self):
        if self._c_stemmer is not NULL:
          cstemmer.sb_stemmer_delete(self._c_stemmer)

    def stem(self, s):
      out = cstemmer.sb_stemmer_stem(self._c_stemmer, s, len(s))
      return out[:cstemmer.sb_stemmer_length(self._c_stemmer)]
