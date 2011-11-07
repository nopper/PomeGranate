cimport cpython
cimport libc.stdlib

cimport cstemmer

cdef class PorterStemmer:
    cdef cstemmer.Stemmer *_c_stemmer

    def __cinit__(self):
        self._c_stemmer = cstemmer.create_stemmer()
        if self._c_stemmer is NULL:
          raise MemoryError()

    def __dealloc__(self):
        if self._c_stemmer is not NULL:
          cstemmer.free_stemmer(self._c_stemmer)

    def stem(self, s):
      return s[:self.cstem(s, len(s) - 1)]

    cdef unicode convert(self, char* s, size_t length):
      return s[:length].decode('UTF-8', 'strict')

    cdef int cstem(self, char* s, size_t length):
        return cstemmer.stem(self._c_stemmer, s, length)
