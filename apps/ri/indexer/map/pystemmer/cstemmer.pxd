cdef extern from "Python.h":
  object PyString_FromStringAndSize(char *s, Py_ssize_t len)

cdef extern from "../libstemmer/libstemmer.h":
  cdef struct sb_stemmer:
    pass

  sb_stemmer* sb_stemmer_new(char *algorithm, char *enc)
  void sb_stemmer_delete(sb_stemmer *stemmer)
  int sb_stemmer_length(sb_stemmer *stemmer)
  char * sb_stemmer_stem(sb_stemmer *stemmer, char *word, int size)
