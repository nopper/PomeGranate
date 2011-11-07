cdef extern from "stemlib/stemmer.h":
  ctypedef struct Stemmer:
    pass

  Stemmer* create_stemmer()
  void free_stemmer(Stemmer* s)

  int stem(Stemmer* s, char* b, int k)
