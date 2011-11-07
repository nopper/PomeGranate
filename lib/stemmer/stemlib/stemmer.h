#ifndef __STEMMER_H_
#define __STEMMER_H_

typedef struct _stemmer Stemmer;

Stemmer* create_stemmer();
void free_stemmer(Stemmer* z);

int stem(Stemmer* z, char * b, int k);

#endif
