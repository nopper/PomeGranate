#ifndef PARSER_H
#define PARSER_H

#include <glib.h>
#include <stdio.h>
#include <archive.h>
#include "libstemmer/libstemmer.h"

#define MAXLEN (1024 * 1024 * 4)
#define MAXWORD 64

#define ID_LENGTH 6
#define ID_OFFSET 16
#define FILE_FORMAT "output-r%06u-p%06u"

#define IS_LATIN(c)              (((c) <= 0x02AF) ||	\
                                  ((c) >= 0x1E00 && (c) <= 0x1EFF))
#define IS_ASCII(c)              ((c) <= 0x007F)
#define IS_ASCII_ALPHA_LOWER(c)  ((c) >= 0x0061 && (c) <= 0x007A)
#define IS_ASCII_ALPHA_HIGHER(c) ((c) >= 0x0041 && (c) <= 0x005A)
#define IS_ASCII_NUMERIC(c)      ((c) >= 0x0030 && (c) <= 0x0039)
#define IS_ASCII_IGNORE(c)       ((c) <= 0x002C)
#define IS_HYPHEN(c)             ((c) == 0x002D)
#define IS_UNDERSCORE(c)         ((c) == 0x005F)

typedef enum {
    WORD_ASCII_HIGHER,
    WORD_ASCII_LOWER,
    WORD_HYPHEN,
    WORD_UNDERSCORE,
    WORD_NUM,
    WORD_ALPHA_HIGHER,
    WORD_ALPHA_LOWER,
    WORD_ALPHA,
    WORD_ALPHA_NUM,
    WORD_IGNORE
} WordType;

struct _Parser {
    struct sb_stemmer *stemmer;
    GHashTable *dict;
    struct archive *input;

    GList *docids;
    GHashTable *docid_set;
    GTree *word_tree;

    glong length; // Keep track of the length of the file
    guint num_reducers;
};

typedef struct _Parser Parser;

Parser* parser_new(guint num_reducers, const char *input);
void parser_free(Parser *parser);
void parser_run(Parser *parser, glong limit);

#endif
