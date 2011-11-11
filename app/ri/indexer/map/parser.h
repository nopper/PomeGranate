#ifndef PARSER_H
#define PARSER_H

#include <glib.h>
#include <stdio.h>
#include <archive.h>
#include "libstemmer/libstemmer.h"

#define BUFFSIZE 8192
#define MAXLEN (1024 * 1024 * 4)
#define MAXWORD 64

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
    struct archive *input;      /* The archive input file */
    struct sb_stemmer *stemmer; /* Stemmer */

    GHashTable *dict;           /* The main hash table word -> table */
    GHashTable *docid_set;      /* A hashtable used as a set to reduce space */

    GList *docids;              /* A linked list used in the final phase */
    GTree *word_tree;           /* A balanced tree to store words in sorted way */

    guint num_reducers;         /* Keep track of the number of reducers used */
    gchar *path;                /* Output path where files will be saved */
};

typedef struct _Parser Parser;

Parser* parser_new(guint num_reducers, const gchar *input, const gchar *path);
void parser_free(Parser *parser);
void parser_run(Parser *parser, guint limit);

#endif
