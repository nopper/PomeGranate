#ifndef PARSER_H
#define PARSER_H

#include <glib.h>
#include <stdio.h>
#include <archive.h>
#include "libstemmer/libstemmer.h"

#define BUFFSIZE 8192
#define INPUT_BUFFER (1024 * 1024 * 4)
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

struct _Posting {
    guint docid;
    guint occurrences;
};

typedef struct _Posting Posting;

/*! \brief The Parser structure is used to keep track of the map function.
 * All the words are stored in dict hash table. The dict maps a word to a GList
 * of Postings. Every document is read sequentially and curr_words is used to
 * keep track of the words inside that document. At the end the information
 * collected will be merged inside the dict hashtable.
 */
struct _Parser {
    struct archive *input;      /*!< The archive input file */
    struct sb_stemmer *stemmer; /*!< Stemmer */

    GHashTable *dict;      /*!< The main hash table word -> GList (reversed) */
    GHashTable *curr_words; /*!< A list containing words of the current document */
    guint curr_docid;

    guint master_id; /*!< ID identifying the master */
    guint worker_id; /*!< ID identifying the worker inside the master */

    guint num_reducers; /*!< Keep track of the number of reducers used */
    gchar *path;        /*!< Output path where files will be saved */
};

typedef struct _Parser Parser;

/*! \brief Create a new Parser object. It may return NULL in case of errors
 * \param master_id unique ID identifying the master
 * \param worker_id unique ID identifying the worker
 * \param num_reducers the reducer which are present in the computation
 * \param input the path to the input file on which execute the map
 * \param path the directory path where all the files produced will be stored
 * \return a new Parser object
 */
Parser* parser_new(guint master_id, guint worker_id, guint num_reducers,
                   const gchar *input, const gchar *path);


/*! \brief Free the parser object previously allocated */
void parser_free(Parser *parser);

/*! \brief Execute the map function without overflowing memory limit
 * \param limit limit expressed in kb about memory utilization of the process
 */
void parser_run(Parser *parser, guint limit);

#endif
