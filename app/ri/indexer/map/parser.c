#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <archive_entry.h>
#include "parser.h"

struct _Iterator
{
    guint num_reducers;  /* The number of reducers we are using */
    guint counter;       /* A simple counter for tuples */

    FILE *file;
    FILE **files;        /* Array of files. They are totally nun_reducers */

    GList *docids;       /* A sorted list of documents ids */

    GHashTable *words;   /* The hashtable containing all words. It is inherithed
                          * by the Parser object
                          */
    GHashTable *table;   /* The hashtable containing the current occurrences
                          * It goes from docid -> occurences
                          */
};

typedef struct _Iterator Iterator;

static inline WordType get_word_type(gunichar c)
{
    /* Fast ascii handling */
    if (IS_ASCII (c)) {
        if (IS_ASCII_ALPHA_LOWER (c)) {
            return WORD_ASCII_LOWER;
        }

        if (IS_ASCII_ALPHA_HIGHER (c)) {
            return WORD_ASCII_HIGHER;
        }

        if (IS_ASCII_IGNORE (c)) {
            return WORD_IGNORE;
        }

        if (IS_ASCII_NUMERIC (c)) {
            return WORD_NUM;
        }

        if (IS_HYPHEN (c)) {
            return WORD_HYPHEN;
        }

        if (IS_UNDERSCORE (c)) {
            return WORD_UNDERSCORE;
        }
    } else {
        if (g_unichar_isalpha (c)) {
            if (!g_unichar_isupper (c)) {
                    return WORD_ALPHA_LOWER;
            } else {
                    return WORD_ALPHA_HIGHER;
            }
        } else if (g_unichar_isdigit (c)) {
            return WORD_NUM;
        }
    }

    return WORD_IGNORE;
}

static inline gint word_compare(const gchar *a, const gchar *b, gpointer data)
{
    return g_strcmp0(a, b);
}

static FILE* create_file(guint reducer_idx)
{
    int fd, i;
    guint fid, nibble;
    GString *filename = g_string_new("");
    g_string_printf(filename, FILE_FORMAT, reducer_idx, 0);

    while (1)
    {
        fid = 0;

        for (i = 0; i < ID_LENGTH; i++)
        {
            nibble = rand() % 9;
            filename->str[i + ID_OFFSET] = '1' + nibble;

            fid *= 10;
            fid += nibble;
        }

        printf("Checking file %s\n", filename->str);

        if ((fd = open(filename->str, O_RDWR | O_CREAT | O_EXCL,
                       S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0)
            continue;

        printf("FID: %u\n", fid);

        g_string_free(filename, TRUE);
        return fdopen(fd, "w+");
    }
}

static void parser_reset(Parser *parser)
{
    g_hash_table_remove_all(parser->dict);
    g_hash_table_remove_all(parser->docid_set);
    g_tree_destroy(parser->word_tree);

    parser->word_tree = g_tree_new_full((GCompareDataFunc)word_compare, NULL, NULL, g_free);
}

static void write_tuple(const guint *docid, Iterator *iter)
{
    const guint *occurr = g_hash_table_lookup(iter->table, docid);

    if (!occurr)
        return;

    fwrite(docid, sizeof(guint), 1, iter->file);
    fwrite(occurr, sizeof(guint), 1, iter->file);

    iter->counter++;
}

static inline gint guint_compare(guint *a, guint *b)
{
    if (*a == *b) return 0;
    if (*a < *b) return -1;
    if (*a > *b) return 1;
}

static gboolean traverse_node(gchar *word, gchar *_, Iterator *iter)
{
    FILE *file;
    glong marker;
    size_t len = strlen(word);

    iter->table = g_hash_table_lookup(iter->words, word);
    iter->file = iter->files[(g_str_hash(word) % iter->num_reducers)];

    fwrite((guint *)&len, sizeof(guint), 1, iter->file);
    fwrite(word, sizeof(gchar), len, iter->file);

    marker = ftell(iter->file);
    fwrite((guint []){0}, sizeof(guint), 1, iter->file);


    iter->counter = 0;
    g_list_foreach(iter->docids, (GFunc)write_tuple, iter);

    fseek(iter->file, marker, 0);
    fwrite(&iter->counter, sizeof(guint), 1, iter->file);
    fseek(iter->file, 0, 2);

    fwrite((gchar []){'\n'}, sizeof(gchar), 1, iter->file);

    return FALSE;
}

static void parser_flushdict(Parser *parser)
{
    int i;
    Iterator iter;

    iter.file = NULL;
    iter.files = g_new0(FILE *, parser->num_reducers);

    for (i = 0; i < parser->num_reducers; i++)
        iter.files[i] = create_file(i);

    iter.num_reducers = parser->num_reducers;
    iter.docids = g_hash_table_get_keys(parser->docid_set);

    printf("Sorting docids set.. ");
    iter.docids = g_list_sort(iter.docids, (GCompareFunc)guint_compare);
    printf("ok\n");

    iter.words = parser->dict;

    printf("Flushing dictionary %d\n", g_hash_table_size(parser->dict));
    g_tree_foreach(parser->word_tree, (GTraverseFunc)traverse_node, &iter);

    for (i = 0; i < parser->num_reducers; i++)
        fclose(iter.files[i]);

    g_free(iter.files);
    g_list_free(iter.docids);

    parser_reset(parser);
}

static inline guint *get_or_create_docid(Parser *parser, guint docid)
{
    guint *docid_key = g_hash_table_lookup(parser->docid_set, &docid);

    if (!docid_key)
    {
        docid_key = g_new(guint, 1);
        *docid_key = docid;

        g_hash_table_insert(parser->docid_set, docid_key, docid_key);
    }

    return docid_key;
}

static void dict_push_new_word(Parser *parser, const guint docid, const char *word)
{
    /* First we have to check if the docid is already in the docid set */
    gchar *word_key;
    guint *docid_key;
    guint *counter;
    GHashTable *inner;

    docid_key = get_or_create_docid(parser, docid);

    /* Let's create the dictionary */
    inner = g_hash_table_new_full(g_int_hash, g_int_equal, NULL, g_free);

    //printf("%p ALLOC\n", inner);

    counter = g_new(guint, 1);
    *counter = 1;

    word_key = g_strdup(word);

    g_hash_table_insert(inner, docid_key, counter);
    g_hash_table_insert(parser->dict, word_key, inner);

    g_tree_insert(parser->word_tree, word_key, word_key);
}

static void parser_putword(Parser *parser, const guint docid, const char *word, glong bytes)
{
    guint *docid_key, *value;
    GHashTable *inner = g_hash_table_lookup(parser->dict, word);

    if (!inner)
    {
        dict_push_new_word(parser, docid, word);
        parser->length += bytes + (sizeof(guint) * 2) + 1;
        return;
    }

    parser->length += sizeof(guint) * 2;
    value = (guint *)g_hash_table_lookup(inner, &docid);

    if (value != NULL)
    {
        *value += 1;
        return;
    }

    docid_key = get_or_create_docid(parser, docid);

    value = g_new(guint, 1);
    *value = 1;

    g_hash_table_insert(inner, docid_key, value);
}

static void parse_file(Parser *parser, guint docid, const char *buff, size_t len)
{
    guint pos = 0;

    gchar *utf8;
    const gchar *p;
    gboolean started;

    gunichar c;
    gunichar word[MAXWORD];
    guint length;
    glong bytes;

    WordType type;

    type = WORD_IGNORE;
    length = 0;

    started = FALSE;

    for(p = buff; pos < len; p = g_utf8_next_char(p), pos++)
    {
        c = g_utf8_get_char(p);
        type = get_word_type(c);

        if (type == WORD_IGNORE)
        {
            if (!started)
                continue;
            else
            {
                // We have a word here.
                utf8 = g_ucs4_to_utf8 (word, length, NULL, &bytes, NULL);

                if (utf8)
                {
                    parser_putword(parser,
                                   docid,
                                   sb_stemmer_stem(parser->stemmer, utf8, bytes),
                                   bytes);
                    g_free(utf8);
                }

                started = FALSE;
                length = 0;

                continue;
            }
        }

        started = TRUE;

        if (length > MAXWORD)
            continue;

        length++;

        if (type == WORD_ASCII_HIGHER)
            c += 32;
        else if (type == WORD_ALPHA_HIGHER)
            c = g_unichar_tolower(c);

        word[length - 1] = c;
    }
}

static guint extract_docid(const char *filename)
{
    guint ret = (guint)atoi(filename + 4);
    //printf("Returning %d %s\n", ret, filename + 4);
    return ret;
}

static void destroy_inner(gpointer inner)
{
    //printf("%p DEALLOC\n", inner);
    g_hash_table_destroy((GHashTable *)inner);
    //g_mem_profile();
}

Parser* parser_new(guint num_reducers, const char *input)
{
    Parser *parser = NULL;
    struct archive *arch = archive_read_new();

    archive_read_support_compression_all(arch);
    archive_read_support_format_all(arch);

    if (archive_read_open_filename(arch, input, 8192) != ARCHIVE_OK)
        return NULL;

    parser = g_new0(struct _Parser, 1);

    parser->input = arch;
    parser->num_reducers = num_reducers;
    parser->stemmer = sb_stemmer_new("english", "UTF_8");

    parser->dict = g_hash_table_new_full(g_str_hash, g_str_equal, NULL, (GDestroyNotify)destroy_inner);
    parser->word_tree = g_tree_new_full((GCompareDataFunc)word_compare, NULL, NULL, g_free);
    parser->docid_set = g_hash_table_new_full(g_int_hash, g_int_equal, NULL, g_free);

    return parser;
}


void parser_free(Parser *parser)
{
    archive_read_finish(parser->input);
    sb_stemmer_delete(parser->stemmer);

    g_hash_table_destroy(parser->dict);
    g_hash_table_destroy(parser->docid_set);
    g_tree_destroy(parser->word_tree);

    g_free(parser);

    g_mem_profile();
}

void parser_run(Parser *parser, glong limit)
{
    size_t size;
    char buff[MAXLEN];
    struct archive_entry *entry;
    guint num_files = 0;

    limit *= 2;

    //g_mem_set_vtable(glib_mem_profiler_table);

    while (archive_read_next_header(parser->input, &entry) == ARCHIVE_OK) {
        guint docid = extract_docid((const char *)archive_entry_pathname(entry));
        size = archive_read_data(parser->input, &buff, MAXLEN);

        if (size > 0)
        {
            parse_file(parser, docid, &buff[0], size);
            num_files++;

            if (parser->length >= limit)
            {
                parser_flushdict(parser);
                printf("Writing down partial result for %d files\n", num_files);
                num_files = 0;
            }
        }
    }

    if (num_files > 0)
        parser_flushdict(parser);
}
