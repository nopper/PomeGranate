#include <stdio.h>
#include <string.h>
#include <archive_entry.h>
#include "parser.h"
#include "utils.h"

struct _Iterator
{
    guint num_reducers;  /*!< The number of reducers we are using */
    guint *counters;     /*!< An array of counter for tuples */
    glong *markers;      /*!< An array of positions in the file */

    ExFile **files;      /*!< Array of files. They are totally num_reducers */
    gchar **buffers;     /*!< Array of buffers */

    GList *docids;       /*!< A sorted list of documents ids */

    GHashTable *words;   /*!< The hashtable containing all words. It is
                          *   inherithed by the Parser object
                          */
    GHashTable *table;   /*!< The hashtable containing the current occurrences
                          *   It goes from docid -> occurences
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

static void parser_reset(Parser *parser)
{
    g_hash_table_remove_all(parser->dict);
    g_hash_table_remove_all(parser->docid_set);
    g_tree_destroy(parser->word_tree);

    parser->word_tree = g_tree_new_full(
        (GCompareDataFunc)word_compare, NULL,
        NULL, g_free
    );
}

static void write_tuple(const guint *docid, Iterator *iter)
{
    guint i;
    const guint *occurr = g_hash_table_lookup(iter->table, docid);

    if (!occurr)
        return;

    i = (*docid % iter->num_reducers);

    fwrite(docid, sizeof(guint), 1, iter->files[i]->file);
    fwrite(occurr, sizeof(guint), 1, iter->files[i]->file);

    iter->counters[i] ++;
}

static inline gint guint_compare(guint *a, guint *b)
{
    if (*a == *b) return 0;
    if (*a < *b) return -1;
    if (*a > *b) return 1;
}

static gboolean traverse_node(gchar *word, gchar *_, Iterator *iter)
{
    guint i;
    FILE *file;
    size_t len = strlen(word);

    iter->table = g_hash_table_lookup(iter->words, word);

    /* Here we should do a doc-based partition */

    for (i = 0; i < iter->num_reducers; i++)
    {
        file = iter->files[i]->file;

        fwrite((guint *)&len, sizeof(guint), 1, file);
        fwrite(word, sizeof(gchar), len, file);

        iter->markers[i] = ftell(file);
        fwrite((guint []){0}, sizeof(guint), 1, file);

        iter->counters[i] = 0;
    }

    g_list_foreach(iter->docids, (GFunc)write_tuple, iter);

    for (i = 0; i < iter->num_reducers; i++)
    {
        file = iter->files[i]->file;

        fseek(file, iter->markers[i], 0);
        fwrite(&iter->counters[i], sizeof(guint), 1, file);
        fseek(file, 0, 2);

        fwrite((gchar []){'\n'}, sizeof(gchar), 1, file);
    }

    return FALSE;
}

static void parser_flushdict(Parser *parser)
{
    int i;
    Iterator iter;

    iter.files = g_new0(ExFile *, parser->num_reducers);
    iter.buffers = g_new0(gchar *, parser->num_reducers);
    iter.markers = g_new0(glong, parser->num_reducers);
    iter.counters = g_new0(guint, parser->num_reducers);

    for (i = 0; i < parser->num_reducers; i++)
    {
        iter.files[i] = create_file(parser->path,
                                    parser->master_id, parser->worker_id, i);
        iter.buffers[i] = (gchar *)g_malloc(BUFFSIZE);

        setvbuf(iter.files[i]->file, (void *)iter.buffers[i],
                _IOFBF, BUFFSIZE);
    }

    iter.num_reducers = parser->num_reducers;
    iter.docids = g_hash_table_get_keys(parser->docid_set);

    printf("Sorting docids set.. ");
    iter.docids = g_list_sort(iter.docids, (GCompareFunc)guint_compare);
    printf("ok\n");

    iter.words = parser->dict;

    printf("Flushing dictionary %d\n", g_hash_table_size(parser->dict));
    g_tree_foreach(parser->word_tree, (GTraverseFunc)traverse_node, &iter);

    for (i = 0; i < parser->num_reducers; i++)
    {
        /* That's the result line that will be parsed by the interpreter. */
        printf("=> %s %u %lu\n",
               iter.files[i]->fname, i, ftell(iter.files[i]->file));

        g_free(iter.files[i]->fname);
        fclose(iter.files[i]->file);
        g_free(iter.files[i]);
        g_free(iter.buffers[i]);
    }

    g_free(iter.files);
    g_free(iter.buffers);
    g_free(iter.markers);
    g_free(iter.counters);
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

static void dict_push_new_word(Parser *parser, const guint docid,
                               const char *word)
{
    /* First we have to check if the docid is already in the docid set */
    gchar *word_key;
    guint *docid_key;
    guint *counter;
    GHashTable *inner;

    docid_key = get_or_create_docid(parser, docid);

    /* Let's create the dictionary */
    inner = g_hash_table_new_full(g_int_hash, g_int_equal, NULL, g_free);

    counter = g_new(guint, 1);
    *counter = 1;

    word_key = g_strdup(word);

    g_hash_table_insert(inner, docid_key, counter);
    g_hash_table_insert(parser->dict, word_key, inner);

    g_tree_insert(parser->word_tree, word_key, word_key);
}

static void parser_putword(Parser *parser, const guint docid,
                           const char *word, glong bytes)
{
    guint *docid_key, *value;
    GHashTable *inner = g_hash_table_lookup(parser->dict, word);

    if (!inner)
    {
        dict_push_new_word(parser, docid, word);
        return;
    }

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

static gboolean text_validate_utf8(const gchar *text, gssize text_len,
                                   GString **str, gsize *valid_len)
{
    gsize len_to_validate;

    g_return_val_if_fail (text, FALSE);

    len_to_validate = text_len >= 0 ? text_len : strlen (text);

    if (len_to_validate > 0) {
        const gchar *end = text;

        /* Validate string, getting the pointer to first non-valid character
         *  (if any) or to the end of the string. */
        g_utf8_validate (text, len_to_validate, &end);
        if (end > text) {
            /* If str output required... */
            if (str) {
                /* Create string to output if not already as input */
                *str = (*str == NULL ?
                        g_string_new_len (text, end - text) :
                        g_string_append_len (*str, text, end - text));
            }

            /* If utf8 len output required... */
            if (valid_len) {
                *valid_len = end - text;
            }

            return TRUE;
        }
    }

    return FALSE;
}


static void parse_file(Parser *parser, guint docid,
                       const char *buff, size_t buff_len)
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

    gsize len;
    GString *str = g_string_new(NULL);

    if (!text_validate_utf8(buff, buff_len, &str, &len))
    {
        g_string_free(str, TRUE);
        return;
    }

    for(p = str->str; pos < len; p = g_utf8_next_char(p), pos++)
    {
        c = g_utf8_get_char(p);
        type = get_word_type(c);

        if (type == WORD_IGNORE)
        {
            if (!started)
                continue;
            else
            {
                /* We have a word here */
                utf8 = g_ucs4_to_utf8 (word, length, NULL, &bytes, NULL);

                if (utf8)
                {
                    parser_putword(
                        parser, docid,
                        sb_stemmer_stem(parser->stemmer, utf8, bytes), bytes);

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

    g_string_free(str, TRUE);
}

static inline guint extract_docid(const char *filename)
{
    return  (guint)atoi(filename + 4);
}

Parser* parser_new(guint master_id, guint worker_id, guint num_reducers,
                   const char *input, const char *path)
{
    Parser *parser = NULL;
    struct archive *arch = archive_read_new();

    archive_read_support_compression_all(arch);
    archive_read_support_format_all(arch);

    if (archive_read_open_filename(arch, input, INPUT_BUFFER) != ARCHIVE_OK)
        return NULL;

    parser = g_new0(struct _Parser, 1);

    parser->input = arch;
    parser->path = g_strdup(path);
    parser->num_reducers = num_reducers;
    parser->master_id = master_id;
    parser->worker_id = worker_id;
    parser->stemmer = sb_stemmer_new("english", "UTF_8");

    /* This is pretty cumbersome. The 3rd and 4th parameters are callbacks to
     * manage disposition respectively of keys and values:
     * - Destroying dict will cause g_hash_table_destroy to be called on all
     *   the inner dicts. Keys are not destroyed!
     *   \_ -> All the inner dict will only destroy the values of the dict
     *         (occurrences in this case).
     * - Destroying word_tree will destroy all the keys (terms) which are also
     *   shared with the main dict hashtable.
     * - Destroying docid_set will remove the keys (docids) shared with the
     *   inner hashtables previously destroyed.
     */

    parser->dict = g_hash_table_new_full(
        g_str_hash, g_str_equal,
        NULL, (GDestroyNotify)g_hash_table_destroy
    );

    parser->word_tree = g_tree_new_full(
        (GCompareDataFunc)word_compare, NULL,
        NULL, g_free
    );

    parser->docid_set = g_hash_table_new_full(
        g_int_hash, g_int_equal,
        g_free, NULL
    );

    return parser;
}


void parser_free(Parser *parser)
{
    archive_read_finish(parser->input);
    sb_stemmer_delete(parser->stemmer);

    g_hash_table_destroy(parser->dict);
    g_hash_table_destroy(parser->docid_set);
    g_tree_destroy(parser->word_tree);

    g_free(parser->path);
    g_free(parser);
}

/* Simple function to get memory usage on a linux system
 * Please note that this return the size in KB
 */
static guint get_memory_usage(void)
{
    FILE *fp;
    guint usage;
    char buff[128];

    if((fp = fopen("/proc/self/status","r")))
    {
        while(fgets(&buff[0], 128, fp) != NULL)
            if(strstr(&buff[0], "VmSize") != NULL)
                if (sscanf(&buff[0], "%*s %d", &usage) == 1)
                {
                    fclose(fp);
                    return usage;
                }

        fclose(fp);
    }

    return 0;
}


void parser_run(Parser *parser, guint limit)
{
    size_t size;
    char buff[MAXLEN];
    struct archive_entry *entry;
    guint docid;
    guint num_files = 0;

    srand(time(NULL));

    /* Here we will just iterate over the archive file by extracting each file
     * member one by one in our buff buffer. Then the parse_file function will
     * take care of analyzing the body of the document. */
    while (archive_read_next_header(parser->input, &entry) == ARCHIVE_OK) {
        docid = extract_docid((const char *)archive_entry_pathname(entry));
        size = archive_read_data(parser->input, &buff, MAXLEN);

        if (size > 0)
        {
            parse_file(parser, docid, &buff[0], size);
            num_files++;

            /* In case the memory limit is reached we flush the information on
             * the disk */
            if (get_memory_usage() >= limit)
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
