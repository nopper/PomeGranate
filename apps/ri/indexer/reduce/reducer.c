#include <stdio.h>
#include <stdlib.h>
#include "libreducer.h"
#include "utils.h"

struct _Context {
    gchar *str;      /*!< The term string being flushed */
    guint length;    /*!< The length of the term string  */

    guint docid;      /*!< The current docID */
    guint occurrence; /*!< The occurence of the term in docid */

    guint num_tuples; /*!< The length of the posting list */
    glong position;   /*!< A placeholder in the file for storing the length of
                       *   the posting list when we reach the end of the list
                       *   itself
                       */

    FILE *file; /*! The file on which we are writing down */
};

typedef struct _Context Context;

void callback(Posting *post, Context *ctx)
{
    gboolean same_id, same_word;

    /* The reduce has finished. Flush all pending informations */
    if (post == NULL)
    {
        printf("Flushing remaining stuff\n");

        if (ctx->position > 0)
        {
            fseek(ctx->file, ctx->position, 0);
            fwrite(&ctx->num_tuples, sizeof(guint), 1, ctx->file);
            fseek(ctx->file, 0, 2);
        }

        fwrite(&ctx->docid, sizeof(guint), 1, ctx->file);
        fwrite(&ctx->occurrence, sizeof(guint), 1, ctx->file);
        fwrite((gchar []){'\n'}, sizeof(gchar), 1, ctx->file);

        return;
    }

    /* If the str is NULL we are at early stage. Initialize the context */
    if (ctx->str == NULL)
    {
        ctx->str = g_strdup(post->term->str);
        ctx->length = post->term->len;

        ctx->docid = post->docid;
        ctx->occurrence = 0;
        ctx->position = 0;
        ctx->num_tuples = 1;

        fwrite(&ctx->length, sizeof(guint), 1, ctx->file);
        fwrite(ctx->str, sizeof(gchar), ctx->length, ctx->file);

        ctx->position = ftell(ctx->file);
        fwrite((gchar []){'\xDE', '\xAD', '\xC0', '\xDE'}, sizeof(gchar), 4, ctx->file);

        same_id = TRUE;
        same_word = TRUE;
    }
    else
    {
        same_id = (post->docid == ctx->docid);
        same_word = (post->term->str == ctx->str || g_strcmp0(post->term->str, ctx->str) == 0);
    }

    /* In case of same word and docid we just update the occurrences */
    if (same_id && same_word)
        ctx->occurrence += post->occurrence;
    /* If we have different docid we have to start another posting in the list */
    else if (!same_id && same_word)
    {
        fwrite(&ctx->docid, sizeof(guint), 1, ctx->file);
        fwrite(&ctx->occurrence, sizeof(guint), 1, ctx->file);

        ctx->num_tuples += 1;
        ctx->docid = post->docid;
        ctx->occurrence = post->occurrence;
    }
    else /* Different word */
    {
        fwrite(&ctx->docid, sizeof(guint), 1, ctx->file);
        fwrite(&ctx->occurrence, sizeof(guint), 1, ctx->file);

        if (ctx->position > 0)
        {
            fseek(ctx->file, ctx->position, 0);
            fwrite(&ctx->num_tuples, sizeof(guint), 1, ctx->file);
            fseek(ctx->file, 0, 2);

            fwrite((gchar []){'\n'}, sizeof(gchar), 1, ctx->file);
        }

        g_free(ctx->str);

        ctx->str = g_strdup(post->term->str);
        ctx->length = post->term->len;
        ctx->docid = post->docid;
        ctx->occurrence = post->occurrence;
        ctx->num_tuples = 1;

        fwrite(&ctx->length, sizeof(guint), 1, ctx->file);
        fwrite(ctx->str, sizeof(gchar), ctx->length, ctx->file);

        ctx->position = ftell(ctx->file);
        fwrite((gchar []){'\xDE', '\xAD', '\xC0', '\xDE'}, sizeof(gchar), 4, ctx->file);
    }
}

int main(int argc, char *argv[])
{
    guint i;
    gulong *ids;
    ExFile *file;
    Context *ctx;
    guint reducer_idx;
    guint master_id;
    guint worker_id;

    if (argc < 5)
    {
        printf("Usage: %s <master-id> <worker-id> <path> "
               "<reduceidx> <int>..\n", argv[0]);
        return -1;
    }

    ids = g_new(gulong, (argc - 5));

    master_id = (guint)atoi(argv[1]);
    worker_id = (guint)atoi(argv[2]);
    reducer_idx = (guint)atoi(argv[4]);

    for (i = 5; i < argc; i++)
        *(ids + (i - 5)) = (gulong)atol(argv[i]);

    file = create_file(argv[3], master_id, worker_id, reducer_idx);
    ctx = g_new0(struct _Context, 1);

    ctx->str = NULL;
    ctx->file = file->file;

    reduce(argv[3], master_id, worker_id, reducer_idx, argc - 5, ids,
           (reduce_callback)callback, (gpointer *)ctx);

    /* The => is a marker in order to communicate with the python interpreter
     * which is just reading the stdout of this process */
    printf("=> %s %lu\n", file->fname, ftell(file->file));

    fclose(file->file);
    g_free(file->fname);
    g_free(file);

    g_free(ctx);
    g_free(ids);
}
