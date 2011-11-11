#include <stdio.h>
#include <stdlib.h>
#include "libreducer.h"
#include "utils.h"

struct _Context {
    gchar *str;
    guint length;

    guint docid;
    guint occurrence;
    glong position;
    guint num_tuples;

    FILE *file;
};

typedef struct _Context Context;

void callback(Posting *post, Context *ctx)
{
    gboolean same_id, same_word;

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
        //fclose(ctx->file);

        return;
    }

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

    if (same_id && same_word)
        ctx->occurrence += post->occurrence;
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
    guint *ids;
    ExFile *file;
    Context *ctx;
    guint reducer_idx;

    printf("int: %d guint: %d\n", sizeof(unsigned int), sizeof(guint));

    if (argc < 3)
    {
        printf("Usage: %s <path> <reduceidx> <int>..\n", argv[0]);
        return -1;
    }

    ids = g_new(guint, (argc - 3));
    reducer_idx = (guint)atoi(argv[2]);

    for (i = 3; i < argc; i++)
        *(ids + (i - 3)) = (guint)atoi(argv[i]);

    file = create_file(argv[1], reducer_idx);
    ctx = g_new0(struct _Context, 1);

    ctx->str = NULL;
    ctx->file = file->file;

    reduce(argv[1], reducer_idx, argc - 3, ids,
           (reduce_callback)callback, (gpointer *)ctx);

    printf("=> %s %lu\n", file->fname, ftell(file->file));

    /* Yes this is lame. Better to create exfile_close or something similar */
    fclose(file->file);
    g_free(file->fname);
    g_free(file);

    g_free(ctx);
    g_free(ids);
}
