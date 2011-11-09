#include <stdio.h>
#include <stdlib.h>
#include "reducer.h"

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

        fwrite(&ctx->docid, sizeof(guint), 1, ctx->file);
        fwrite(&ctx->occurrence, sizeof(guint), 1, ctx->file);
        fwrite((gchar []){'\n'}, sizeof(gchar), 1, ctx->file);
        fclose(ctx->file);

        return;
    }

    if (ctx->str == NULL)
    {
        ctx->str = g_strdup(post->term->str);
        ctx->length = post->term->len;

        ctx->docid = post->docid;
        ctx->occurrence = 0;
        ctx->position = 0;
        ctx->num_tuples = 0;
    }

    same_id = (post->docid == ctx->docid);
    same_word = (g_strcmp0(post->term->str, ctx->str) == 0);

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
    else
    {
        if (ctx->position > 0)
        {
            fseek(ctx->file, ctx->position, 0);
            fwrite(&ctx->num_tuples, sizeof(guint), 1, ctx->file);
            fseek(ctx->file, 0, 2);
        }

        if (ftell(ctx->file) > 0)
            fwrite((gchar []){'\n'}, sizeof(gchar), 1, ctx->file);

        fwrite(&ctx->length, sizeof(guint), 1, ctx->file);
        fwrite(ctx->str, sizeof(gchar), ctx->length, ctx->file);

        ctx->position = ftell(ctx->file);
        fwrite((gchar []){'\xDE', '\xAD', '\xC0', '\xDE'}, sizeof(gchar), 4, ctx->file);

        fwrite(&ctx->docid, sizeof(guint), 1, ctx->file);
        fwrite(&ctx->occurrence, sizeof(guint), 1, ctx->file);

        g_free(ctx->str);

        ctx->str = g_strdup(post->term->str);
        ctx->length = post->term->len;
        ctx->occurrence = post->occurrence;
        ctx->num_tuples = 1;
    }
}

int main(int argc, char *argv[])
{
    printf("int: %d guint: %d\n", sizeof(unsigned int), sizeof(guint));

    if (argc < 2)
    {
        printf("Usage: %s <outputfile> <int>..\n", argv[0]);
        return -1;
    }

    int *ids = (int *)malloc(sizeof(int) * (argc - 2));

    for (int i = 2; i < argc; i++)
        *(ids + (i - 2)) = atoi(argv[i]);

    Context *ctx = g_new0(struct _Context, 1);
    ctx->str = NULL;
    ctx->file = fopen("/tmp/out", "wb");

    reduce(argc - 2, ids, (reduce_callback)callback, (gpointer *)ctx);

    free(ids);
}
