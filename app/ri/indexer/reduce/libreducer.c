#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "libreducer.h"
#include <glib.h>

FileReader* file_reader_new(int reducer_id, int file_id)
{
    FileReader *reader = g_new0(struct _FileReader, 1);

    GString *name = g_string_new("");
    g_string_printf(name, "output-r%06d-p%06d", reducer_id, file_id);

    reader->filename = g_strdup(name->str);
    reader->file = fopen(name->str, "rb");

    g_string_free(name, TRUE);

    if (reader->file == NULL) {
        printf("Unable to open %s\n", reader->filename);

        g_free(reader->filename);
        g_free(reader);

        return NULL;
    }

    return reader;
}

gboolean file_reader_next(FileReader *reader, Posting *post)
{
    gchar delim = '\0';
    guint termlen = 0;
    gboolean read_next = FALSE;

    Cursor *cur = reader->cur;

    if (cur != NULL && cur->current == cur->postings)
    {
        fread(&delim, sizeof(gchar), 1, reader->file);

        if (delim != '\n')
        {
            printf("Error: bogus delimiter on position %u\n", ftell(reader->file));
            return TRUE;
        }

        read_next = TRUE;
    }
    else if (cur == NULL)
    {
        cur = g_new0(struct _Cursor, 1);
        cur->term = g_string_new("");
        read_next = TRUE;
    }

    if (read_next == TRUE) {
        fread(&termlen, sizeof(guint), 1, reader->file);

        if (feof(reader->file))
        {
            printf("Reached EOF\n");
            return TRUE;
        }

        if (termlen > 100 || termlen == 0)
            printf("Error: length=%d file=%s pos=%d\n", termlen, reader->filename, ftell(reader->file));

        cur->term = g_string_set_size(cur->term, termlen);
        fread(cur->term->str, sizeof(gchar), termlen, reader->file);

        cur->current = 0;
        fread(&cur->postings, sizeof(guint), 1, reader->file);

        reader->cur = cur;
    }

    if (cur->current < cur->postings)
    {
        cur->current++;
        fread(&post->docid, sizeof(guint), 1, reader->file);
        fread(&post->occurrence, sizeof(guint), 1, reader->file);
        post->term = cur->term;
    }

    return FALSE;
}

void file_reader_close(FileReader *reader)
{
    g_free(reader->filename);
    fclose(reader->file);

    if (reader->cur)
    {
        if (reader->cur->term)
            g_string_free(reader->cur->term, TRUE);
        g_free(reader->cur);
    }

    g_free(reader);
}

void reduce(guint reducer_idx, guint nfile, guint *ids, reduce_callback callback, gpointer udata)
{
    guint i, j, res, stop, iterations = 0;
    Posting post[nfile];
    FileReader* readers[nfile], *reader;

    for (i = 0; i < nfile; i++)
    {
        reader = file_reader_new(reducer_idx, ids[i]);
        file_reader_next(reader, &post[i]);
        readers[i] = reader;
    }

    while (1)
    {
        Posting *minimum = &post[0], *current = &post[1];

        for (i = 1; i < nfile; i++)
        {
            res = memcmp(minimum->term->str, current->term->str,
                         MIN(minimum->term->len, current->term->len));

            if (res > 0 || (res == 0 && minimum->docid > current->docid))
                minimum = current;

            current++;
        }

        callback(minimum, udata);

        i = minimum - &post[0];
        iterations ++;

        if (file_reader_next(readers[i], &post[i]) == TRUE)
        {
            file_reader_close(readers[i]);

            if (nfile == 1)
                break;

            stop = nfile - i - 1;

            // We need to do a reallocation and remove this file
            for (j = 0; j < stop; j++,i++) {
                readers[i] = readers[i + 1];
                post[i] = post[i + 1];
            }
            nfile--;
        }
    }

    callback(NULL, udata);

    printf("Iterations: %d\n", iterations);

    return;
}
