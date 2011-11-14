#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "utils.h"

ExFile* create_file(const gchar *path, guint reducer_idx)
{
    int fd, i;
    guint fid, nibble;
    ExFile *ret;
    gchar *fname;
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

        printf("Checking existence of file %s in %s\n", filename->str, path);
        fname = g_build_filename(path, filename->str, NULL);

        if ((fd = open(fname, O_RDWR | O_CREAT | O_EXCL,
                       S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0)
        {
            g_free(fname);
            continue;
        }

        ret = g_new0(struct _ExFile, 1);
        ret->file = fdopen(fd, "w+");
        ret->fname = g_strdup(filename->str);

        g_free(fname);
        g_string_free(filename, TRUE);

        return ret;
    }
}

