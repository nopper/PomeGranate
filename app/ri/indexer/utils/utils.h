#ifndef UTILS_H
#define UTILS_H

#include <glib.h>

#define ID_LENGTH 6
#define ID_OFFSET 16
#define FILE_FORMAT "output-r%06u-p%06u"

/* Just an extended structure to keep track of the file name */
struct _ExFile
{
    FILE *file;   /* The file pointer */
    gchar *fname; /* The name of the file. To be freed with g_free */
};

typedef struct _ExFile ExFile;

ExFile* create_file(const gchar *path, guint reducer_idx);

#endif // UTILS_H
