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

/*! \brief Create a new ExFile object
 *
 * Please note that we do not provide any kind of API to free the ExFile just
 * created. Therefore whenever you do not need the file anymore it is your
 * responsibility to free all the internal fields. E.g.:
 *   1. g_free(ex->filename);
 *   2. fclose(ex->file);
 *   3. g_free(ex);
 *
 * \param path the path in which the file should be created
 * \param reducer_idx the reduce which the file refers to
 * \return an ExFile object that should eventually be freed
 */
ExFile* create_file(const gchar *path, guint reducer_idx);

#endif // UTILS_H
