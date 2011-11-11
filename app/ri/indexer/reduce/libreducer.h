#ifndef _LIBREDUCER_H_
#define _LIBREDUCER_H_

#include <glib.h>

struct _Cursor {
  int postings;
  int current;
  GString *term;
};

struct _FileReader {
  FILE *file;
  char *filename;
  struct _Cursor *cur;
};

struct _Posting {
  int docid;
  int occurrence;
  GString *term;
};

typedef struct _Cursor Cursor;
typedef struct _FileReader FileReader;
typedef struct _Posting Posting;

FileReader *file_reader_new(const gchar *path, int reducer_id, int file_id);
gboolean file_reader_next(FileReader *reader, Posting *post);
void file_reader_close(FileReader *reader);

typedef void (*reduce_callback)(Posting *post, gpointer udata);
void reduce(const gchar *path, guint reducer_idx, guint nfile, guint *ids,
            reduce_callback callback, gpointer udata);

#endif
