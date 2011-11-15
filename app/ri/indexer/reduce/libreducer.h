#ifndef _LIBREDUCER_H_
#define _LIBREDUCER_H_

#include <glib.h>

/*! \brief The structure represent a simple cursor that is used to manage the
 *         reading operation during the reduce phase.
 *
 *  It keeps track of the current term being read and thanks to current
 *  of the current position of the posting lists we are reading.
 */
struct _Cursor {
  guint postings; /*!< The total number of postings tuples */
  guint current;  /*!< The current position. Always <= postings */
  GString *term;  /*!< The current term being read */
};

/*! \brief The FileReader is used to keep track of different inputs file which
 * are going to being merged
 */
struct _FileReader {
  FILE *file;          /*!< A file pointer from which we are reading from */
  gchar *filename;     /*!< A string representing the file name */
  struct _Cursor *cur; /*!< The cursors representing a sort of position in the
                            file */
};

/*! \brief A simple object representing a *SINGLE* posting. It can be seen as a
 * triple <term, docid, occurrence
 */
struct _Posting {
  guint docid;      /*!< The document ID */
  guint occurrence; /*!< The occurrence of the term in the document */
  GString *term;    /*!< The term itself */
};

typedef struct _Cursor Cursor;
typedef struct _FileReader FileReader;
typedef struct _Posting Posting;

/*! \brief Create a new FileReader object
 * \param path the path where the file resides into
 * \param reducer_idx the reducer id
 * \param file_id the unique ID identifying the file
 * \return a new FileReader object that should be closed with file_reader_close
 *         or NULL if the file was not found
 */
FileReader *file_reader_new(const gchar *path, guint reducer_id, guint file_id);

/*! \brief Advance the cursor of the FileReader object and update the post
 * pointer.
 *
 * The function will advance reading the file and consequently update the
 * Posting object argument
 *
 * \param reader the FileReader object
 * \param post the Posting object that will be updated
 * \return TRUE if the file reached the end and no more data is available
 */
gboolean file_reader_next(FileReader *reader, Posting *post);

/*! \brief Close the input file and deallocate the FileReader object
 * \param reader the FileReader object
 */
void file_reader_close(FileReader *reader);

/*! \brief User defined callback for the reduce
 *
 * The callback is called at each iteration of the reduce function. Each time
 * the function is called the post pointer will contain up to date information
 * about the current posting being processed. The post object will point to the
 * current minimum posting element. Please note that the sorting is done first
 * by term and then by docid.
 *
 * The last call suggesting the end of the reduce will be indicated by making
 * the post pointer point to NULL.
 *
 * \param post a Posting object that will be update automatically before each
 *             call
 * \param udata user defined data pointer
 */
typedef void (*reduce_callback)(Posting *post, gpointer udata);

/*! \brief Run the reduce algorithm on the inputs files
 * \param path the path where all the files are stored
 * \param reducer_idx the reducer ID
 * \param nfile an integer telling how many inputs file we are reducing
 * \param ids an array of file IDs
 * \param callback the function that will be called at each iteration
 * \param udata user-data pointer
 */
void reduce(const gchar *path, guint reducer_idx, guint nfile, guint *ids,
            reduce_callback callback, gpointer udata);

#endif
