from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

sources = [
    "stemmer.pyx",

    "../libstemmer/api.c",
    "../libstemmer/libstemmer.c",
    "../libstemmer/stem_UTF_8_danish.c",
    "../libstemmer/stem_UTF_8_dutch.c",
    "../libstemmer/stem_UTF_8_english.c",
    "../libstemmer/stem_UTF_8_finnish.c",
    "../libstemmer/stem_UTF_8_french.c",
    "../libstemmer/stem_UTF_8_german.c",
    "../libstemmer/stem_UTF_8_hungarian.c",
    "../libstemmer/stem_UTF_8_italian.c",
    "../libstemmer/stem_UTF_8_norwegian.c",
    "../libstemmer/stem_UTF_8_porter.c",
    "../libstemmer/stem_UTF_8_portuguese.c",
    "../libstemmer/stem_UTF_8_russian.c",
    "../libstemmer/stem_UTF_8_spanish.c",
    "../libstemmer/stem_UTF_8_swedish.c",
    "../libstemmer/utilities.c",
]

setup(
    name = 'Stemmer',
    version = '1.0',
    description = 'Stemmer python module extension from tracker',
    author = 'Francesco Piccinno',
    author_email = 'stack.box@gmail.com',
    url = 'http://git.gnome.org/tracker',
    cmdclass = {'build_ext': build_ext},
    ext_modules = [Extension("stemmer", sources)]
)
