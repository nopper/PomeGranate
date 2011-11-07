from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

setup(
    name = 'PorterStemmer',
    version = '1.0',
    description = 'PorterStemmer python module extension',
    author = 'Francesco Piccinno',
    author_email = 'stack.box@gmail.com',
    url = 'http://tartarus.org/~martin/PorterStemmer/',
    cmdclass = {'build_ext': build_ext},
    ext_modules = [Extension("stemmer", ["stemmer.pyx", "stemlib/stemmer-impl.c"])]
)
