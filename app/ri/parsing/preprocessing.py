import re
import string
import glob

from stemmer import PorterStemmer


# improved list from Stone, Denis, Kwantes (2010)
STOPWORDS = """
a about above across after afterwards again against all almost alone along already also although always am among amongst amoungst amount an and another any anyhow anyone anything anyway anywhere are around as at back be
became because become becomes becoming been before beforehand behind being below beside besides between beyond bill both bottom but by call can
cannot cant co computer con could couldnt cry de describe
detail did do doesn done down due during
each eg eight either eleven else elsewhere empty enough etc even ever every everyone everything everywhere except few fifteen
fify fill find fire first five for former formerly forty found four from front full further get give go
had has hasnt have he hence her here hereafter hereby herein hereupon hers herself him himself his how however hundred i ie
if in inc indeed interest into is it its itself keep last latter latterly least less ltd
just
kg km
made many may me meanwhile might mill mine more moreover most mostly move much must my myself name namely
neither never nevertheless next nine no nobody none noone nor not nothing now nowhere of off
often on once one only onto or other others otherwise our ours ourselves out over own part per
perhaps please put rather re
quite
rather really regarding
same see seem seemed seeming seems serious several she should show side since sincere six sixty so some somehow someone something sometime sometimes somewhere still such system take ten
than that the their them themselves then thence there thereafter thereby therefore therein thereupon these they thick thin third this those though three through throughout thru thus to together too top toward towards twelve twenty two un under
until up unless upon us used using
various very very via
was we well were what whatever when whence whenever where whereafter whereas whereby wherein whereupon wherever whether which while whither who whoever whole whom whose why will with within without would yet you
your yours yourself yourselves
"""
STOPWORDS = frozenset(w for w in STOPWORDS.split() if w)

RE_TAGS = re.compile(r"<([^>]+)>")
RE_NUMERIC = re.compile(r"[0-9]+")

RE_NONALPHA = re.compile(r"[^a-z0-9\ ]")
RE_SPACES = re.compile(r"(\s|\\n|\\r|\\t)+")

RE_ALPHANUM1 = re.compile(r"([a-z]+)([0-9]+)")
RE_ALPHANUM2 = re.compile(r"([0-9]+)([a-z]+)")

PUNCT_TRANS = string.maketrans("", "")

stemmer = PorterStemmer()

def strip_punctuation(s):
    return s.translate(PUNCT_TRANS, string.punctuation)


def strip_tags(s):
    # assumes s is already lowercase
    return RE_TAGS.sub("", s)


def strip_short(s, minsize=3):
    for e in s.split():
        if len(e) >= minsize:
            yield e

def strip_numeric(s):
    return RE_NUMERIC.sub("", s)


def strip_non_alphanum(s):
    # assumes s is already lowercase
    # FIXME replace with unicode compatible regexp, without the assumption
    return RE_NONALPHA.sub(" ", s)


def strip_multiple_whitespaces(s):
    return RE_SPACES.sub(" ", s)


def split_alphanum(s):
    return RE_ALPHANUM2.sub((RE_ALPHANUM1.sub(r"\1 \2", s), r"\1 \2", s))


# Here we can further optimize by joining multiple regex in one
FILTERS = (str.lower, strip_tags, strip_punctuation,
           strip_multiple_whitespaces, strip_numeric)

def preprocess_string(s):
    for filter in FILTERS:
        s = filter(s)

    for word in strip_short(s):
        if word not in STOPWORDS:
            word = stemmer.stem(word)
            if word:
                yield word
