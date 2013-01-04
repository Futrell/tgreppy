from nltk.stem.wordnet import WordNetLemmatizer
l = WordNetLemmatizer()

contractions = {
	"'re" : "are",
	"'s" : "is",
	"'m" : "am",
	"'ve" : "have",
	"'d" : "have",
}

def decontract(w):
	return contractions.get(w, w)

def lemmatize_words(words, pos="v"):
	uwords = (decontract(w) for w in words)
	return [l.lemmatize(w, pos) for w in uwords]

def lemmatize_file(infilename, outfilename, pos="v"):
	words = (w.strip() for w in open(infilename))
	lemmata = lemmatize_words(words, pos)
	outfile = open(outfilename, "w")
	for lemma in lemmata:
		print >>outfile, lemma


