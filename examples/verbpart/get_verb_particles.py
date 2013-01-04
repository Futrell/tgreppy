from tgreppy import TGrep2
import sys

NP_FIRST_QI = 3
NP_LAST_QI = 4
ORDER = False

def read_tsv(filename):
	d = {}
	infile = open(filename)
	lines = (line.strip().split("\t") for line in infile)
	return {x[0] : x[1] for x in lines}

try:
	verb_types = read_tsv("wsjverbs.tsv")
except IOError:
	verb_types = {}

def print_VPs(corpus, order=ORDER):
	t = TGrep2("/Shared/Corpora/Treebank-3/tgrep2able/%s.t2c.gz" % corpus)
	d = t.query_from_file("queries.txt", flags=["", "t"])
	d.columns = "verb_tag prt_tag np_tag verb prt np q_index".split()
	#d.columns = "verb_tag prt_tag verb prt q_index".split()
	from lemmatization_notes import l, decontract
	d["verblem"] = d["verb"].apply(lambda x: l.lemmatize(decontract(x), "v"))
	d["verblem_tag"] = d["verblem"].apply(lambda x: "(VB%s %s)" % (verb_types.get(x, "N"), x))
	for i in range(len(d)):
		if d["prt_tag"][i] and "VB" in d["prt_tag"][i]:
			d["prt"][i] = None
	d["prt_tag"] = d["prt"].apply(lambda x: "(PRT %s)" % x)
	for i in range(len(d)):
		args = []
		args.append(d["verblem_tag"][i])
		if not order or d["q_index"][i] == NP_FIRST_QI:
			if d["prt_tag"][i] and "None" not in d["prt_tag"][i]:
				args.append(d["prt_tag"][i])
			if d["np_tag"][i] and "-NONE-" not in d["np_tag"][i]:
				args.append(d["np_tag"][i])
		else:
			if d["np_tag"][i] and "-NONE-" not in d["np_tag"][i]:
				args.append(d["np_tag"][i])
			if d["prt_tag"][i] and "None" not in d["prt_tag"][i]:
				args.append(d["prt_tag"][i])
		print("((VP %s))" % " ".join(args))
	return d

if __name__ == "__main__":
	print_VPs(sys.argv[1])
