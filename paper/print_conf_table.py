#!/bin/bash python



speedup_list = [4, 2, 1.5]

# pipeline progress in percent
progress_list = [0, 25, 40, 50, 75, 90]

# in percent
fraction_list = [100, 75, 60, 50, 25, 10]

def speedup_amdahl(seq, parallelism):
	return 1.0 / (seq + (1.0 - seq) / parallelism)

def speedup_progress_amdahl(progress, speedup):
	return speedup_amdahl(progress/100.0, speedup)

layout = "l"
first = True
for _ in progress_list:
	layout = "{}|l".format(layout)
	first = False

print("\\begin{{tabular}}{{{}}}".format(layout))
print("\\toprule")
s = "$\\phi = $"
for p in progress_list:
	s = "{} & {}\\%".format(s, p)
print("{}\\\\\n".format(s))
for speedup in speedup_list:
	print("\\midrule")
	print(" \\multicolumn{{{}}}{{l}}{{\\textbf{{Speedup of fraction $s = {}\\times$}}}}\\\\".format(
		len(progress_list)+1, speedup))
	for fraction_perc in fraction_list:
		s = "$f={}\\%$".format(fraction_perc)
		for progress_perc in progress_list:
			not_f = 1.0 - fraction_perc/100.0
			S = speedup_amdahl(not_f, speedup)
			# S = (fraction_perc/100.0) * speedup + (1.0 - fraction_perc/100.0)
			S = speedup_progress_amdahl(progress_perc, S)
			#print("progress={p}, runtime_fraction={f}, final_speedup={s}".format(
			#	p=progress_perc, f=fraction_perc, s=S))

			su = "{:.2f}".format(S)
			su_perc = "{:.0f}".format(S*100)

			if int(su_perc) >= 130:
				su = "\\cellcolor{{orange!10}}{}".format(su)
			elif int(su_perc) < 110:
				su = "\\cellcolor{{gray!10}}\\emph{{{}}}".format(su)
			else:
				su = "\\cellcolor{{blue!10}}{}".format(su)

			s = "{} & {}".format(s, su)
		print("{}\\\\".format(s))
print("\\bottomrule")
print("\\end{tabular}")