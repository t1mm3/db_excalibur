#!/bin/env python

import subprocess
import argparse
import os

kMagicContent = "Some Magic File Content"

def run_commands(commands):
	num_failed = 0
	processes = []
	for cmd in commands:
		p = subprocess.Popen([cmd], shell=True)
		processes.append((cmd, p))

	for (cmd, p) in processes:
		p.wait()
		if p.returncode != 0:
			print("Command '{}' failed with code {}".format(cmd, p.returncode))
			num_failed += 1

	return num_failed


def exists(path):
	if not os.path.exists(path):
		return False
	if not os.path.exists(path + "/DONE"):
		return False

	f = open(path + "/DONE")
	s = f.read()
	matches = s == kMagicContent
	f.close()
	return matches

def make_dir(path):
	if not os.path.exists(path):
		os.makedirs(path)

def add_success_file(path):
	f = open(path + "/DONE", "w")
	f.write(kMagicContent)
	f.close()

def main():
	parser = argparse.ArgumentParser(description="""Generates data for the experiments""",
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument("--binary_dir", help="Path to binary files", action='store',
		default="@CMAKE_CURRENT_BINARY_DIR@")
	parser.add_argument("--output_dir", help="Path to resulting CSV files", action='store',
		default="@CMAKE_CURRENT_BINARY_DIR@")
	parser.add_argument("-f", "--overwrite", help="Overwrite files", action='store_true')

	args = parser.parse_args()

	bin_dir = args.binary_dir
	out_dir = args.output_dir


	num_failed = 0
	path = out_dir + "/join"
	generate = args.overwrite
	if exists(path):
		print("path '{}' already created".format(path))
	else:
		print("'{}' does not exist".format(path))
		make_dir(path)
		generate = True

	if generate:
		num_failed += run_commands([
			"{bin}/join_exp_gen_data --mode build -o {opath} --config \"10000000;4;9900000\"".format(bin=bin_dir, opath=path + "/build_10000000_off9900000.csv"),
			"{bin}/join_exp_gen_data --mode build -o {opath} --config \"10000000;4;990000\"".format(bin=bin_dir, opath=path + "/build_10000000_off990000.csv"),
			"{bin}/join_exp_gen_data --mode build -o {opath} --config \"10000000;4;5000000\"".format(bin=bin_dir, opath=path + "/build_10000000_off5000000.csv"),
			"{bin}/join_exp_gen_data --mode build -o {opath} --config \"10000000;4\"".format(bin=bin_dir, opath=path + "/build_10000000.csv"),
		])

		num_failed += run_commands([
			"{bin}/join_exp_gen_data --mode probe -o {opath} --config \"4000000,10;200000000,10000000\"".format(bin=bin_dir, opath=path + "/probe_4000000_200000000.csv"),
			"{bin}/join_exp_gen_data --mode probe -o {opath} --config \"200000000,10;4000000,10000000\"".format(bin=bin_dir, opath=path + "/probe_2000000000_4000000.csv"),
			"{bin}/join_exp_gen_data --mode probe -o {opath} --config \"12000000,10;120000000,10000000\"".format(bin=bin_dir, opath=path + "/probe_12000000_120000000.csv")
		])

		if num_failed <= 0:
			assert(num_failed == 0)
			add_success_file(path)


	num_failed = 0
	path = out_dir + "/group"
	generate = args.overwrite
	if exists(path):
		print("path '{}' already created".format(path))
	else:
		print("'{}' does not exist".format(path))
		make_dir(path)
		generate = True

	if generate:
		num_failed += run_commands([
			"{bin}/join_exp_gen_data --mode probe -o {opath} --config \"20000000,4;100000000,1000000\"".format(bin=bin_dir, opath=path + "/group_2000000_100000000.csv"),
			"{bin}/join_exp_gen_data --mode probe -o {opath} --config \"100000000,4;2000000,1000000\"".format(bin=bin_dir, opath=path + "/group_100000000_2000000.csv"),
			"{bin}/join_exp_gen_data --mode probe -o {opath} --config \"6000000,4;60000000,1000000\"".format(bin=bin_dir, opath=path + "/group_6000000_60000000.csv")
		])

		if num_failed <= 0:
			assert(num_failed == 0)
			add_success_file(path)


if __name__ == '__main__':
	main()