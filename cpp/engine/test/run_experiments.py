#!/bin/env python3
import socket
import argparse
import multiprocessing

import run_combinations


def run(mode, outfile, args, threads=None, scale=None, reps=None, timeOut=None, maxTOReps=3, extraArgs=None):
	import subprocess

	args_str = "-o {out}".format(out=outfile)

	if threads is not None:
		args_str="{} -j {}".format(args_str, threads)

	if scale is not None:
		args_str="{} -s {}".format(args_str, scale)

	if reps is not None:
		args_str="{} -r {}".format(args_str, reps)

	command = "{prefix}/experiments --host {host} --mode {mode} {args} {extra}".format(
		prefix=args.binary_dir, mode=mode, host=args.host, args=args_str,
		outfile=outfile, extra=extraArgs if extraArgs is not None else "")

	if timeOut is not None and timeOut < 0:
		timeOut = None


	if timeOut is None:
		print("Run '{}' without timeout".format(command))
		process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
		process.wait()
	else:
		num_timeout = 0
		while True:
			if num_timeout >= maxTOReps:
				print("Too many timeouts for command '{}'".format(command))
				return

			print("Running command '{}' with timeOut {} sec".format(command, timeOut))

			out, err, timeout = run_combinations.Command(command).run(
				capture=True, timeout=timeOut)

			if not timeout:
				if out is not None:
					print(out)
				if err is not None:
					print(err, file = sys.stderr)
				break

			print("Got timeouts for command '{}'".format(command))
			num_timeout = num_timeout + 1

def main():
	parser = argparse.ArgumentParser(description="""Runs the experiments""", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument("--host", help="Hostname", action='store',
		default=socket.gethostname())
	parser.add_argument("--binary_dir", help="Path to binary files", action='store',
		default="@CMAKE_CURRENT_BINARY_DIR@")
	parser.add_argument("--output_dir", help="Path to resulting CSV files", action='store',
		default="@CMAKE_CURRENT_BINARY_DIR@")
	parser.add_argument("--threads", help="Number of threads", action='store',
		default=multiprocessing.cpu_count(), type=int)
	parser.add_argument("--timeout", help="Default timeout in secs", action='store',
		default=12*60*60, type=int)


	args = parser.parse_args()
	num_threads = int(args.threads)
	timeout = int(args.timeout)

	run("qperf", args.output_dir + "/qperf_all_sf50.csv", args, threads=num_threads, scale=50)
	run("speed_plot", args.output_dir + "/qspeed_1_sf50.csv", args, threads=1, scale=50)
	run("qrisk", args.output_dir + "/qrisk_1_sf10.csv", args, threads=1, scale=10)
	

	if True:
		if args.host != "jewels03.scilens.private":
			run("q6", args.output_dir + "/q6_all_sf100.csv", args, threads=num_threads, scale=100)

		if args.host in ["diamonds2.scilens.private"]:
			run("q6", args.output_dir + "/q6_all_sf300.csv", args, threads=num_threads, scale=300)

		run("q6", args.output_dir + "/q6_1_sf50.csv", args, threads=1, scale=50)
		run("q6", args.output_dir + "/q6_all_sf50.csv", args, threads=num_threads, scale=50)

    run("code_cache", args.output_dir + "/cache_1.csv", args, threads=1)
    run("code_cache", args.output_dir + "/cache_8.csv", args, threads=8)

	

	if args.host in ["diamonds2.scilens.private"]:
		run("qperf", args.output_dir + "/qperf_all_sf300.csv", args, threads=num_threads, scale=300)
		run("qperf", args.output_dir + "/qperf_all_sf500.csv", args, threads=num_threads, scale=500)

	if args.host != "jewels03.scilens.private":
		run("qperf", args.output_dir + "/qperf_all_sf100.csv", args, threads=num_threads, scale=100, timeOut=timeout)
		# run("qperf", args.output_dir + "/qperf_1_sf100.csv", args, threads=1, scale=100, timeOut=timeout)

	run("qperf", args.output_dir + "/qperf_all_sf10.csv", args, threads=num_threads, scale=10)
	run("qperf", args.output_dir + "/qperf_all_sf50.csv", args, threads=num_threads, scale=50)
	run("qperf", args.output_dir + "/qperf_1_sf50.csv", args, threads=1, scale=50)

	# run("q1sel", args.output_dir + "/q1sel_1_sf10.csv", args, threads=1, scale=10)
	# run("sumsel", args.output_dir + "/sumsel_1_sf10.csv", args, threads=1, scale=10)

	run("qperf", args.output_dir + "/qperf_1_sf1.csv", args, threads=1, scale=1)
	run("qperf", args.output_dir + "/qperf_1_sf10.csv", args, threads=1, scale=10)


	run("qrisk", args.output_dir + "/qrisk_1_sf10.csv", args, threads=1, scale=10)

	run("adapt", args.output_dir + "/adapt_1_sf10.csv", args, threads=1, reps=300, scale=10)
	run("adapt", args.output_dir + "/adapt_all_sf10.csv", args, threads=num_threads, reps=300, scale=10)

	run("join", args.output_dir + "/join_1.csv", args, threads=1)
	run("join", args.output_dir + "/join_all.csv", args)

	run("qperf", args.output_dir + "/qperf_1_sf10_100.csv", args, threads=1, scale=10, reps=300)

if __name__ == '__main__':
	main()
