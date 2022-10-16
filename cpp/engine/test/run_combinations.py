#! /bin/env python

import concurrent.futures
import multiprocessing
import argparse
import subprocess
import threading
import os

test_dir = "@CMAKE_CURRENT_BINARY_DIR@"
executables = [test_dir + "/test_engine"]

def create_dir(path):
	if not os.path.exists(path):
		os.makedirs(path)
		return True
	return False

# modified version of https://www.bo-yang.net/2016/12/01/python-run-command-with-timeout
class Command(object):
	def __init__(self, cmd):
		self.cmd = cmd
		self.process = None
		self.out = None
		self.err = None

	def run_command(self, capture = False):
		if not capture:
			self.process = subprocess.Popen(self.cmd,shell=True)
			self.process.communicate()
			return
		# capturing the outputs of shell commands
		self.process = subprocess.Popen(self.cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,stdin=subprocess.PIPE)
		out,err = self.process.communicate()
		if len(out) > 0:
			self.out = bytes.decode(out).splitlines()
		else:
			self.out = None
		if len(err) > 0:
			self.err = bytes.decode(err).splitlines()
		else:
			self.err = None

	# set default timeout to 2 minutes
	def run(self, capture = False, timeout = 120):
		thread = threading.Thread(target=self.run_command, args=(capture,))
		thread.start()
		thread.join(timeout)
		timed_out = False
		if thread.is_alive():
			print('Command timeout, kill it: ' + self.cmd)
			self.process.terminate()
			thread.join()
			timed_out = True
		return (self.out, self.err, timeout)

def get_combination_path(args, unique_id):
	return "{}/combination{}/".format(args.out, unique_id)

def run_test(magic):
	exe, args, config, unique_id = magic
	output_path = get_combination_path(args, unique_id)
	create_dir(output_path)

	cmd = "{exe} --rng-seed {rng_seed} --use-colour no --wait-for-keypress never --out {out} --reporter xml --config \"{config}\"".format(
		exe=exe, rng_seed=args.rng_seed,
		out="{}/junit.xml".format(output_path), config=config)

	if args.fast:
		cmd = "{} --abort".format(cmd)

	print("running " + cmd)

	with open("{}/command.txt".format(output_path), "w") as cmd_file:
		cmd_file.write("{}\n".format(cmd))

		out, err, timeout = Command(cmd).run(capture=True, timeout=args.timeout)

		with open("{}/stdout.txt".format(output_path), "w") as f:
			if out is not None:
				for line in out:
					f.write(line)
					f.write("\n")

			if timeout:
				f.write("TIMEOUT\n")
			
		with open("{}/stderr.txt".format(output_path), "w") as f:
			if err is not None:
				for line in err:
					f.write(line)
					f.write("\n")

			if timeout:
				f.write("TIMEOUT\n")

import xml.etree.ElementTree as ET

def run_combinations(f, configs, args):
	f.write("""
<html>
<head>
	<title>Test results</title>
</head>
<body>""")


	table_configs = []
	for config, unique_id in configs:
		path = get_combination_path(args, unique_id)
		file = "{}/junit.xml".format(path)
		table_configs.append((file, config, unique_id))

	# Run tests
	tasks = []
	with concurrent.futures.ThreadPoolExecutor(max_workers = args.workers) as executor:
		for exe in executables:
			for config, unique_id in configs:
				t = executor.submit(run_test, (exe, args, config, int(unique_id)))
				tasks.append((t, config, unique_id))

	# Analyze output
	matrix_results = {}
	for file, config, unique_id in table_configs:
		root = ET.parse(file).getroot()
		for group in root.findall('Group'):
			for test in group.findall('TestCase'):
				sections = test.findall('Section')
				for sec in sections:
					key = (group.attrib["name"], test.attrib["name"],
						sec.attrib["name"])

					for res in sec.findall("OverallResults"):
						num_fail = res.attrib["failures"]

						if key not in matrix_results:
							matrix_results[key] = {}
						matrix_results[key][config] = num_fail

				if len(sections) == 0:
					key = (test.attrib["name"], test.attrib["name"], "")

					for res in sec.findall("OverallResult"):
						if res.attrib["success"] == "true":
							num_fail = 0
						else:
							num_fail = 1

						if key not in matrix_results:
							matrix_results[key] = {}
						matrix_results[key][config] = num_fail

	# Summary
	f.write("<h1>Summary</h1>")

	total_failed = 0
	total_missing = 0

	config_failed = {}
	config_missing = {}

	for _, config, _ in table_configs:
		config_failed[config] = 0
		config_missing[config] = 0

	for key in matrix_results:
		(group, test, section) = key
		columns = matrix_results[key]

		for _, config, _ in table_configs:

			if config in columns:
				num_fail = columns[config]
				num_fail = int(num_fail)

				if num_fail > 0:
					total_failed += 1
					config_failed[config] += 1
			else:
				total_missing += 1
				config_missing[config] += 1

	f.write("""
<ul>
<li>{failed} tests failed</li>
<li>{missing} tests missing</li>
</ul>""".format(failed=total_failed, missing=total_missing))

	# Matrix
	f.write("<h1>Matrix</h1>")
	f.write("""<table>
		<tr><th>Group</th><th>Case</th><th>Section</th>""")
	for (file, config, unique_id) in table_configs:
		f.write('<th><a href="{path}"><p style="font-size:smaller;">{cfg}</p></a></th>'.format(path=file,
			cfg=config.replace(";", "<br/>")))

	f.write("</tr>")

	f.write("<tr><th></th><th></th><th></th>""")
	for (file, config, unique_id) in table_configs:
		f.write("<th>{} failed</th>".format(config_failed[config]))
	f.write("</tr>")

	f.write("<tr><th></th><th></th><th></th>""")
	for (file, config, unique_id) in table_configs:
		f.write("<th>{} missing</th>".format(config_missing[config]))
	f.write("</tr>")

	f.write("<tr><th></th><th></th><th></th>""")
	for (file, config, unique_id) in table_configs:
		path = get_combination_path(args, unique_id)
		f.write('<th><a href="{path}">stdout</a></th>'.format(
			path="{}/stdout.txt".format(path),
			cfg=config))
	f.write("</tr>")

	f.write("<tr><th></th><th></th><th></th>""")
	for (file, config, unique_id) in table_configs:
		path = get_combination_path(args, unique_id)
		f.write('<th><a href="{path}">stderr</a></th>'.format(
			path="{}/stderr.txt".format(path),
			cfg=config))
	f.write("</tr>")


	for key in matrix_results:
		(group, test, section) = key
		columns = matrix_results[key]
		f.write("<tr>")
		f.write("<td>{}</td><td>{}</td><td>{}</td>".format(
			group, test, section))

		for _, config, _ in table_configs:
			color = "green"
			text = "ok"
			if config in columns:
				num_fail = columns[config]
				num_fail = int(num_fail)
				if num_fail > 0:
					color = "red"
					text = "{}".format(num_fail)
			else:
				color = "yellow"
				text = "missing"

			f.write('<td style="background-color: {color}">{text}</td>'.format(
				color=color, text=text))

		f.write("</tr>\n")

	f.write("</table>")

	f.write("""
<h1>Build Info</h1>

<table>
<tr><th>Key</th><th>Value</th></tr>

<tr><td>Binary dir</td><td>@PROJECT_BINARY_DIR@</td>
<tr><td>Build type</td><td>@CMAKE_BUILD_TYPE@</td>
</table>
""")

	f.write("""
</body>
</html>
""")

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-j", "--workers", help="Number of workers",
		default=multiprocessing.cpu_count(), type=int)
	parser.add_argument("--timeout", help="Test timeout in secs",
		default=60*10, type=int)
	parser.add_argument("--fast", help="Abort after first failure",
		default=False, action="store_true")
	parser.add_argument("-o", "--out", help="Output directory",
		default=".")
	parser.add_argument("--rng_seed", help="Number of workers",
		default=13, type=int)

	args = parser.parse_args()

	print("Running combinations with {} workers".format(args.workers))
	assert(args.workers > 0)

	space_vector_sizes = [4, 1024]
	space_enable_optimizations = ["true", "false"]
	space_enable_cache = ["true", "false"]

	configs = []
	unique_id = 0

	max_parallel = multiprocessing.cpu_count()

	space = [
		(4, "false", "false", 1,				"false"),
		(1024, "true", "true", 1,				"true"),
		(1024, "true", "true", max_parallel,	"false"),
		(1024, "true", "true", max_parallel,	"true"),
		(1024, "true", "true", max_parallel*2,	"true"),
		(4, "false", "false", max_parallel/2,	"true")
		]
	for (vector_size, enable_optimizations, enable_cache, parallelism, dynamic_minmax) in space:
		s = "vector_size={};enable_optimizations={};enable_cache={};parallelism={};dynamic_minmax={};morsel_size={}".format(
			vector_size, enable_optimizations, enable_cache, parallelism, dynamic_minmax, 16*vector_size)
		if s is configs:
			pass

		configs.append((s, unique_id))
		unique_id = unique_id+1

	with open("{}/test.html".format(args.out), "w") as f:
		run_combinations(f, configs, args)
	
	print("Finished")

if __name__ == "__main__":
	main()