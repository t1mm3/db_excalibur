#! /bin/env python

import json
import argparse

def main():
	parser = argparse.ArgumentParser(description='Visualizes profiles')
	parser.add_argument('filename', nargs='+')

	args = parser.parse_args()

	# Load JSON data
	assert(len(args.filename) == 1)
	for file_name in args.filename:
		print("Visualize {}".format(file_name))

		with open(file_name) as f:
			data = json.loads(f.read())
			print(data)

		break

if __name__ == "__main__":
	main()