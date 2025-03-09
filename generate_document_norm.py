import math
from collections import defaultdict
from utils.file_handler import inverted_index_postings
import sys

def generate_doc_norm_file(output_dir):
	print("Reading files and creating doc_squared_sum...")
	# Compute squared sum of TF-IDF values for each document
	num_postings_read = 0
	doc_squared_sum = defaultdict(float)
	for row in inverted_index_postings(output_dir):
		num_postings_read += 1
		doc_id = row[0]
		doc_tfidf = float(row[1])
		doc_squared_sum[doc_id] += doc_tfidf ** 2
		print(f"\rNumber of postings processed: {num_postings_read}", end='')
		sys.stdout.flush()

	# Compute the L2 Norm for each document
	print("\nCalculating the doc_norms")
	doc_norms = {doc_id: math.sqrt(sq_sum) for doc_id, sq_sum in doc_squared_sum.items()}
	print("Sorting the doc norms by doc_id")
	sorted_doc_norms = dict(sorted(doc_norms))

	# Write this to a file
	doc_norm_file = "doc_norms.txt"
	print(f"Writing the doc norms to {doc_norm_file} file")
	with open(doc_norm_file, mode='w') as file:
		for doc_id, norm in sorted_doc_norms.items():
			file.write(f"{doc_id} {norm}\n")

	print(f"Generated Document Norms to {doc_norm_file}")

def main():
	generate_doc_norm_file("output")

if __name__ == "__main__":
	main()