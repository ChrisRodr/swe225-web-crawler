import math
from collections import defaultdict
from utils.file_handler import inverted_index_postings

def generate_doc_norm_file(output_dir):
	# Compute squared sum of TF-IDF values for each document
	doc_squared_sum = defaultdict(float)
	for token, postings in inverted_index_postings(output_dir):
		for row in postings:
			doc_id = row[0]
			doc_tfidf = float(row[1])
			doc_squared_sum[doc_id] += doc_tfidf ** 2

	# Compute the L2 Norm for each document
	doc_norms = {doc_id: math.sqrt(sq_sum) for doc_id, sq_sum in doc_squared_sum.items()}
	sorted_doc_norms = dict(sorted(doc_norms))

	# Write this to a file
	doc_norm_file = "doc_norms.txt"
	with open(doc_norm_file, mode='w') as file:
		for doc_id, norm in sorted_doc_norms.items():
			file.write(f"{doc_id} {norm}\n")

	print(f"Generated Document Norms to {doc_norm_file}")


def main():
	generate_doc_norm_file("output")

if __name__ == "__main__":
	main()