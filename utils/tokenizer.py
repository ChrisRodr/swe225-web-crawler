import string
import sys
from collections import Counter

"""
The time complexity of this function is O(n) where n is the number of words in the file.
The space complexity is O(n) where n is the number of words in the file.

This function removes all punctuation and separates the words.
For example "hello*world" becomes "hello world" and will count as 2 words.
The idea is that punctuation does not count as a token, and only words with alphanumeric characters are counted.
"""
def tokenize(textFilePath):
	# Exclude both underscore and apostrophe from punctuation
	all_punctuation = string.punctuation.replace('_', '').replace("'", '') + "¡¿""''«»–—‒…‽†‡"

	tokens = []

	line = ' '.join(line.split())
	line = line.translate(str.maketrans(all_punctuation, ' ' * len(all_punctuation))).lower()
	tokens.extend(line.split())

	return tokens

"""
The time complexity of this function is O(n) where n is the number of words in the file.
The space complexity is O(n) where n is the number of words in the file since we are 
using a dictionary to store the word frequencies.
"""
def computeWordFrequencies(tokens):
	return dict(Counter(tokens))

"""
The time complexity of this function is O(nlogn) where n is the number of unique words in the file.
The space complexity is O(1) since we are not using any additional space.
"""
def printnew(word_frequency):
	sorted_frequencies = sorted(word_frequency.items(), key=lambda x: x[1], reverse=True)
	for word, freq in sorted_frequencies:
		print(f"{word}: {freq}")

def main():
	if len(sys.argv) < 2:
		sys.exit(1)
		
	file_name = sys.argv[1]
	
	tokens = tokenize(file_name)
	word_frequency = computeWordFrequencies(tokens)
	printnew(word_frequency)

if __name__ == "__main__":
	main()