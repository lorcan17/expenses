import nltk
from nltk.stem import PorterStemmer


def get_nlp_ready(descriptions):
    tokenized_descriptions = descriptions.str.lower()
    tokenized_descriptions = tokenized_descriptions.apply(nltk.word_tokenize)

    # Use NLTK's Porter stemmer to stem the tokens
    stemmer = PorterStemmer()
    stemmed_tokens= tokenized_descriptions.apply(lambda d : [stemmer.stem(t) for t in d])

    # Join tokens into one string
    stemmed_tokens = stemmed_tokens.apply(' '.join)

    return stemmed_tokens
