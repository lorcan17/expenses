import pickle
import ssl

from dotenv import load_dotenv

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

from functions import google_funcs, nlp_funcs

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context


load_dotenv()

query = "./src/sql/model.sql"
keys = google_funcs.decrypt_creds("./config/encrypt_google_cloud_credentials.json")

df = google_funcs.big_query_export(keys, query, True)
dtypes = {col: 'str' for col in df.columns}

# Create training data with descriptions and their corresponding categories
descriptions = df["exp_desc"]
categories = df["cat_name_subcat_name"]#.to_numpy()
# Use NLTK to tokenize the descriptions

stemmed_tokens = nlp_funcs.get_nlp_ready(descriptions)

# Create a TfidfVectorizer object
vectorizer = TfidfVectorizer()

# Use sklearn to split the data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(stemmed_tokens, categories, test_size=0.25)

# Use the vectorizer to encode the training and test data as tf-idf vectors
X_train = vectorizer.fit_transform(X_train)
X_test = vectorizer.transform(X_test)

# Train a logistic regression model on the training data
model = LogisticRegression()
model.fit(X_train, y_train)

# Use the trained model to make predictions on the test data
y_pred = model.predict(X_test)

# Evaluate the model's performance using the accuracy metric
accuracy = accuracy_score(y_test, y_pred)

# Print the model's accuracy
print(f"Accuracy of the model on a subset of the data is {accuracy:.2f}")

# As overfitting is not important herewe will create a model using the entire set of data
#
#  Use the vectorizer to encode the entire dataset as tf-idf vectors
X_full = vectorizer.fit_transform(stemmed_tokens)

# Train a logistic regression model on the entire dataset
model = LogisticRegression()
model.fit(X_full, categories)

# Use the  model to make predictions on the data
y_pred = model.predict(X_full)

# Evaluate the model's performance using the accuracy metric
accuracy = accuracy_score(categories, y_pred)

# Print the model's accuracy
print(f"Accuracy on the entire dataset: {accuracy:.2f}")

# Save model to 
pickle.dump(vectorizer, open("./data/vectorizer.pickle", "wb"))
pickle.dump(model, open("./data/model.pickle", "wb"))
