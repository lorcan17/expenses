import pickle
import ssl

from dotenv import load_dotenv

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

from functions import google_funcs

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context


load_dotenv()

QUERY = """select cat_name_subcat_name, exp_desc, from `budgeting.stg_expenses` a join
`budgeting.dim_splitwise_category` b on a.subcat_id = b.subcat_id"""
keys = google_funcs.decrypt_creds("./encrypt_google_cloud_credentials.json")

client = google_funcs.big_query_connect(keys)

df = google_funcs.big_query_export(keys,QUERY)
dtypes = {col: 'str' for col in df.columns}

# Create training data with descriptions and their corresponding categories
descriptions = df["exp_desc"]
categories = df["cat_name_subcat_name"]#.to_numpy()
# Use NLTK to tokenize the descriptions

stemmed_tokens = google_funcs.get_nlp_ready(descriptions)

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
print(accuracy)


pickle.dump(vectorizer, open("vectorizer.pickle", "wb"))
pickle.dump(model, open("model.pickle", "wb"))
