
import os
from dotenv import load_dotenv
load_dotenv()
from cryptography.fernet import Fernet
#Use this to encrypt google_cloud_credentials.json

def write_key():
    """
    Generates a key and save it into a file
    """
    key = Fernet.generate_key()
    with open("key.key", "wb") as key_file:
        key_file.write(key)

def load_key():
    """
    Loads the key from the current directory named `key.key`
    """
    return open("key.key", "rb").read()


write_key()
key = load_key()
f = Fernet(key)
#print(key)
#print(GOOGLE_JSON_KEY)
#print("hey")
#print(keys)


filename = "google_cloud_credentials.json"
with open(filename, "rb") as file:
        # read all file data
       file_data = file.read()

encrypted_data = f.encrypt(file_data)
encrypt_filename = "encrypt_google_cloud_credentials.json"
with open(encrypt_filename, "wb") as file:
        file.write(encrypted_data)

f = Fernet(key)
with open(encrypt_filename, "rb") as file:
# read the encrypted data
    encrypted_data = file.read()
# decrypt data
    decrypted_data = f.decrypt(encrypted_data)
    #print(decrypted_data)
# write the original file
decrypt_encrypt_filename = "decrypt_encrypt_google_cloud_credentials.json"
with open(decrypt_encrypt_filename, "wb") as file:
    file.write(decrypted_data)
