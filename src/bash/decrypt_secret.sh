#!/bin/sh

# Decrypt the file
# mkdir $HOME/
# --batch to prevent interactive command
# --yes to assume "yes" for questions
gpg --quiet --batch --yes --decrypt --passphrase="$ENCRYPT_KEY" \
--output config/google_cloud_credentials.json config/google_cloud_credentials.json.gpg
