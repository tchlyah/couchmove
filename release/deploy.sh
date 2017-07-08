#!/usr/bin/env bash

openssl aes-256-cbc -K $encrypted_467352795a68_key -iv $encrypted_467352795a68_iv -in release/codesigning.asc.enc -out release/codesigning.asc -d
gpg --fast-import release/codesigning.asc

mvn deploy -P sign,build-extras -DskipTests --settings release/settings.xml