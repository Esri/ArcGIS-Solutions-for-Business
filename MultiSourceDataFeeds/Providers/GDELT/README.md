# Conda Environment

To configure an environment for the GDELT tools, run the following
steps after activating your Python 3 Command Prompt:
 * conda create --name user-gdelt --clone arcgispro-py3 -y
 * activate user-gdelt
 * pip install newspaper3k
 * python -c "import nltk; nltk.download('punkt')"