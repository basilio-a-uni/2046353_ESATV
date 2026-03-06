from textblob import TextBlob
from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route("", methods=['POST'])
def rest_sensors():
    


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
