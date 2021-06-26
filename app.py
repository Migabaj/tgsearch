import json
from flask import Flask
from flask import render_template, redirect, url_for, request


app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/search', methods=['POST', 'GET'])
def search_results():
    from poiskovik import search
    text = request.form['query']
    search(text)

    with open("posts.json", encoding="utf-8") as f:
        result = json.loads(f.read())

    return render_template("/search.html", result=result)


if __name__ == '__main__':
    app.run(debug=True)