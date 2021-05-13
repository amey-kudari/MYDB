from flask import Flask, render_template, url_for, request
import json, os
app = Flask(__name__)


"""
DATABASE
"""
class database():
    def __init__(self, name):
        self.name = './databases/' + name+'.mydb'
        if not os.path.exists(self.name):
            open(self.name, 'w')
    def insert(self, rrow):
        row = '|,|'.join([str(i) for i in rrow])
        open(self.name, 'a').write(row + '\n')
    def ret(self):
        return json.dumps([i.split('|,|') for i in open(self.name,'r').read().strip().split('\n')])
    def clear(self):
        open(self.name, 'w')
"""
END DATABASE
"""

@app.route('/')
def home():
    return render_template('home.html')


@app.route('/docs')
def docs():
    return render_template('docs.html')

# requests.post(url+'insert/qwerasdfzxcv', data = {'row' : json.dumps([1,2,'asdf'])}).content
@app.route("/query/<tablename>", methods = ['POST','GET', 'DELETE'])
def insert(tablename):
    print('GOT')
    if request.method == 'POST' and len(tablename.strip()) > 0:
        print(tablename.strip())
        row = json.loads(request.form['row'])
        print("POSTTT!!!!")
        db = database(tablename.strip())
        db.insert(row)
        return 'INSERTED'
    elif request.method == 'GET' and len(tablename.strip()) > 0:
        db = database(tablename.strip())
        return db.ret()
    elif request.method == 'DELETE' and len(tablename.strip()) > 0:
        db = database(tablename.strip())
        db.clear()
        return 'DELETED'
    else:
        return 'MAKE A POST REQUEST WITH VALID TABLENAME'


# @app.route('/get')
# def ret():


if __name__ == '__main__':
    app.run(debug=True)
