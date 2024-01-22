from flask import Flask, render_template, request
from setupsocket import on_select

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/result',methods = ['POST', 'GET'])
def result():
   if request.method == 'POST':
      formresult = request.form.getlist('stocks')
      print('FORMRESULT',formresult[0])
      result = on_select(stockname=formresult[0])
      print(result)
      return render_template("result.html",result = result)