from flask import Flask, request

app = Flask(__name__)

def lcm(a, b):
    from math import gcd
    return a * b // gcd(a, b)

@app.route('/<path:email>')
def calc(email):
    x = request.args.get('x')
    y = request.args.get('y')

    try:
        x = int(x)
        y = int(y)
        if x < 0 or y < 0:
            return "NaN"
    except:
        return "NaN"

    return str(lcm(x, y))
