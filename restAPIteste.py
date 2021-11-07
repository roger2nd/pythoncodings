import urequests as requests # upython requests lib =urequests
#import requests
import json

URL = 'https://teste-de-comunicacao.firebaseio.com/.json'

#para = json.dumps({'hu':25})
#dado = {'hu' : 25}
payload = {'Corrente':2,
            'Tensao':25,
            'Temperatura': 26,
            'Ilu':600,
            'Humidade': 80}
#r = requests.patch(url = URL, data=json.dumps({'Tensao':220}))
p = requests.get(url = URL)
data = p.json()

print(data)
