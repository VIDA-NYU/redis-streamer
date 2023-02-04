import requests

PREFIX = 'http://127.0.0.1:7890'
session = requests.Session()

#### STREAMS
r = session.post(url=f'{PREFIX}/token',data={'username':'test', 'password':'test'})
assert(r.ok)
print('POST /token', r.content.decode('utf-8'))
token = r.json()['access_token']
headers = {'Authorization': f'Bearer {token}'}

r = session.get(url=f'{PREFIX}/streams/', headers=headers)
assert(r.ok)
print('GET /streams/', r.content.decode('utf-8'))

r = session.put(url=f'{PREFIX}/streams/dev0', headers=headers,
                params={'desc': 'testing stream','meta': {},'override': True})
assert(r.ok)
print('PUT /streams/dev0', r.content.decode('utf-8'))

r = session.get(url=f'{PREFIX}/streams/dev0', headers=headers)
assert(r.ok)
print('GET /streams/dev0', r.content.decode('utf-8'))

r = session.delete(url=f'{PREFIX}/streams/dev0', headers=headers)
assert(r.ok)
print('DELETE /streams/dev0', r.content.decode('utf-8'))


# #### DATA

r = session.get(url=f'{PREFIX}/data/test', headers=headers, params={'count':1,'last_entry_id':'*'})
assert(r.ok)
print('GET /data/test', r.headers.get('entry-offset'), len(r.content))

filenames = [('test','file1.bin'), ('test', 'file2.bin')]
entries = [('entries', (sid, open(fn,'rb').read())) for sid,fn in filenames]
r = session.post(url=f'{PREFIX}/data/test', headers=headers, files=entries)
assert(r.ok)
print('POST /data/test', r.content.decode('utf-8'))

r = session.get(url=f'{PREFIX}/data/dev0', headers=headers, params={'count':2,'last_entry_id':'*'})
assert(r.ok)
print('GET /data/dev0', r.headers.get('entry-offset'), len(r.content))

r = session.get(url=f'{PREFIX}/data/test+dev0?count=2&last_entry_id=1650811721065-1+*', headers=headers)
assert(r.ok)
print('GET /data/test+dev0', r.headers.get('entry-offset'), len(r.content))
