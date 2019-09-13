import sys
import psycopg2
import requests

if len(sys.argv) == 1:
    sys.stderr.write('No table defined\n')
    sys.exit(1)

table = sys.argv[1]

conn = psycopg2.connect("dbname='dbname' user='user' host='localhost' password='password'")
cur = conn.cursor()

select_rows_stm = '''SELECT t1.record_id, t1.product_name
FROM {table}_product_list t1
LEFT OUTER JOIN {table}_contracts t2 ON t1.record_id = t2.record_id
WHERE t2.record_id IS NULL'''.format(table=table)
cur.execute(select_rows_stm)
rows = cur.fetchall()

for idx, row in enumerate(rows, start=1):
    row_id = row[0]
    text = row[1]

    r = requests.get(url='https://www.hlidacstatu.cz/api/v1/search',
                     params={'query': text}, headers={'Authorization': 'Token ' + 'TOKEN'})
    data = r.json()

    for item in data['items']:
        contract = {'record_id': row_id,
                    'sign_date': item['datumUzavreni'],
                    'payer': item['Platce']['ico'],
                    'supplier': item['Prijemce'][0]['ico'],
                    'description': item['predmet'],
                    'amount': item['CalculatedPriceWithVATinCZK']}
        insert_stm = '''INSERT INTO {table}_contracts (record_id, sign_date, payer, supplier, description, amount)
VALUES (%s, %s, %s, %s, %s, %s)'''.format(table=table)
        cur.execute(insert_stm, list(contract.values()))
    conn.commit()
cur.close()
