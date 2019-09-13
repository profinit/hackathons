import sys
import psycopg2
import time

from ufal.nametag import *


def encode_entities(text):
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def sort_entities(entities):
    return sorted(entities, key=lambda entity: (entity.start, -entity.length))


# In Python2, wrap sys.stdin and sys.stdout to work with unicode.
if sys.version_info[0] < 3:
    import codecs
    import locale

    encoding = locale.getpreferredencoding()
    sys.stdin = codecs.getreader(encoding)(sys.stdin)
    sys.stdout = codecs.getwriter(encoding)(sys.stdout)

if len(sys.argv) == 1:
    sys.stderr.write('Usage: %s recognizer_model\n' % sys.argv[0])
    sys.exit(1)

sys.stderr.write('Loading ner: ')
ner = Ner.load(sys.argv[1])
if not ner:
    sys.stderr.write("Cannot load recognizer from file '%s'\n" % sys.argv[1])
    sys.exit(1)
sys.stderr.write('done\n')

table = sys.argv[2]
if not table:
    sys.stderr.write('No table defined\n')
    sys.exit(1)

forms = Forms()
tokens = TokenRanges()
entities = NamedEntities()
sortedEntities = []
openEntities = []
tokenizer = ner.newTokenizer()
if tokenizer is None:
    sys.stderr.write("No tokenizer is defined for the supplied model!")
    sys.exit(1)

start_time = time.time()

# connect to a database
# Insert credentials
conn = psycopg2.connect("dbname='dbname' user='user' host='localhost' password='password'")
print('Connected')
cur = conn.cursor()

select_rows_stm = '''SELECT t1.record_id, t1.subject
FROM {table} t1
LEFT OUTER JOIN {table}_name_tag t2 ON t1.record_id = t2.record_id
WHERE t2.record_id IS NULL'''.format(table=table)
cur.execute(select_rows_stm)
rows = cur.fetchall()

for idx, row in enumerate(rows, start=1):
    text = ''
    row_id = row[0]
    line = '' if row[1] is None else row[1]
    line = line.rstrip('\r\n')
    text += line[:400]
    if not line: continue

    # Tokenize and recognize
    tokenizer.setText(text)
    t = 0
    text_out = '<text>'
    while tokenizer.nextSentence(forms, tokens):
        ner.recognize(forms, entities)
        sortedEntities = sort_entities(entities)

        # Write entities
        e = 0
        for i in range(len(tokens)):
            # sys.stdout.write(encode_entities(text[t:tokens[i].start]))
            if (i == 0):
                # sys.stdout.write("<sentence>")
                text_out += "<sentence>"
            # Open entities starting at current token
            while (e < len(sortedEntities) and sortedEntities[e].start == i):
                # sys.stdout.write('<ne type="%s">' % encode_entities(sortedEntities[e].type))
                text_out += ('<ne type="%s">' % encode_entities(sortedEntities[e].type))
                openEntities.append(sortedEntities[e].start + sortedEntities[e].length - 1)
                e = e + 1

            # The token itself
            # sys.stdout.write('<token>%s</token>' % encode_entities(text[tokens[i].start : tokens[i].start + tokens[i].length]))
            text_out += ('<token>%s</token>' % encode_entities(
                text[tokens[i].start: tokens[i].start + tokens[i].length]))

            # Close entities ending after current token
            while openEntities and openEntities[-1] == i:
                # sys.stdout.write('</ne>')
                text_out += ('</ne>')
                openEntities.pop()
            if (i + 1 == len(tokens)):
                # sys.stdout.write("</sentence>")
                text_out += ("</sentence>")
            t = tokens[i].start + tokens[i].length
    text_out += '</text>'
    # Write rest of the text
    # sys.stdout.write(encode_entities(text[t:]))
    # print(text_out + "   <- TEXT OUT")
    insert_stm = 'INSERT INTO {table}_name_tag (name_tag, record_id) VALUES (%s, %s)'.format(table=table)
    cur.execute(insert_stm, (text_out, row_id))
    if idx % 1000 == 0:
        conn.commit()
conn.commit()
cur.close()

print("time elapsed: {:.2f}s".format(time.time() - start_time))
