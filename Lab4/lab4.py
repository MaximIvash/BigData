import requests
from bs4 import BeautifulSoup
import re
import sqlite3

# ===== 1) Список URL статей =====
URLS = [
    "https://ru.wikipedia.org/wiki/Python",
    "https://ru.wikipedia.org/wiki/Monty_Python",
    "https://ru.wikipedia.org/wiki/Data_science",
    "https://ru.wikipedia.org/wiki/Machine_learning",
    "https://ru.wikipedia.org/wiki/Artificial_intelligence",
    "https://ru.wikipedia.org/wiki/Computer_science",
    "https://ru.wikipedia.org/wiki/Information_retrieval"
]

# ===== 2) Скачивание и парсинг страниц =====
def fetch_pages(urls):
    docs = {}
    links = {}
    for url in urls:
        try:
            print(f"Fetching {url} ...")
            r = requests.get(url, headers={"User-Agent": "Python MiniSearch"})
            if r.status_code != 200:
                print(f"Failed: {url}")
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            text = soup.get_text().lower()
            words = [w for w in re.findall(r"[а-яёa-z]+", text)]
            docs[url] = words

            # собираем ссылки на другие страницы из списка URLS
            a_tags = soup.find_all("a", href=True)
            page_links = []
            for a in a_tags:
                href = a['href']
                if href.startswith("/wiki/"):
                    full_url = "https://ru.wikipedia.org" + href
                    if full_url in URLS:
                        page_links.append(full_url)
            links[url] = page_links
        except Exception as e:
            print(f"Error fetching {url}: {e}")
    return docs, links

docs, links = fetch_pages(URLS)

# ===== 3) Индексация в SQLite =====
conn = sqlite3.connect("database.db")
c = conn.cursor()

c.execute("DROP TABLE IF EXISTS documents")
c.execute("DROP TABLE IF EXISTS words")
c.execute("DROP TABLE IF EXISTS doc_words")
c.execute("DROP TABLE IF EXISTS links")

c.execute("CREATE TABLE documents (id INTEGER PRIMARY KEY, url TEXT)")
c.execute("CREATE TABLE words (id INTEGER PRIMARY KEY, word TEXT)")
c.execute("CREATE TABLE doc_words (doc_id INTEGER, word_id INTEGER)")
c.execute("CREATE TABLE links (from_doc INTEGER, to_doc INTEGER)")

word_index = {}
doc_id_map = {}

for url, words in docs.items():
    c.execute("INSERT INTO documents (url) VALUES (?)", (url,))
    doc_id = c.lastrowid
    doc_id_map[url] = doc_id
    for w in words:
        if w not in word_index:
            c.execute("INSERT INTO words (word) VALUES (?)", (w,))
            word_index[w] = c.lastrowid
        c.execute("INSERT INTO doc_words (doc_id, word_id) VALUES (?, ?)", (doc_id, word_index[w]))

for src, dsts in links.items():
    for dst in dsts:
        if dst in doc_id_map:
            c.execute("INSERT INTO links (from_doc, to_doc) VALUES (?, ?)", (doc_id_map[src], doc_id_map[dst]))

conn.commit()

# ===== 4) PageRank MapReduce с учетом dangling nodes =====
def pagerank_mapreduce(graph, d=0.85, iters=10):
    nodes = list(graph.keys())
    N = len(nodes)
    ranks = {n: 1/N for n in nodes}

    for _ in range(iters):
        new_ranks = {}
        dangling_sum = sum(ranks[n] for n in nodes if len(graph[n]) == 0)
        for n in nodes:
            rank_sum = dangling_sum / N
            for m, outs in graph.items():
                if n in outs:
                    rank_sum += ranks[m] / len(outs)
            new_ranks[n] = (1 - d)/N + d * rank_sum
        ranks = new_ranks
    return ranks

pr_map = pagerank_mapreduce(links)
print("\nPageRank MapReduce:")
for k,v in pr_map.items():
    print(f"{k}: {v:.4f}")

# ===== 5) PageRank Pregel =====
def pagerank_pregel(graph, d=0.85, iters=10):
    nodes = list(graph.keys())
    N = len(nodes)
    ranks = {n: 1/N for n in nodes}

    for _ in range(iters):
        messages = {n:0 for n in nodes}
        for node, outs in graph.items():
            if outs:
                contribution = ranks[node] / len(outs)
                for dst in outs:
                    messages[dst] += contribution
            else:
                for dst in nodes:
                    messages[dst] += ranks[node] / N

        for n in nodes:
            ranks[n] = (1 - d)/N + d * messages[n]

    return ranks

pr_pregel = pagerank_pregel(links)
print("\nPageRank Pregel:")
for k,v in pr_pregel.items():
    print(f"{k}: {v:.4f}")

# ===== 6) Полнотекстовый поиск =====
def search_daat(query):
    q_terms = query.lower().split()
    q_marks = ",".join("?"*len(q_terms))
    sql = f"""
    SELECT d.url, COUNT(*) as score
    FROM documents d
    JOIN doc_words dw ON d.id = dw.doc_id
    JOIN words w ON dw.word_id = w.id
    WHERE w.word IN ({q_marks})
    GROUP BY d.url
    ORDER BY score DESC
    """
    c.execute(sql, q_terms)
    return c.fetchall()

def search_taat(query):
    scores = {}
    for term in query.lower().split():
        c.execute("""
        SELECT d.url
        FROM documents d
        JOIN doc_words dw ON d.id = dw.doc_id
        JOIN words w ON dw.word_id = w.id
        WHERE w.word = ?
        """, (term,))
        for (url,) in c.fetchall():
            scores[url] = scores.get(url, 0) + 1
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)

query = "питон язык"
print("\nSearch DAAT:", search_daat(query))
print("Search TAAT:", search_taat(query))

conn.close()
