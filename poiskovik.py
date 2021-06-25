import sqlite3
import pandas as pd
import pymorphy2
morph = pymorphy2.MorphAnalyzer()
import json


#подключаемся к бд
con = sqlite3.connect('telegram1.db')
c = con.cursor()
con.commit()


#функция лемматизации
def lemmatization(token):
    p = morph.parse(token)[0]
    return p.normal_form


#поиск точной формы
def get_posts_for_token(qua):
    x = (qua, )
    
    list_id_posts = []
    for row in c.execute("""SELECT list_posts_id FROM tokens
                            WHERE token=?""", x):
        list_id_posts.extend(row[-1].replace('[', '').replace(']', '').replace(' ', '').split(','))
 
    channel_names = []
    post_texts = []
    for id_p in list_id_posts:
        z = (int(id_p), )
        for row in c.execute("""SELECT channels.name, posts.text FROM posts JOIN channels
            ON posts.id_channel = channels.id_channel WHERE id_post=?""", z):
            channel_names.append(row[-2])
            post_texts.append(row[-1])
    return post_texts, channel_names


#поиск всех форм, кроме поданной на поиск, по лемме
def search_lemma(q):
    x = (q, )
    for row in c.execute("""SELECT id_token FROM tokens
        WHERE token=?""", x):
        i = str(row[-1])
    le = (lemmatization(q), )
    for row in c.execute("""SELECT list_tokens FROM lemmas
        WHERE lemma=?""", le):
        tokens_id = row[-1].replace('[', '').replace(']', '').replace(' ', '').split(',')
        tokens_id.remove(i)

    list_tokens = []
    for j in tokens_id:
        y = (j, )
        for row in c.execute("""SELECT token FROM tokens
                                WHERE id_token=?""", y):
            list_tokens.append(row[-1])

    post_texts = []
    channel_names = []
    for t in list_tokens:
        a, b = get_posts_for_token(t)
        post_texts.extend(a)
        channel_names.extend(b)
    return post_texts, channel_names


#поиск по всем формам синонимов
def search_synonyms(q):
    x = (q, )
    for row in c.execute("""SELECT list_synonyms FROM tokens
        WHERE token=?""", x):
        synonyms_id = row[-1]

    list_synonyms = []
    for j in synonyms_id:
        y = (j, )
        for row in c.execute("""SELECT token FROM tokens
                                WHERE id_token=?""", y):
            list_synonyms.append(row[-1])

    post_texts = []
    channel_names = []
    for s in list_synonyms:
        a, b = search_lemma(s)
        post_texts.extend(a)
        channel_names.extend(b)
    return post_texts, channel_names


#главная функция поиска
def search(qua: str):
    q = qua.lower()
    post_texts, channel_names = get_posts_for_token(q)
    if len(post_texts) == 0:
        with open('posts.json', 'a', encoding='utf-8') as file:
            json.dump('No posts yet...', file)
    else:    
        a, b = search_lemma(q)
        post_texts.extend(a)
        channel_names.extend(b)
        c, d = search_synonyms(q)
        post_texts.extend(c)
        channel_names.extend(d)
        df_output = pd.DataFrame()
        df_output['post'] = post_texts
        df_output['channel'] = channel_names
        output = df_output.to_dict('records')
        with open('posts.json', 'w', encoding='utf-8') as file:
            json.dump(output, file)

