import sqlite3
import pandas as pd
import pymorphy2
morph = pymorphy2.MorphAnalyzer()
from tqdm.auto import tqdm
import json
import re
con = sqlite3.connect('telegram.db')
c = con.cursor()
con.commit()


#функция для лемматизации токена через PyMorphy
def lemmatization(token):
    p = morph.parse(token)[0]
    return p.normal_form


#функция, которая, получая на вход токен, ищет точные совпадения
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


#функция, которая, получая на вход токен, лемматизирует его, затем смотрит, какие у этой леммы есть токены, и выводит те посты, в которых эти токены есть
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


#функция, которая, получая на вход токен, смотрит, какие у него есть синонимы, и выводит те посты, в которых эти синонимы есть
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


#функция, которая выполняет поиск и сохраняет результат для одного токена
def search_one(qua: str):
    q = qua.lower()
    post_texts, channel_names = get_posts_for_token(q)  
    a, b = search_lemma(q)
    post_texts.extend(a)
    channel_names.extend(b)
    c, d = search_synonyms(q)
    post_texts.extend(c)
    channel_names.extend(d)
    if len(post_texts) == 0:
        with open('posts.json', 'w', encoding='utf-8') as file:
            json.dump('No posts yet...', file)
    else: 
        df_output = pd.DataFrame()
        df_output['post'] = post_texts
        df_output['channel'] = channel_names
        df_output = df_output.drop_duplicates()
        output = df_output.to_dict('records')
        with open('posts.json', 'w', encoding='utf-8') as file:
            json.dump(output, file)


#функция, которая ищет точные вхождения более, чем одного токена в тексты (для этого был специально создан столбец text_clear, в котором тексты очищены от пунктуации и приведены к нижнему регистру)
def search_many_tokens(qua):
    channel_ids = []
    post_texts = []
    for row in c.execute("""SELECT text, id_channel, text_clear FROM posts"""):
        if qua in row[-1]:
            post_texts.append(row[-3])
            channel_ids.append(row[-2])
 
    channel_names = []
    for id_c in channel_ids:
        z = (int(id_c), )
        for row in c.execute("""SELECT channels.name FROM channels
            WHERE channels_id=?""", z):
            channel_names.append(row[-1])
    return post_texts, channel_names


#функция, выполняющая поиск для любого запроса
def search(qua):
    qua = str(qua).lower()
    qua_l = qua.split()
    if len(qua)==0:
        with open('posts.json', 'w', encoding='utf-8') as file:
            json.dump('Your query is empty :(', file)
    elif len(qua)==1:
        search_one(qua)
    else:
        search_many(qua)
