import pandas as pd
import requests
import time
from sqlalchemy import create_engine 
import json
import ast




def ImportMatch(donnees) :
    user = donnees["USER"]
    password = donnees["PASSWORD"]
    host = 'localhost'
    port = '5432'
    database = 'Champ_selectbdd'

    # Chaîne de connexion
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

    table_name = 'Matchs'

    # Lecture de la table dans un DataFrame
    matchs = pd.read_sql(table_name, engine)
    engine.dispose()
    matchs['bans'] = matchs['bans'].apply(lambda x: ast.literal_eval(x))
    return matchs



def database_graph(matches,donnees):
    USER = donnees["USER"]
    PASSWORD = donnees["PASSWORD"]
    HOST = "localhost"  # ou une adresse IP distante
    PORT = "5432"
    DB_NAME = "Champ_selectbdd"

    # Créer une connexion SQLAlchemy
    engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")
    try:
        # Essayer de se connecter
        with engine.connect() as connection:
            print("✅ Connexion réussie à la base de données via SQLAlchemy !")
    except Exception as e:
        print(f"❌ Erreur de connexion : {e}")

    
    graph=matches[["matchId","bans","pickOrder","elo"]]
    graph["bans"]=graph["bans"].astype(str)
    graph["pickOrder"]=graph["pickOrder"].astype(str)
    matches.to_sql("Matchs_graph", engine, if_exists="append", index=False)


def champ(matchs):
    champion = matchs.groupby('championId')['lane'].apply(lambda x: x.value_counts().idxmax()).reset_index(name='lane')
    map_lane ={"TOP" : 0, "JUNGLE" :1,"MIDDLE":2,"BOTTOM":3}
    champion["lane"]=champion["lane"].map(map_lane)
    return champion

def PickOrder(matchs,champion):
    mapped = champion.set_index('championId')['lane'].to_dict()
    matchs["pickOrder"]=matchs["bans"].apply(lambda id_list: [mapped.get(id_val) for id_val in id_list])
    return matchs

with open('../server.json', 'r', encoding='utf-8') as fichier :
    donnees = json.load(fichier)

matchs = ImportMatch(donnees) 
champion = champ(matchs)
matchs = PickOrder(matchs,champion)
database_graph(matchs.groupby("matchId").first(),donnees)