import pandas as pd
import requests
import time
from sqlalchemy import create_engine 
import json
import ast
from sklearn.preprocessing import MinMaxScaler




def get_matchs(donnees):
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


def JoueurDef(matchs):
    jcj=matchs.merge(matchs[["matchId","championId","lane","teamId"]],on="matchId")
    jcj=jcj[jcj["championId_x"]!=jcj["championId_y"]]
    jcj["enemy"]=jcj["teamId_x"]!=jcj["teamId_y"]
    jcj["direct"]=jcj["lane_x"]==jcj["lane_y"]
    return jcj


def Similarity(jcj):
    feature = ["kda", "killParticipation", "goldPerMinute","damageDealtToObjectives","damageDealtToBuildings","timeCCingOthers","totalDamageShieldedOnTeammates","totalHeal","goldSpent","goldEarned","killingSprees","totalMinionsKilled","damagePerMinute","soloKills","skillshotsDodged","skillshotsHit"]
    test = jcj.groupby(["championId_x","championId_y","elo","direct","enemy"])[feature].agg(["std","mean"])
    test=test.dropna()
    #test[["skillshotsHit","damageDealtToObjectives","timeCCingOthers","damageDealtToBuildings","totalHeal","killingSprees","soloKills"]]= test[["skillshotsHit","damageDealtToObjectives","timeCCingOthers","damageDealtToBuildings","totalHeal","killingSprees","soloKills"]].fillna(0)
    test = test.replace([float("inf"), float("-inf")], float("nan"))

    test["influence"]= test.xs('std', axis=1, level=1).mean(axis=1)
    test["impact"]= test.xs('mean', axis=1, level=1).mean(axis=1)


    normalized_f=["championId_x","championId_y","elo","direct","enemy","influence",'impact']
    test.columns=test.columns.droplevel(1)
    test=test.dropna().reset_index()
    test= test.rename(str,axis="columns") 
    df_normalized = test[normalized_f].copy()  

    # On normalise la similarité mais que pour les champion (on cherche à avoir un chiffre pour évaluer ses match up par rapport aux autres)
    for champ in test["championId_x"].unique() : 
        for d in test[test["championId_x"]==champ]["direct"].unique():
            for e in  test[(test["championId_x"]==champ)&(test["direct"]==d)]["enemy"].unique():
                col="influence"
                df_normalized.loc[(df_normalized["championId_x"]==champ)&(df_normalized["direct"]==d)&(df_normalized["enemy"]==e),col]  = (test[(test["championId_x"]==champ)&(test["direct"]==d)&(test["enemy"]==e)][col] - test[(test["championId_x"]==champ)&(test["direct"]==d)&(test["enemy"]==e)][col].min()) / (test[(test["championId_x"]==champ)&(test["direct"]==d)&(test["enemy"]==e)][col].max() - test[(test["championId_x"]==champ)&(test["direct"]==d)&(test["enemy"]==e)][col].min())
                col="impact"
                df_normalized.loc[(df_normalized["championId_x"]==champ)&(df_normalized["direct"]==d)&(df_normalized["enemy"]==e),col]  = (test[(test["championId_x"]==champ)&(test["direct"]==d)&(test["enemy"]==e)][col] - test[(test["championId_x"]==champ)&(test["direct"]==d)&(test["enemy"]==e)][col].min()) / (test[(test["championId_x"]==champ)&(test["direct"]==d)&(test["enemy"]==e)][col].max() - test[(test["championId_x"]==champ)&(test["direct"]==d)&(test["enemy"]==e)][col].min())
    return df_normalized


def intodb(matches,donnees):
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

    matches.to_sql("MatchUp", engine, if_exists="append", index=False)


with open('../server.json', 'r', encoding='utf-8') as fichier :
    donnees = json.load(fichier)


matchs=get_matchs(donnees)
jcj=JoueurDef(matchs)
similarity=Similarity(jcj)
intodb(similarity,donnees)