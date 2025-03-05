import pandas as pd
import requests
import time
from sqlalchemy import create_engine
import os
import json

def fetch(json,match,feature,feature_chall,feature_teams):
    try :
        if json["info"]['queueId']==420 :
            data=[]
            for k in range(len(json['metadata']["participants"])) :
                
                data+=[json['metadata']["matchId"]]

                data+=[[json['info']["teams"][0]["bans"][k]["championId"] for k in range(5)] + [json['info']["teams"][1]["bans"][k]["championId"] for k in range(5)]]
                if k>5:
                    for feat in feature_teams :
                        data+=[json['info']["teams"][1][feat]]

                else : 
                    for feat in feature_teams :
                        data+=[json['info']["teams"][0][feat]]

                for feat_chall in feature_chall :
                    data+=[json['info']["participants"][k]["challenges"][feat_chall]]
                    

                for feat in feature :
                    data+=[json['info']["participants"][k][feat]]

                match+=[data]
                data=[]
    except Exception as e:
        print(f"Une erreur est survenue : {e}")
    return match

def getgame(elo,api_key): 

    headers = [{
        "X-Riot-Token": api_key[0]
    },{
        "X-Riot-Token": api_key[1]
    }]
    region = 'euw1'
    request=0
    request_=0

    joueurs=[]
    for div in ["I","II","III","IV"] : 
        url = f"https://euw1.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/{elo}/{div}"
        for k in range(1,4):
            try :
                joueurs = requests.get(url+f"?page={k}", headers=headers[0]).json()
                request+=1
            except Exception as e  :
                print(f"error : {e}")
                request+=1

    
    games=[]

    for joueur in joueurs :
        if request<99 :
            try :
                game=requests.get(f'https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{joueur["puuid"]}/ids?start=0&count=15',headers=headers[0]).json()
                print(game)
                games+=game
                request+=1
            except Exception as e  :
                print(f"error : {e}")
                request+=1
        else :
            time.sleep(120)
            request=0  


    pd.DataFrame(columns=["gameId"],data=games).to_csv(f"gamesID_{elo}.csv", index=False)
    return games



def database(matches,donnees):
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

    matches.to_sql("Matchs", engine, if_exists="append", index=False)



def getmatchs(games,api_key,elo,donnees):
    headers = [{
        "X-Riot-Token": api_key[0]
    },{
        "X-Riot-Token": api_key[1]
    }]

    region = 'euw1'
    feature=['puuid','championId',"deaths","kills","damageDealtToObjectives","damageDealtToBuildings","magicDamageDealt","magicDamageTaken","physicalDamageDealt","physicalDamageTaken",
             "timeCCingOthers","totalDamageShieldedOnTeammates","totalHeal","goldSpent","goldEarned","totalEnemyJungleMinionsKilled","totalAllyJungleMinionsKilled","lane","killingSprees","totalMinionsKilled"]
    
    feature_chall=["kda","killParticipation","goldPerMinute","damagePerMinute","soloKills","skillshotsDodged","skillshotsHit","visionScorePerMinute"]
    feature_teams=["teamId","win"]

    matchs=[]

    print(len(games))
    request=0

    for x in games:
        if request<99 :
            try :
                matchs= fetch(requests.get(f'https://europe.api.riotgames.com/lol/match/v5/matches/{x}',headers=headers[0]).json(),matchs,feature,feature_chall,feature_teams)
                request+=1

            except Exception as e  :
                print(f"error : {e}")
        else :
            time.sleep(120)
            request=0  
            
    matchs=pd.DataFrame(data=matchs,columns=["matchId","bans"]+feature_teams+feature_chall+feature)
    matchs["elo"]=elo

    matchs.to_csv(f"matchesID_{elo}.csv", index=False)
    database(matchs,donnees)

    return "finished"



with open('server.json', 'r', encoding='utf-8') as fichier :
    donnees = json.load(fichier)
api_key = ['RGAPI-687c461e-3a20-4c87-a6cd-b9c50a8f2829','RGAPI-bbb6ae30-5405-4a77-9779-97c6ef6c8139']

elo = "DIAMOND"
file_path = f"gamesID_{elo}.csv"

if os.path.exists(file_path) :
    games = pd.read_csv(file_path)["gameId"].values.tolist()
else :
    games = getgame(elo,api_key)
    time.sleep(120)


getmatchs(games,api_key,elo,donnees)
