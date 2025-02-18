import requests
from bs4 import BeautifulSoup
import pandas as pd

get_url    = "https://sterz.stmk.gv.at/at.gv.stmk.capp/cms/cfvs/process.do?app=BET_Gem&embed=new-link&output=1&width=700&height=500&ip01=JJ&ip02=JJ&ip03=JJ&ip04=JJ&ip04def=02"
post_url   = "https://sterz.stmk.gv.at/at.gv.stmk.capp/cms/cfvs/search.do"
FROM_YEAR     = "2022"
UNTIL_YEAR    = "2024"
SAISON_WINTER = "02"
SAISON_SUMMER = "08"

# -------------------------------

headers = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded",
}

municipality_map = {
    "61253": "Admont",
    "62138": "Aflenz",
    "61254": "Aich",
    "61203": "Aigen im Ennstal",
    "61701": "Albersdorf-Prebuch",
    "61001": "Allerheiligen bei Wildon",
    "61204": "Altaussee",
    "61205": "Altenmarkt bei Sankt Gallen",
    "61756": "Anger",
    "61206": "Ardning",
    "61002": "Arnfels",
    "61207": "Bad Aussee",
    "62202": "Bad Blumau",
    "62375": "Bad Gleichenberg",
    "62273": "Bad Loipersdorf",
    "61255": "Bad Mitterndorf",
    "62376": "Bad Radkersburg",
    "60349": "Bad Schwanberg",
    "62264": "Bad Waltersdorf",
    "61626": "Bärnbach",
    "61757": "Birkfeld",
    "62105": "Breitenau am Hochlantsch",
    "62139": "Bruck an der Mur",
    "62205": "Buch-St. Magdalena",
    "62206": "Burgau",
    "62265": "Dechantskirchen",
    "62377": "Deutsch Goritz",
    "60659": "Deutschfeistritz",
    "60344": "Deutschlandsberg",
    "60660": "Dobl-Zwaring",
    "62209": "Ebersdorf",
    "62311": "Edelsbach bei Feldbach",
    "61627": "Edelschrott",
    "60661": "Eggersdorf bei Graz",
    "61049": "Ehrenhausen an der Weinstraße",
    "60345": "Eibiswald",
    "62314": "Eichkögl",
    "61101": "Eisenerz",
    "61007": "Empersdorf",
    "62378": "Fehring",
    "62266": "Feistritztal",
    "62379": "Feldbach",
    "60608": "Feldkirchen bei Graz",
    "60662": "Fernitz-Mellach",
    "61708": "Fischbach",
    "61758": "Fladnitz an der Teichalm",
    "61710": "Floing",
    "62007": "Fohnsdorf",
    "60305": "Frauental an der Laßnitz",
    "62211": "Friedberg",
    "60663": "Frohnleiten",
    "62280": "Fürstenfeld",
    "62008": "Gaal",
    "61008": "Gabersdorf",
    "61256": "Gaishorn am See",
    "61050": "Gamlitz",
    "61711": "Gasen",
    "61628": "Geistthal-Södingberg",
    "61759": "Gersdorf an der Feistritz",
    "61051": "Gleinstätten",
    "61760": "Gleisdorf",
    "62380": "Gnas",
    "60611": "Gössendorf",
    "62268": "Grafendorf bei Hartberg",
    "61012": "Gralla",
    "60613": "Gratkorn",
    "60664": "Gratwein-Straßengel",
    "60101": "Graz",
    "62214": "Greinbach",
    "61213": "Gröbming",
    "60346": "Groß Sankt Florian",
    "61013": "Großklein",
    "62216": "Großsteinbach",
    "62269": "Großwilfersdorf",
    "61215": "Grundlsee",
    "61761": "Gutenberg",
    "62326": "Halbenrain",
    "60617": "Hart bei Graz",
    "62219": "Hartberg",
    "62220": "Hartberg Umgebung",
    "62270": "Hartl",
    "60618": "Haselsdorf-Tobelbad",
    "61217": "Haus",
    "60619": "Hausmannstätten",
    "61052": "Heiligenkreuz am Waasen",
    "61016": "Heimschuh",
    "61017": "Hengsberg",
    "61629": "Hirschegg-Pack",
    "60665": "Hitzendorf",
    "61719": "Hofstätten an der Raab",
    "62010": "Hohentauern",
    "62271": "Ilz",
    "61762": "Ilztal",
    "61257": "Irdning-Donnersbachtal",
    "62330": "Jagerberg",
    "62040": "Judenburg",
    "61630": "Kainach bei Voitsberg",
    "60623": "Kainbach bei Graz",
    "62272": "Kaindorf",
    "60624": "Kalsdorf bei Graz",
    "61105": "Kalwang",
    "61106": "Kammern im Liesingtal",
    "62140": "Kapfenberg",
    "62332": "Kapfenstein",
    "62141": "Kindberg",
    "62381": "Kirchbach-Zerlach",
    "62382": "Kirchberg an der Raab",
    "61019": "Kitzeck im Sausal",
    "62335": "Klöch",
    "62041": "Knittelfeld",
    "62014": "Kobenz",
    "61631": "Köflach",
    "61437": "Krakau",
    "61107": "Kraubath an der Mur",
    "62115": "Krieglach",
    "61611": "Krottendorf-Gaisfeld",
    "60626": "Kumberg",
    "62226": "Lafnitz",
    "61258": "Landl",
    "61020": "Lang",
    "62116": "Langenwang",
    "60318": "Lannach",
    "60628": "Laßnitzhöhe",
    "61222": "Lassing",
    "61021": "Lebring-Sankt Margarethen",
    "61053": "Leibnitz",
    "61108": "Leoben",
    "61054": "Leutschach an der Weinstraße",
    "60629": "Lieboch",
    "61259": "Liezen",
    "61612": "Ligist",
    "62039": "Lobmingtal",
}

session = requests.session()

def getKey():
    key = ""
    response = session.post(get_url, headers=headers)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")

        input_field = soup.find("input", {"name": "key"})
        if input_field:
            key = input_field.get("value", "")
            print(key)
            return key
        else:
            print("Input field with name='key' not found")
            return None

def getData(gemeinde, fromYear, untilYear, season):
    payload = {
        "listvalues[0]": gemeinde,
        "listvalues[1]": fromYear,
        "listvalues[2]": untilYear,
        "listvalues[3]": season,
        "key": getKey()
    }

    response = session.post(post_url, data=payload, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")

        tables = soup.find_all("table", class_="ergebnis")
        data_list = {}

        if len(tables) != 2:
            print(f"Error: Unexpected response for Gemeinde {municipality_map.get(gemeinde)}")
            data_list.update({"Gemeinde": municipality_map.get(gemeinde)})
            return data_list

        for table in tables:
            cols = table.find_all("th")
            rows = table.find_all("td")

            for i in range(len(cols)):
                data_list.update({cols[i].text.strip(): rows[i].text.strip()})

        return data_list

    else:
        print(f"Failed request with status code {response.status_code} for Gemeinde {municipality_map.get(gemeinde)}")
        data_list.update({"Gemeinde": municipality_map.get(gemeinde)})
    
    return data_list

data_list = []

for gem_key, gem_name in municipality_map.items():
    data_list.append(getData(gem_key, FROM_YEAR, UNTIL_YEAR, SAISON_WINTER))
    data_list.append(getData(gem_key, FROM_YEAR, UNTIL_YEAR, SAISON_SUMMER))

df = pd.DataFrame(data_list)
df.to_csv("output.csv", index=False, header=False)
print("Data extraction complete. Saved as 'output.csv'.")
