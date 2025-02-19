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
    '61253': 'Admont', '62138': 'Aflenz', '61254': 'Aich', '61203': 'Aigen im Ennstal', '61701': 'Albersdorf-Prebuch', '61001': 'Allerheiligen bei Wildon', '61204': 'Altaussee', '61205': 'Altenmarkt bei Sankt Gallen', '61756': 'Anger', '61206': 'Ardning', '61002': 'Arnfels', '61207': 'Bad Aussee', '62202': 'Bad Blumau', '62375': 'Bad Gleichenberg', '62273': 'Bad Loipersdorf', '61255': 'Bad Mitterndorf', '62376': 'Bad Radkersburg', '60349': 'Bad Schwanberg', '62264': 'Bad Waltersdorf', '61626': 'Bärnbach', '61757': 'Birkfeld', '62105': 'Breitenau am Hochlantsch', '62139': 'Bruck an der Mur', '62205': 'Buch-St. Magdalena', '62206': 'Burgau', '62265': 'Dechantskirchen', '62377': 'Deutsch Goritz', '60659': 'Deutschfeistritz', '60344': 'Deutschlandsberg', '60660': 'Dobl-Zwaring', '62209': 'Ebersdorf', '62311': 'Edelsbach bei Feldbach', '61627': 'Edelschrott', '60661': 'Eggersdorf bei Graz', '61049': 'Ehrenhausen an der Weinstraße', '60345': 'Eibiswald', '62314': 'Eichkögl', '61101': 'Eisenerz', '61007': 'Empersdorf', '62378': 'Fehring', '62266': 'Feistritztal', '62379': 'Feldbach', '60608': 'Feldkirchen bei Graz', '60662': 'Fernitz-Mellach', '61708': 'Fischbach', '61758': 'Fladnitz an der Teichalm', '61710': 'Floing', '62007': 'Fohnsdorf', '60305': 'Frauental an der Laßnitz', '62211': 'Friedberg', '60663': 'Frohnleiten', '62280': 'Fürstenfeld', '62008': 'Gaal', '61008': 'Gabersdorf', '61256': 'Gaishorn am See', '61050': 'Gamlitz', '61711': 'Gasen', '61628': 'Geistthal-Södingberg', '61759': 'Gersdorf an der Feistritz', '61051': 'Gleinstätten', '61760': 'Gleisdorf', '62380': 'Gnas', '60611': 'Gössendorf', '62268': 'Grafendorf bei Hartberg', '61012': 'Gralla', '60613': 'Gratkorn', '60664': 'Gratwein-Straßengel', '60101': 'Graz', '62214': 'Greinbach', '61213': 'Gröbming', '60346': 'Groß Sankt Florian', '61013': 'Großklein', '62216': 'Großsteinbach', '62269': 'Großwilfersdorf', '61215': 'Grundlsee', '61761': 'Gutenberg', '62326': 'Halbenrain', '60617': 'Hart bei Graz', '62219': 'Hartberg', '62220': 'Hartberg Umgebung', '62270': 'Hartl', '60618': 'Haselsdorf-Tobelbad', '61217': 'Haus', '60619': 'Hausmannstätten', '61052': 'Heiligenkreuz am Waasen', '61016': 'Heimschuh', '61017': 'Hengsberg', '61629': 'Hirschegg-Pack', '60665': 'Hitzendorf', '61719': 'Hofstätten an der Raab', '62010': 'Hohentauern', '62271': 'Ilz', '61762': 'Ilztal', '61257': 'Irdning-Donnersbachtal', '62330': 'Jagerberg', '62040': 'Judenburg', '61630': 'Kainach bei Voitsberg', '60623': 'Kainbach bei Graz', '62272': 'Kaindorf', '60624': 'Kalsdorf bei Graz', '61105': 'Kalwang', '61106': 'Kammern im Liesingtal', '62140': 'Kapfenberg', '62332': 'Kapfenstein', '62141': 'Kindberg', '62381': 'Kirchbach-Zerlach', '62382': 'Kirchberg an der Raab', '61019': 'Kitzeck im Sausal', '62335': 'Klöch', '62041': 'Knittelfeld', '62014': 'Kobenz', '61631': 'Köflach', '61437': 'Krakau', '61107': 'Kraubath an der Mur', '62115': 'Krieglach', '61611': 'Krottendorf-Gaisfeld', '60626': 'Kumberg', '62226': 'Lafnitz', '61258': 'Landl', '61020': 'Lang', '62116': 'Langenwang', '60318': 'Lannach', '60628': 'Laßnitzhöhe', '61222': 'Lassing', '61021': 'Lebring-Sankt Margarethen', '61053': 'Leibnitz', '61108': 'Leoben', '61054': 'Leutschach an der Weinstraße', '60629': 'Lieboch', '61259': 'Liezen', '61612': 'Ligist', '62039': 'Lobmingtal', '61727': 'Ludersdorf-Wilfersdorf', '61632': 'Maria Lankowitz', '62142': 'Mariazell', '61716': 'Markt Hartmannsdorf', '61109': 'Mautern in Steiermark', '62343': 'Mettersdorf am Saßbach', '61260': 'Michaelerberg-Pruggern', '61728': 'Miesenbach bei Birkfeld', '61261': 'Mitterberg-Sankt Martin', '61729': 'Mitterdorf an der Raab', '61615': 'Mooskirchen', '61730': 'Mortantsch', '61410': 'Mühlen', '61438': 'Murau', '62383': 'Mureck', '62143': 'Mürzzuschlag', '61731': 'Naas', '60666': 'Nestelbach bei Graz', '62144': 'Neuberg an der Mürz', '62274': 'Neudau', '61439': 'Neumarkt in der Steiermark', '61413': 'Niederwölz', '61110': 'Niklasdorf', '62042': 'Obdach', '61024': 'Oberhaag', '61440': 'Oberwölz', '61262': 'Öblarn', '62232': 'Ottendorf an der Rittschein', '62384': 'Paldau', '61763': 'Passail', '60632': 'Peggau', '62125': 'Pernegg an der Mur', '62233': 'Pinggau', '62385': 'Pirching am Traubenberg', '61764': 'Pischelsdorf am Kulm', '60323': 'Pölfing-Brunn', '62275': 'Pöllau', '62235': 'Pöllauberg', '62043': 'Pöls-Oberkurzheim', '62044': 'Pölstal', '60324': 'Preding', '60670': 'Premstätten', '61111': 'Proleb', '61740': 'Puch bei Weiz', '62021': 'Pusterwald', '60667': 'Raaba-Grambach', '61112': 'Radmer', '61027': 'Ragnitz', '61236': 'Ramsau am Dachstein', '61441': 'Ranten', '61741': 'Ratten', '61743': 'Rettenegg', '62386': 'Riegersburg', '62276': 'Rohr bei Hartberg', '62277': 'Rohrbach an der Lafnitz', '61618': 'Rosental an der Kainach', '61263': 'Rottenmann', '61030': 'Sankt Andrä-Höch', '62387': 'Sankt Anna am Aigen', '62145': 'Sankt Barbara im Mürztal', '60639': 'Sankt Bartholomä', '61264': 'Sankt Gallen', '61442': 'Sankt Georgen am Kreischberg', '61055': 'Sankt Georgen an der Stiefing', '62026': 'Sankt Georgen ob Judenburg', '62242': 'Sankt Jakob im Walde', '61032': 'Sankt Johann im Saggautal', '62244': 'Sankt Johann in der Haide', '60326': 'Sankt Josef (Weststeiermark)', '61745': 'Sankt Kathrein am Offenegg', '61443': 'Sankt Lambrecht', '62245': 'Sankt Lorenzen am Wechsel', '62128': 'Sankt Lorenzen im Mürztal', '60668': 'Sankt Marein bei Graz', '62146': 'Sankt Marein im Mürztal', '62045': 'Sankt Marein-Feistritz', '62046': 'Sankt Margarethen bei Knittelfeld', '61621': 'Sankt Martin am Wöllmißberg', '60347': 'Sankt Martin im Sulmtal', '61113': 'Sankt Michael in Obersteiermark', '61033': 'Sankt Nikolai im Sausal', '60641': 'Sankt Oswald bei Plankenwarth', '62388': 'Sankt Peter am Ottersbach', '60329': 'Sankt Peter im Sulmtal', '62032': 'Sankt Peter ob Judenburg', '61114': 'Sankt Peter-Freienstein', '60642': 'Sankt Radegund bei Graz', '61765': 'Sankt Ruprecht an der Raab', '62389': 'Sankt Stefan im Rosental', '61115': 'Sankt Stefan ob Leoben', '60348': 'Sankt Stefan ob Stainz', '61060': 'Sankt Veit in der Südsteiermark', '62247': 'Schäffern', '61444': 'Scheifling', '61265': 'Schladming', '61428': 'Schöder', '61057': 'Schwarzautal', '62034': 'Seckau', '60669': 'Seiersberg-Pirka', '61243': 'Selzthal', '60645': 'Semriach', '61748': 'Sinabelkirchen', '61633': 'Söding-Sankt Johann', '61266': 'Sölk', '62047': 'Spielberg', '62131': 'Spital am Semmering', '61744': 'St. Kathrein am Hauenstein', '61746': 'St. Margarethen an der Raab', '61425': 'St. Peter am Kammersberg', '61445': 'Stadl-Predlitz', '61267': 'Stainach-Pürgg', '60350': 'Stainz', '61624': 'Stallhofen', '62132': 'Stanz im Mürztal', '60646': 'Stattegg', '60647': 'Stiwoll', '62390': 'Straden', '61750': 'Strallegg', '61061': 'Straß in Steiermark', '62256': 'Stubenberg', '61446': 'Teufenbach-Katsch', '60648': 'Thal', '61751': 'Thannhausen', '62147': 'Thörl', '62368': 'Tieschen', '61043': 'Tillmitsch', '61116': 'Traboch', '62148': 'Tragöß-Sankt Katharein', '61247': 'Trieben', '61120': 'Trofaiach', '62135': 'Turnau', '60651': 'Übelbach', '62372': 'Unterlamm', '62036': 'Unzmarkt-Frauenburg', '60653': 'Vasoldsberg', '61625': 'Voitsberg', '62278': 'Vorau', '61118': 'Vordernberg', '61045': 'Wagna', '61119': 'Wald am Schoberpaß', '62279': 'Waldbach-Mönichwald', '60654': 'Weinitzen', '62048': 'Weißkirchen in Steiermark', '61766': 'Weiz', '62262': 'Wenigzell', '60655': 'Werndorf', '60341': 'Wettmannstätten', '60351': 'Wies', '61251': 'Wildalpen', '61059': 'Wildon', '61252': 'Wörschach', '60656': 'Wundschuh', '62038': 'Zeltweg'
}

test = {
    '61253': 'Admont', '62138': 'Aflenz'
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

def getData(gemeinde, fromYear, untilYear, season, includeBetten = True, includeBetriebe = True):
    gemeindeName = municipality_map.get(gemeinde)
    data_list = {}
    data_list[gemeindeName] = {}

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

        if len(tables) != 2:
            print(f"Error: Unexpected response for Gemeinde {gemeindeName}")
            return data_list
        
        if includeBetriebe:
            betriebe = {}
            cols = tables[0].find_all("th")
            rows = tables[0].find_all("td")

            for i in range(1, len(cols)):
                betriebe[cols[i].text.strip()] = rows[i].text.strip()

            data_list[gemeindeName]["betriebe"] = betriebe

        if includeBetten:
            betten = {}
            cols = tables[1].find_all("th")
            rows = tables[1].find_all("td")

            for i in range(1, len(cols)):
                betten[cols[i].text.strip()] = rows[i].text.strip()

            data_list[gemeindeName]["betten"] = betten

        return data_list

    else:
        print(f"Failed request with status code {response.status_code} for Gemeinde {municipality_map.get(gemeinde)}")
        data_list.update({"Gemeinde": municipality_map.get(gemeinde)})
    
    return data_list

def exportCSV(data, filename):
    merged_data = {}

    for entry in data:
        for gemeinde, values in entry.items():
            # Ensure the Gemeinde exists in merged_data
            if gemeinde not in merged_data:
                merged_data[gemeinde] = {}

            # Merge 'betten' and 'betriebe' data
            for category, records in values.items():
                if category not in merged_data[gemeinde]:
                    merged_data[gemeinde][category] = {}

                # Merge all year-period records
                for year, value in records.items():
                    merged_data[gemeinde][category][year] = value

    flattened_data = []

    for gemeinde, values in merged_data.items():
        row = {'Gemeinde': gemeinde}
        for category, records in values.items():
            for year, value in records.items():
                row[f"{year} ({category})"] = value
        flattened_data.append(row)

    df = pd.DataFrame(flattened_data)
    df.to_csv(filename, index=False)

    return


data_list = []
for gem_key, gem_name in municipality_map.items():
    data_list.append(getData(gem_key, FROM_YEAR, UNTIL_YEAR, SAISON_WINTER, includeBetriebe=True, includeBetten=False))
    data_list.append(getData(gem_key, FROM_YEAR, UNTIL_YEAR, SAISON_SUMMER, includeBetriebe=True, includeBetten=False))
exportCSV(data_list, "betriebe.csv")

data_list = []
for gem_key, gem_name in municipality_map.items():
    data_list.append(getData(gem_key, FROM_YEAR, UNTIL_YEAR, SAISON_WINTER, includeBetriebe=False, includeBetten=True))
    data_list.append(getData(gem_key, FROM_YEAR, UNTIL_YEAR, SAISON_SUMMER, includeBetriebe=False, includeBetten=True))
exportCSV(data_list, "betten.csv")

print("Data extraction complete.")
