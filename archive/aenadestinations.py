import bs4
import datetime
import pandas
import re
import requests

# source url for initial scrape
source_url = 'https://www.aena.es/es/josep-tarradellas-barcelona-el-prat/'+\
              'aerolineas-y-destinos/destinos-aeropuerto.html'

def main():
    # gets a list of airport urls from aena website
    airport_urls = get_airport_urls()
    # gets a list of all airports and all destinations
    airport_destinations = get_airport_destinations(airport_urls)
    # converts destinations list to dataframe
    airport_destinations_df = list_to_dataframe(airport_destinations)
    # get todays date
    date = datetime.datetime.now().strftime('%Y-%m-%d')
    # save results as csv
    airport_destinations_df.to_csv('data/aena_destinations_' + str(date) + '.csv')

# returns a list of airport urls from source url
def get_airport_urls():
    # get html from source url
    res = requests.get(source_url)
    # parse source url text 
    soup = bs4.BeautifulSoup(res.text, 'html.parser')
    # return matching elements from article class 
    elems = soup.select(r'li.visible')
    # set variables for loop
    counter = 0
    url_list = []
    # extract all element text and hrefs to list
    for elem in elems[0:]:
        # append elements to list and modify url
        url_list.append([str(counter),
                         elem.a.text,
                          'https://aena.es' +\
                           elem.a.get('href')\
                           .replace('.html','/aerolineas-y-destinos/destinos-aeropuerto.html')])
        counter +=1
    return url_list

# returns a list of airport destinations from airport urls
def get_airport_destinations(airport_urls):
    # cycle through each airport url
    route_list = []
    for url in airport_urls:
        # check if airport url in remediation dict
        remediationurl = url_remediation(url[1])
        # if url is None, use preset url
        if remediationurl is None:
            print (url[2])
            # get html from individual aena webpage
            res = requests.get(url[2])
        # if remediation url exists, use remediation url            
        else:
            print (remediationurl)
            # get html from individual aena webpage
            res = requests.get(remediationurl)            
        # parse text
        soup = bs4.BeautifulSoup(res.text, 'html.parser')
        # return matching elements from article class 
        elems = soup.select(r'article.fila.resultado.regular.filtered')
        # set variables for loop extract
        counter = 0
        text_list = []
        # extract airport name and all text elements from article class to list
        for elem in elems[0:]:
            text_list.append(str(counter) +\
                             '\n' + url[1] +\
                             elem.text) 
            counter +=1
        if len(text_list) == 0:
            print ('   0 results found for ' + str(url[1]) + ' check url.')
        # set variable for clean up loop
        #loop through and clean up destinations list
        for item in text_list:
            route = re.split('\n',item)
            route = list(filter(None, route))
            route_list.append(route)
    return route_list

# mapping function for non-uniform urls
def url_remediation(destination):
    # unfortunately not all urls are uniform, thus
    dict={
          'Almería (LEI)': 'https://www.aena.es/es/almeria/aerolineas-y-destinos/destinos-del-aeropuerto-de-almeria.html',
          'Asturias (OVD)': 'https://www.aena.es/es/asturias/aerolineas-y-destinos/destinos-del-aeropuerto.html',
          'Badajoz (BJZ)': 'https://www.aena.es/es/badajoz/aerolineas-y-destinos/destinos-del-aeropuerto.html',
          'César Manrique-Lanzarote (ACE)': 'https://www.aena.es/es/cesar-manrique-lanzarote/aerolineas-y-destinos/destinos-del-aeropuerto.html',
          'El Hierro (VDE)': 'https://www.aena.es/es/el-hierro/aerolineas-y-destinos/destinos-del-aeropuerto.html',
          'Federico García Lorca Granada-Jaén (GRX)': 'https://www.aena.es/es/f.g.l.-granada-jaen/aerolineas-y-destinos/destinos.html',
          'Fuerteventura (FUE)': 'https://www.aena.es/es/fuerteventura/aerolineas-y-destinos-fue/destinos-aeropuerto-fue.html',
          'Gran Canaria (LPA)': 'https://www.aena.es/es/gran-canaria/aerolineas-y-destinos/destinos-del-aeropuerto-gran-canaria.html',
          'Internacional Región de Murcia (RMU)': 'https://www.aena.es/es/internacional-region-de-murcia/aerolineas-y-destinos/destinos-del-aeropuerto.html',
          'Jerez (XRY)': 'https://www.aena.es/es/jerez/aerolineas-y-destinos/.html',
          'Menorca (MAH)': 'https://www.aena.es/es/destinos-del-aeropuerto-menorca.html',
          'Pamplona (PNA)': 'https://www.aena.es/es/pamplona/aerolineas-y-destinos/destinos-del-aeropuerto.html',
          'Tenerife Norte-Ciudad de La Laguna (TFN)': 'https://www.aena.es/es/tenerife-norte-ciudad-de-la-laguna/aerolineas-destinos/destinos-del-aeropuerto.html',
          'Valencia (VLC)': 'https://www.aena.es/es/valencia/aerolineas-destinos/destinos-aeropuerto.html',
          'Valladolid (VLL)': 'https://www.aena.es/es/valladolid/aerolineas-y-destinos/destinos-del-aeropuerto.html',
          'Vitoria (VIT)': 'https://www.aena.es/es/vitoria/aerolineas-y-destinos/destinos-del-aeropuerto.html'
          }
    return dict.get(destination)

# converts destinations list to dataframe
def list_to_dataframe(route_list):
    # create the dataframe
    route_df = pandas.DataFrame(route_list, columns = ['Result',
                                                   'Origin',
                                                   'Destination',
                                                   '',
                                                   'DestinationCountry',
                                                   '',
                                                   'Airlines'])
    # return only the relevant columns
    route_df = route_df[['Result',
                        'Origin',
                        'Destination',
                        'DestinationCountry',
                        'Airlines']]
    return route_df

main()
# for later:
# coordinates sourced from:
# https://openflights.org/data.html

# source:
# https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat