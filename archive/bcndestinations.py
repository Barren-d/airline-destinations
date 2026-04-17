import bs4
import datetime
import pandas
import re
import requests

# get html from aena webpage
res = requests.get('https://www.aena.es/es/josep-tarradellas-barcelona-el-prat/'+\
 	               'aerolineas-y-destinos/destinos-aeropuerto.html')
# parse text 
soup = bs4.BeautifulSoup(res.text, 'html.parser')

# return matching elements from article class 
elems = soup.select(r'article.fila.resultado.regular.filtered')

counter = 0
text_list = []
# extract all text elements from article class to list
for elem in elems[0:]:
	text_list.append(str(counter) +\
	                 '\nBARCELONA-EL PRAT JOSEP TARRADELLAS (BCN)' +\
	                 elem.text)	
	counter +=1

route_list = []
for item in text_list:
	route = re.split('\n',item)
	route = list(filter(None, route))
	route_list.append(route)

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
# get todays date
date = datetime.datetime.now().strftime('%Y-%m-%d')
# save results as csv
route_df.to_csv('data/bcn_destinations_' + str(date) + '.csv')
