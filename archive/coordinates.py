import pandas
import re

def get_coordinates():
	data = pandas.read_table(r'data/airports_2022-08-03.dat',
		                     sep=',',
		                     names=['AirportID',
		                              'Name',
		                              'City',
		                              'Country',
		                              'IATA',
		                              'ICAO',
		                              'Latitude',
		                              'Longitude',
		                              'Altitude',
		                              'Timezone',
		                              'DST',
		                              'TzDatabase',
		                              'Type',	
		                              'Source'])
	return data[['Name','IATA','Latitude','Longitude']]

def apply_IATA_regex(data):
	IATA_regex = re.findall('\((.*)\)',data)
	if len(IATA_regex) == 1:
		return IATA_regex[0]
	else:
		return IATA_regex

def extract_IATA():
	data = pandas.read_csv(r'data/bcn_destinations_2022-07-31.csv')
	data['OriginIATA'] = data['Origin'].apply(apply_IATA_regex)
	data['DestinationIATA'] = data['Destination'].apply(apply_IATA_regex)
	return data

def join_coordinate_data(routes, coordinates):
	data = pandas.merge(routes, coordinates, left_on = 'OriginIATA',right_on = 'IATA')
	data = pandas.merge(data, coordinates, left_on = 'DestinationIATA',right_on = 'IATA')	
	return data


bcndata = extract_IATA()
coordinates = get_coordinates()
joineddata = join_coordinate_data(bcndata, coordinates)

print (bcndata.head(n=5))
joineddata.to_csv('data/test.csv')