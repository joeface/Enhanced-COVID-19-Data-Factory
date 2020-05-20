'''
Fetchs COVID-19 spread data from 4 sources:
1. CSSE at JHU ArcGIS,
2. CSSE at JHU github repo
3. Worldometer website
4. Manual Input from Google Spreadsheet

It also fetches global 2020 Population estimates based on UN data and calculates Confirmed Cases/Deaths/Recovered/Active Cases density for given population and number of Confirmed Cases

The data is to be saved into Redis Storage

'''
import sys
import os
import logging
import csv
import json
from io import StringIO
from datetime import datetime

import requests
import json_logging
from bs4 import BeautifulSoup
from redis.sentinel import Sentinel


'''
Google Spreadsheet URL providing the data for manual update
The Spreadsheet should be pubished as a CSV and prove data in format:
Country Title,Confirmed Cases,Deaths,Recovered,Source,Latest Update (YYYY-MM-DD HH:MM:SS, GMT)
'''
MANUAL_DATA_SOURCE_URL = os.environ.get(
    'MANUAL_DATA_SOURCE_URL') if os.environ.get('MANUAL_DATA_SOURCE_URL') else None


COUNTRIES = {}
CODES = {}

'''
Used to normalize country titles as long as each data source has own naming standard
'''
TITLES = {
    'Iran (Islamic Republic of)': 'Iran',
    'US': 'United States of America',
    'USA': 'United States of America',
    'UK': 'United Kingdom',
    'Republic of Moldova': 'Moldova',
    'Mainland China': 'China',
    'Viet Nam': 'Vietnam',
    'Macao SAR': 'Macau S.A.R',
    'Macao': 'Macau S.A.R',
    'China, Macao SAR': 'Macau S.A.R',
    'Russian Federation': 'Russia',
    'China, Hong Kong SAR': 'Hong Kong S.A.R.',
    'Hong Kong SAR': 'Hong Kong S.A.R.',
    'Hong Kong': 'Hong Kong S.A.R.',
    'Holy See': 'Vatican (Holy See)',
    'Vatican (Holy Sea)': 'Vatican (Holy See)',
    'Vatican City': 'Vatican (Holy See)',
    'occupied Palestinian territory': 'The Palestinian Territories',
    'Palestine': 'The Palestinian Territories',
    'West Bank and Gaza': 'The Palestinian Territories',
    'State of Palestine': 'The Palestinian Territories',
    'Republic of Korea': 'Korea, South',
    'S. Korea': 'Korea, South',
    'Czechia': 'Czech Republic',
    'Taiwan*': 'Taiwan',
    'China, Taiwan Province of China': 'Taiwan',
    'Cote d\'Ivoire': 'Ivory Coast (Côte d\'Ivoire)',
    'Côte d\'Ivoire': 'Ivory Coast (Côte d\'Ivoire)',
    'Ivory Coast': 'Ivory Coast (Côte d\'Ivoire)',
    'UAE': 'United Arab Emirates',
    'Faeroe Islands': 'Faroe Islands',
    'St. Vincent Grenadines': 'Saint Vincent and the Grenadines',
    'CAR': 'Central African Republic',
    'St. Barth': 'St. Barths',
    'Saint Barthélemy': 'St. Barths',
    'DRC': 'Democratic Republic of the Congo',
    'Congo (Kinshasa)': 'Democratic Republic of the Congo',
    'Kyrgyzstan': 'Kyrgyz Republic',
    'Diamond Princess': 'Diamond Princess (Cruise Ship)',
    'MS Zaandam': 'MS Zaandam (Cruise Ship)',
    'Cruise Ship': 'Diamond Princess (Cruise Ship)',
    'Cabo Verde': 'Cape Verde',
    'East Timor': 'Timor-Leste',
    'Congo (Brazzaville)': 'Congo',
    'Curacao': 'Curaçao',
    'Burma': 'Myanmar',
    'United Republic of Tanzania': 'Tanzania',
    'Venezuela (Bolivarian Republic of)': 'Venezuela',
    'Dem. People\'s Republic of Korea': 'North Korea',
    'Bolivia (Plurinational State of)': 'Bolivia',
    'United States Virgin Islands': 'U.S. Virgin Islands',
    'Lao People\'s Democratic Republic': 'Laos',
    'Brunei Darussalam': 'Brunei',
    'Saint Martin (French part)': 'Saint Martin',
    'Syrian Arab Republic': 'Syria',
    '': '',
}

REDIS_MASTER = os.environ.get(
    'REDIS_MASTER') if os.environ.get('REDIS_MASTER') else 'mymaster'

json_logging.ENABLE_JSON_LOGGING = True
json_logging.init_non_web()

logger = logging.getLogger("Test Logger")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))


class CovidDataEnhancedFactory(object):

    def __init__(self):

        self.covid_data = {}  # App Data Storage
        self.geojson_countries = {}  # GeoJSON data for each country

    def execute(self):

        redis_status = False

        try:

            self.read_country_list()
            self.read_geojson()

            try:
                self.covid_data = self.read_arcgis()
                logger.info('We have latest data from JHU ArcGIS')

            except Exception:
                logger.exception(
                    '! Error fetching data from JHU ArcGIS')
                raise

            try:
                self.combine_data()
                logger.info(
                    'Got COVID data from Worldometer and Manual Data Source')

            except Exception:
                logger.exception(
                    '! Error acquiring data from Worldometer and Manual Data Source')
                raise

            try:

                self.read_population_data()
                logger.info(
                    'COVID data combined with Population')

            except Exception:
                logger.exception(
                    "! Error combining COVID data with Population")
                raise

            try:
                if not self.validate_json():
                    logger.exception("! COVID JSON data validation fail")
                    raise

                logger.info('Data is valid and ready to writing into Redis')
                redis_status = self.save_to_redis()

            except Exception:
                logger.exception(
                    '! Error while saving data into Redis')
                raise

            logger.info('Saving JSON: ' + 'OK' if redis_status else 'FAIL')

        except:
            logger.exception('! AN ERROR UPDATING COVID DATA')

        return redis_status

    def add_country_data(self, country_name=None, confirmed=0, deaths=0, recovered=0, latest_update=None, source=''):
        '''
        Normalize Country title
        Return a dictionary with COVID-19 country data with source, latest update label and county ISO code
        '''
        if country_name and country_name in TITLES:
            country_name = TITLES[country_name]

        if country_name and country_name in CODES:
            country_code = CODES[country_name]

            confirmed = self.parse_num(confirmed)
            deaths = self.parse_num(deaths)
            recovered = self.parse_num(recovered)
            active = confirmed - deaths - recovered

            return {
                'code': country_code,
                'titles': COUNTRIES[country_code] if country_code in COUNTRIES else [],
                'confirmed': confirmed,
                'deaths': deaths,
                'recovered': recovered,
                'active': active if active > 0 else 0,
                'latest_update': latest_update,
                'source': source
            }

        if country_name:
            logger.info(f'{country_name} title from {source} not found'.encode('utf-8'))

        return None

    def read_country_list(self):
        '''
        Obtain a list of countries from a Google Sheet with ISO code, English and Russian tranlations
        '''

        r = requests.get(
            'https://docs.google.com/spreadsheets/d/e/2PACX-1vQDTcss-EA85HJQrEZF-PinI9uF6qNpBLo-E4O1hJRNFE0xrqD0geF-DqsC1i4x5uG-0GJvxHG8pC67/pub?gid=0&single=true&output=csv', timeout=40)
        r.encoding = 'utf-8'

        if r.status_code != requests.codes.ok:
            logger.error('Died from Coronavirus trying to fetch countries')

        csv_reader = csv.reader(StringIO(r.text), delimiter=',')
        line = 0
        for row in csv_reader:
            if line > 0:
                code = row[0]
                COUNTRIES[code] = {
                    'en': row[1],
                    'ru': row[2]
                }
                CODES[row[1]] = code
            line += 1

        logger.info('Now we have the list of countries')

    def read_geojson(self):
        '''
        Create a dictionary for each country with it's Title, ISO-code and GeoJSON geometry to outline on the map
        '''
        with open(os.path.dirname(os.path.realpath(__file__)) + '/world-map-geo.json') as json_file:
            features = json.load(json_file)

            for item in features['features']:
                self.geojson_countries[item['properties']['ISO_A3']] = {'title': item['properties']['NAME_SORT'],
                                                                        'code': item['properties']['ISO_A3'], 'geometry': item['geometry']}

            logger.info('And GeoJSON data as well')

    def read_population_data(self, verbose=False):

        data = {}

        found = 0
        not_found = 0

        r = requests.get(
            'https://docs.google.com/spreadsheets/d/e/2PACX-1vQH1zxL8a82N_e3RWag6V6X4RkpM6E7gN-o2XKjJ8cN1FWMTGen_lATkvm8kjyNvJayJsqVHz5h3hI_/pub?gid=0&single=true&output=csv', timeout=40)
        r.encoding = 'utf-8'

        if r.status_code != requests.codes.ok:
            logger.error(
                'Died from Coronavirus trying to fetch population data')

        try:
            csv_reader = csv.reader(StringIO(r.text), delimiter=',')

            for row in csv_reader:

                country_name = row[0]

                if country_name in TITLES:
                    country_name = TITLES[country_name]

                if country_name in CODES:

                    found += 1

                    country_code = CODES[country_name]
                    population = self.parse_num(row[1])

                    if country_code in self.covid_data and population > 0:

                        self.covid_data[country_code][
                            'population'] = population

                        # Confirmend Cases Density per 100k
                        confirmed_density = round(
                            self.covid_data[country_code]['confirmed'] * 100 / population, 2)

                        if self.covid_data[country_code]['confirmed'] > 0:
                            # Deaths and Recovered Density per confirmed cases
                            deaths_density = round(
                                self.covid_data[country_code]['deaths'] * 1000 / self.covid_data[country_code]['confirmed'], 2)
                            recovered_density = round(
                                self.covid_data[country_code]['recovered'] * 1000 / self.covid_data[country_code]['confirmed'])
                        else:
                            deaths_density = 0
                            recovered_density = 0

                        active_density = round(
                            self.covid_data[country_code]['active'] * 100 / population, 2) if self.covid_data[country_code]['active'] > 0 else 0

                        self.covid_data[country_code]['cd'] = confirmed_density
                        self.covid_data[country_code]['rd'] = recovered_density
                        self.covid_data[country_code]['dd'] = deaths_density
                        self.covid_data[country_code]['ad'] = active_density

                        if verbose:
                            logger.info("{code:6s} | {cd:8s} | {dd:8s} | {rd:8s} | {ac:8s} ".format(
                                code=country_code, cd=str(confirmed_density), dd=str(deaths_density), rd=str(recovered_density), ac=str(active_density)))

                        if confirmed_density == 0:
                            self.covid_data[country_code]['co'] = 0
                        elif confirmed_density > 0 and confirmed_density < 10:
                            self.covid_data[country_code]['co'] = 0.2
                        elif confirmed_density >= 10 and confirmed_density < 100:
                            self.covid_data[country_code]['co'] = 0.4
                        elif confirmed_density >= 100 and confirmed_density < 200:
                            self.covid_data[country_code]['co'] = 0.6
                        elif confirmed_density >= 200 and confirmed_density < 300:
                            self.covid_data[country_code]['co'] = 0.8
                        elif confirmed_density >= 300:
                            self.covid_data[country_code]['co'] = 1

                        if recovered_density == 0:
                            self.covid_data[country_code]['ro'] = 0
                        elif recovered_density > 0 and recovered_density < 10:
                            self.covid_data[country_code]['ro'] = 0.2
                        elif recovered_density >= 10 and recovered_density < 100:
                            self.covid_data[country_code]['ro'] = 0.4
                        elif recovered_density >= 100 and recovered_density < 200:
                            self.covid_data[country_code]['ro'] = 0.6
                        elif recovered_density >= 200 and recovered_density < 300:
                            self.covid_data[country_code]['ro'] = 0.8
                        elif recovered_density >= 300:
                            self.covid_data[country_code]['ro'] = 1

                        if deaths_density == 0:
                            self.covid_data[country_code]['do'] = 0
                        elif deaths_density > 0 and deaths_density < 5:
                            self.covid_data[country_code]['do'] = 0.2
                        elif deaths_density >= 5 and deaths_density < 10:
                            self.covid_data[country_code]['do'] = 0.4
                        elif deaths_density >= 10 and deaths_density < 50:
                            self.covid_data[country_code]['do'] = 0.6
                        elif deaths_density >= 50 and deaths_density < 100:
                            self.covid_data[country_code]['do'] = 0.8
                        elif deaths_density >= 100:
                            self.covid_data[country_code]['do'] = 1

                        if active_density == 0:
                            self.covid_data[country_code]['ao'] = 0
                        elif active_density > 0 and active_density < 10:
                            self.covid_data[country_code]['ao'] = 0.2
                        elif active_density >= 10 and active_density < 100:
                            self.covid_data[country_code]['ao'] = 0.4
                        elif active_density >= 100 and active_density < 200:
                            self.covid_data[country_code]['ao'] = 0.6
                        elif active_density >= 200 and active_density < 300:
                            self.covid_data[country_code]['ao'] = 0.8
                        elif active_density >= 300:
                            self.covid_data[country_code]['ao'] = 1

                else:
                    not_found += 1
                    # logger.info(f'Country not found: {country_name}')

            return data

        except Exception as e:

            logger.error('! Error occured while reading population data')
            data = {}

        return data

    def read_covid_csse(self):
        '''
        Fetch data from CSSE at JHU COVID-19 github repo.
        Always retrieve yesterday CVS as long as CSSE updates it on a daily basis
        Returns a dictionary with data
        '''

        covid_data = {}
        day = datetime.now().day - 1

        r = requests.get('https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_daily_reports/{0}-{1}-2020.csv'.format(
            datetime.now().strftime('%m'), str(day) if day > 9 else '0{}'.format(day)), timeout=60)

        if r.status_code != requests.codes.ok:
            logger.warning('! Unable to fetch latest data from github')
            return covid_data

        csv_reader = csv.reader(StringIO(r.text), delimiter=',')
        line = 0

        for row in csv_reader:

            if line > 0:
                country_name = row[3]
                latest_update = row[4]
                confirmed = int(row[7])
                deaths = int(row[8])
                recovered = int(row[9])

                obj = self.add_country_data(
                    country_name=country_name, confirmed=confirmed, deaths=deaths, recovered=recovered, latest_update=latest_update, source='JHU CSSE')

                if obj:
                    covid_data[obj['code']] = obj

            line += 1

        return covid_data

    def read_manual_data(self):
        '''
        Fetch manual populated data from a Google Spreadsheet
        Return a dictionary with data
        '''
        data = {}

        if not MANUAL_DATA_SOURCE_URL:
            logger.warning('! No manual data source provided')
            return data

        r = requests.get(MANUAL_DATA_SOURCE_URL, timeout=40)
        r.encoding = 'utf-8'

        if r.status_code != requests.codes.ok:
            logger.error('! Can not fetch manual data from: ' +
                         MANUAL_DATA_SOURCE_URL)
            return data

        try:
            csv_reader = csv.reader(StringIO(r.text), delimiter=',')
            line = 0
            for row in csv_reader:
                if line > 0:

                    obj = self.add_country_data(country_name=row[0], confirmed=row[1], deaths=row[
                        2], recovered=row[3], latest_update=row[5], source={'ru': row[4], 'en': row[6]})

                    if obj:
                        data[obj['code']] = obj

                line += 1

            logger.info('Fetched data from Manual Data Source')

        except Exception:
            logger.exception(
                '! Error parsing manual data from: ' + MANUAL_DATA_SOURCE_URL)
            data = {}

        return data

    def read_arcgis(self):
        '''
        Fetch data from CSSE at JHU COVID-19 ArcGIS service
        Return a dictionary with data
        '''

        data = {}

        response = requests.get('https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/ncov_cases/FeatureServer/2/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=OBJECTID%20ASC&outSR=102100&resultOffset=0&resultRecordCount=250&cacheHint=true&quantizationParameters=%7B%22mode%22%3A%22edit%22%7D', timeout=120)

        if response.status_code != requests.codes.ok:
            logger.warn(
                '! Unable to fetch latest data from CSSE at JHU ArcGIS')
            return data

        json_data = json.loads(response.text)

        if not 'features' in json_data:
            logger.warn('! Wrong data format from CSSE at JHU ArcGIS')
            return data

        for item in json_data['features']:

            obj = self.add_country_data(
                country_name=item['attributes']['Country_Region'], confirmed=item['attributes']['Confirmed'], deaths=item['attributes']['Deaths'], recovered=item['attributes']['Recovered'], latest_update=datetime.fromtimestamp(item['attributes']['Last_Update'] / 1000).strftime("%Y/%m/%d, %H:%M:%S"), source='JHU CSSE')

            if obj:
                data[obj['code']] = obj

        return data

    def read_worldometer(self):
        '''
        Fetch data from COVID-19 page on Worldometer website
        https://www.worldometers.info/coronavirus/

        Return a dictionary with data
        '''

        data = {}

        url = 'https://www.worldometers.info/coronavirus/'
        response = requests.get(url, timeout=40)

        if response.status_code != requests.codes.ok:
            logger.warn('! Unable to fetch latest data from Worldometer')
            return data

        html = BeautifulSoup(response.text, "html.parser")
        table = html.find('table', id='main_table_countries_today')

        for row in table.find('tbody').find_all('tr'):
            cells = row.find_all('td')

            obj = self.add_country_data(country_name=cells[1].text.strip(), confirmed=self.parse_num(cells[2].text), deaths=self.parse_num(
                cells[4].text), recovered=self.parse_num(cells[6].text), latest_update=datetime.now().strftime("%Y/%m/%d, %H:%M:%S"), source='Worldometer')

            if obj:
                data[obj['code']] = obj

        return data

    def combine_data(self):
        '''
        Walks through all data sources and combines data using the rule:
        * First priority — ArcGIS data added into covid_data
        * If a country not found in ArcGIS but presents in CSSE git repo — append it to covid_data
        * If a country presents in Worldometer and cannot be found in our storage (or country code is among the list: SRB, KGZ, KAZ, RUS, UKR, MZX, UZB) - append it
        * If a country presents in manually updated dictionary — overwrite the data in storage

        Returns a dictionary with data
        '''

        wom_data = self.read_worldometer()
        csse = self.read_covid_csse()
        man_data = self.read_manual_data()

        for code in csse:
            if code in self.covid_data:

                if self.covid_data[code]['confirmed'] < csse[code]['confirmed'] or self.covid_data[code]['deaths'] < csse[code]['deaths'] or self.covid_data[code]['recovered'] < csse[code]['recovered']:

                    if self.covid_data[code]['confirmed'] < csse[code]['confirmed']:
                        self.covid_data[code][
                            'confirmed'] = csse[code]['confirmed']

                    if self.covid_data[code]['deaths'] < csse[code]['deaths']:
                        self.covid_data[code]['deaths'] = csse[code]['deaths']

                    if self.covid_data[code]['recovered'] < csse[code]['recovered']:
                        self.covid_data[code][
                            'recovered'] = csse[code]['recovered']

                    self.covid_data[code]['latest_update'] = csse[code]['latest_update'].replace(
                        'T', ' ')
                    self.covid_data[code]['source'] = 'JHU CSSE'

            else:
                self.covid_data[code] = csse[code]
                self.covid_data[code]['latest_update'] = self.covid_data[code]['latest_update'].replace(
                    'T', ' ')
                self.covid_data[code]['source'] = 'JHU CSSE'

        if(len(wom_data)):

            for code in wom_data:
                if code not in self.covid_data or code in ('SRB', 'KGZ', 'KAZ', 'RUS', 'UKR', 'MZX', 'UZB',):
                    if code in COUNTRIES:
                        self.covid_data[code] = wom_data[code]

                    else:
                        logger.warn('! CODE NOT FOUND: ' + code)

        if(len(man_data)):

            for code in man_data:
                self.covid_data[code] = man_data[code]

        return None

    def create_geojson(self, language='ru'):
        '''
        Generates valid GeoJSON with COVID-19 as a property dictionary
        '''
        map_data = []

        for c, data in self.covid_data.items():

            if c in self.geojson_countries:
                country_data = self.geojson_countries[c]

                if c in COUNTRIES:

                    source = ''

                    if type(data['source']) is str:
                        source = data['source']
                    elif type(data['source']) is dict:
                        if language in data['source']:
                            source = data['source'][language]

                    props = {'name': COUNTRIES[country_data['code']][language],
                             'latest_update': data['latest_update'],
                             'confirmed': data['confirmed'],
                             'deaths': data['deaths'],
                             'recovered': data['recovered'],
                             'active': data['active'],
                             'population': data['population'] if 'population' in data else 0,
                             'source': source,
                             'cd': data['cd'] if 'cd' in data else 0,
                             'dd': data['dd'] if 'dd' in data else 0,
                             'rd': data['rd'] if 'rd' in data else 0,
                             'ad': data['ad'] if 'ad' in data else 0,
                             'co': data['co'] if 'co' in data else 0,
                             'do': data['do'] if 'do' in data else 0,
                             'ro': data['ro'] if 'ro' in data else 0,
                             'ao': data['ao'] if 'ao' in data else 0,
                             }

                    if data['confirmed'] > 0 or data['deaths'] > 0 or data['recovered'] > 0 or data['latest_update']:
                        map_data.append({
                            'type': 'Feature',
                            'id': c,
                            'properties': props,
                            'geometry': country_data['geometry']
                        })

                else:
                    logger.info('- Translation not found:',
                                country_data['code'])

            else:
                logger.error('! Country GeoJSON not found:', c)

        return map_data

    def validate_json(self):
        '''
        Check number of records to Save
        '''

        # Should be less or equal to original list of countries and at list 100
        # items long

        if len(COUNTRIES) >= len(self.covid_data) > 100:

            for code, data in self.covid_data.items():

                # Check Confirmed/Deaths/Recovered values and control sum (we
                # allow Turkmenistan and Northern Korea to be COVID-19 free)
                if (not (data['confirmed'] > 0 or data['deaths'] > 0 or data['recovered'] > 0) or not (data['confirmed'] >= data['deaths'] + data['recovered'])) and code not in ('TKM', 'PRK',):
                    logger.error(data)
                    raise ValueError

            # Return True if data is valid
            return True

        # Invalid nuber of records
        return False

    def save_to_redis(self):
        '''
        Save JSON dump into Redis storage
        as covid_data_ru and covid_data_en objects
        '''

        status = False
        data = {}

        for language in ('ru', 'en'):
            data[language] = self.create_geojson(language)

        try:

            logger.info('Connecting to Redis Sentinel')

            sentinel_client = Sentinel(
                [('redis-sentinel', 26379)], socket_timeout=0.1)

            logger.info('Discovering Redis Master')
            sentinel_client.discover_master(REDIS_MASTER)
            master = sentinel_client.master_for(
                REDIS_MASTER, socket_timeout=0.1)

            logger.info('Discovering Redis Slave')
            slave = sentinel_client.slave_for(REDIS_MASTER, socket_timeout=0.1)

            for language in ('ru', 'en'):

                logger.info(f'Saving {language.upper()} covid_data into Slave')
                status = slave.set('covid_data_' + language,
                                   json.dumps(data[language], ensure_ascii=False))

                if not status:
                    return False

            return status

        except:
            logger.warning(
                'Can not proceed with Redis. Saving JSON as a file.')

            try:
                for language in ('ru', 'en'):
                    with open(language + '.json', 'w') as outfile:
                        logger.info(f'Saving {language.upper()} as {language}.json')
                        json.dump(data[language], outfile, ensure_ascii=False)
            except:
                logger.warning('Can not save data as a file.')

            return False

    def parse_num(self, text=None):
        '''
        Format numeric data from Worldometer
        Return 0 or a valid number
        '''
        if type(text) is int:
            return text

        text = text.strip().replace(',', '')
        if len(text):
            try:
                return int(text)
            except:
                return 0
        return 0


def update_covid19_data(event=None, context=None):

    cdf = CovidDataEnhancedFactory()
    cdf.execute()


if __name__ == "__main__":
    update_covid19_data()
