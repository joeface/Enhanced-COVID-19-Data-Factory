# Enhanced COVID-19 Data Factory with Redis Cluster Support

Enhanced COVID-19 Data Factory is a fork of [COVID-19 Data Factory](https://github.com/joeface/COVID-19-Data-Fabric). This is a Python script that fetches actual COVID-19 data from CSSE at JHU, Worldometers and projects it onto Population data (UN). The app supports manual data input from a Google Sheet as well.

The output of the script is ready-to-go [GeoJSON list](https://medium.com/@joeface/building-a-covid-19-map-using-django-leafletjs-google-spreadsheets-and-s3-cloud-storage-75bb522771f9) with geometry for all countries.

A lightweight list or GeoJSON features for each country used to render the map may be found in [world-map-geo.json](world-map-geo.json) file.

**By default the result is stored in Redis Cluster. In case Redis is not available, data is stored as a file in WORKDIR**

Live map is available [here](https://www.currenttime.tv/a/covid-19-interactive-map/30484955.html).


## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install required packages.
Packages may be installed in a virtualenv (recommended) or globally.

```bash
pip install -r requirements.txt
```

## Usage

If you would like to activate Manual Data Input feature, please set environment variable MANUAL_DATA_SOURCE_URL with a link to Google Sheet. More details on the feature you may find in a [blog post](https://medium.com/analytics-vidhya/how-to-keep-your-app-serverless-with-google-sheets-as-a-data-storage-e80dff70e9fc). Sample Sheet is available [here](https://docs.google.com/spreadsheets/d/10Xlr25Qtb-sRDJPqX66zqPPEP8yignUsPxxhT0O9Zb0/edit?usp=sharing).

```bash
export MANUAL_DATA_SOURCE_URL='https://docs.google.com/spreadsheets/d/e/SPREADSHEET_ID/pub?gid=0&single=true&output=csv'
```

It is also possible to force the app to use Worldometer data for a country. Just set up an environment variable FORCE_WORLDOMETER with a comma-separated list of ISO-codes. Like this: 

```bash
export FORCE_WORLDOMETER=KGZ,KAZ,RUS,UKR,MZX,UZB
```

You may also set your Redis Master name as an environment variable (default value is 'mymaster'):

```bash
export REDIS_MASTER='my-redis-master'
```

Copy **main.py** file content into Google Cloud Function editor or Amazon Lambda and execute function.

```python

update_covid19_data()

```

To run the app on your Linux container simply run
```python

python main.py

```


You may also setup a scheduler (cron) to run the command periodically. 
The instruction below runs the app each 5th minute of each hour using python from app virtualenv (we need access to packages listed in requirements.txt) and rewrites log at $PATH_TO_LOG
```bash
05 * * * * $PATH_TO_VIRTUALENV/bin/python $PATH_TO_APP/main.py > $PATH_TO_LOG 2>&1
```

## Localizaton
Currently the app generates data localized for two languages: English and Russian. 
However, Farsi, Balkan, Georgia, Ukrainian, Belarus and Kyrgyz are also supported.


## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)