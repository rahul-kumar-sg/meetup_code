import requests
import pandas as pd, numpy as np
from time import sleep
import datetime
from sqlalchemy import create_engine

dt = datetime.datetime.now()
date = dt.strftime('%Y-%m-%d %H:%M')

#wait for rate limit reset when we make too many requests
def check_throttle(response):
    if response.headers['X-RateLimit-Remaining'] <= str(1):
        time = response.headers['X-RateLimit-Reset']
        print('Sleeping for ' + time + ' seconds while waiting for rate limit reset...')
        sleep(int(time))

if __name__ == '__main__':
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="root", pw="SG@1234", db="sample_check"))

    conn = engine.connect()
    conn.execute("CREATE TABLE IF NOT EXISTS meetups_all_data (Ecosystem_City varchar(50),\
                 members varchar(10), group_id varchar(50),\
                meetup_city varchar(20), country varchar(10), description MEDIUMTEXT,\
                name varchar(100), who varchar(200));")
    # Read inputs
    cities_df = pd.read_csv("updated_ecosystems.csv")
    headers = {  # need token for authorization
        'Authorization': 'Bearer 2f3271bf6909accd62c4f1a13fed1cbd', 'Cache-Control': 'no-cache', "Pragma": "no-cache"
    }

    df = pd.DataFrame(
        columns=['Ecosystem_City', 'members', 'group_id', 'meetup_city', 'country', 'description', 'name', 'created',
                 'who'])
    locations = tuple(
        zip(cities_df['Ecosystem'].tolist(), cities_df['Latitude'].tolist(), cities_df['Longitude'].tolist()))

    cities = []
    group_id = []
    city = []
    country = []
    description = []
    members = []
    name = []
    created = []
    who = []

    for (location, lat, lon) in (locations):  # make call to first location
        params = {
            # tech events id
            # this is the filter for the tech events, category: 34
            'category': 34,
            'lat': lat,
            'lon': lon,
            'radius': 62,
            'only': 'members,id,name,city,country,created,description,who',
            'offset': 0,
            'page': 200

        }

        print("\rEcosystem processing: {}".format(location), end='',flush=True)

        response = requests.get('https://api.meetup.com/find/groups', headers=headers, params=params)
        json = response.json()

        check_throttle(response)
        # max events is 200 per page, so pull more while there are more
        while True:
            for dict in response.json():
                cities.append(location)
                members.append(dict['members']) if 'members' in dict else members.append(0)
                name.append(dict['name']) if 'name' in dict else name.append(0)
                city.append(dict['city']) if 'city' in dict else city.append(0)
                country.append(dict['country']) if 'country' in dict else country.append(0)
                created.append(dict['created']) if 'created' in dict else created.append(0)
                description.append(dict['description']) if 'description' in dict else description.append(0)
                group_id.append(dict['id']) if 'id' in dict else group_id.append(0)
                who.append(dict['who']) if 'who' in dict else who.append(0)
            if len(response.json()) < 1:
                break  # if there isn't a next page, break

            params['offset'] += 1
            response = requests.get('https://api.meetup.com/find/groups', headers=headers, params=params)
            check_throttle(response)

    df['Ecosystem_City'] = cities
    df['members'] = members
    df['group_id'] = group_id
    df['meetup_city'] = city
    df['country'] = country
    df['created'] = created
    df['name'] = name
    df['description'] = description
    df['who'] = who

    df.to_sql('meetups_all_data', con=engine, if_exists='append', chunksize=1000, index=False)
    conn.close()

