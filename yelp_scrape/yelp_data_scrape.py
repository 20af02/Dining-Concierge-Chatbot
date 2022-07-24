import requests
import json
import csv
import datetime

API_KEY= ""


# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.


# Defaults for our simple example.
DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'Manhattan'
SEARCH_LIMIT = 50

def search(api_key, term, location, offset):
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT,
        'offset': offset
    }
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }
    url = '{0}{1}'.format(API_HOST, SEARCH_PATH)
    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()

def scrape_yelp():
    #cuisine list Chinese, Italian, Japanese, Mexican, Greek
    cuisines = ['Indian']
    # create a csv file with headers
    with open('yelp_data_new.csv', 'a') as yelp_csv:
        field_names = ['business_id', 'insertedAtTimestamp', 'name', 'address', 'coordinates','number_of_reviews', 'rating', 'zip_code', 'cuisine']
        writer = csv.DictWriter(yelp_csv, fieldnames=field_names, delimiter='|')
        #writer.writeheader()
        for cuisine in cuisines:
            print("scraping {} cuisine".format(cuisine))
            cuisine = cuisine + ' restaurant'
            offset = 0
            records = 0
            res_id = []
            while len(res_id) < 1000:
                print("Start of the loop, {cui} cuisine has {rec} rows".format(cui=cuisine, rec=len(res_id)))
                res = search(API_KEY, cuisine, DEFAULT_LOCATION, offset)
                if res.get('businesses') is None:
                    print("No {} cuisine found".format(cuisine))
                    break
                for x in res['businesses']:
                    if x['id'] in res_id:
                        continue
                    res_id.append(x['id'])
                    row = {
                        'business_id': x['id'],
                        'insertedAtTimestamp': str(datetime.datetime.now()),
                        'name': x['name'],
                        'address': x['location']['display_address'],
                        'coordinates': str(x['coordinates']['latitude']) + ',' + str(x['coordinates']['longitude']),
                        'number_of_reviews': x['review_count'],
                        'rating': x['rating'],
                        'zip_code': x['location']['zip_code'],
                        'cuisine': cuisine

                    }
                    writer.writerow(row)
                records += len(res['businesses'])
                if records > res['total']:
                    break
                offset += 50
                print("End of the loop, {cui} cuisine has {rec} rows".format(cui=cuisine, rec=len(res_id)))
            print("{cui} cuisine has {rec} rows".format(cui=cuisine, rec=len(res_id)))

def main():
    scrape_yelp()

if __name__=='__main__':
    main()