from definitions import URL_FORCAST, URL_STREAM_NODE
import json
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

TOKEN = ''

def requests_retry_session(
    retries=5,
    backoff_factor=0.3,
    status_forcelist=(400, 429,  500, 502, 503, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def get_stream_nodes() -> {}:
	"""Get stored stream_nodes"""
	s = requests.Session()
	s.headers = {'Authorization': 'Bearer ' + TOKEN}
	s.params = {
		"Where": '1=1',
		'outFields': 'STREAM_ID, MUNICIPIO, UF, CIDADE, FATOR_CORRECAO',
		"f": "json"
	}
	response = requests_retry_session(session=s).get(f'{URL_STREAM_NODE}/query')
	streams = {}
	for feat in response.json()['features']:
		streams[feat['attributes']['STREAM_ID']] = [
			feat['attributes']['UF'],
			feat['attributes']['MUNICIPIO'],
			feat['attributes']['CIDADE'],
			feat['attributes']['FATOR_CORRECAO']
		]
	return streams


def get_geoglow_data() -> {}:
	"""Get geoglow stored data."""
	s = requests.Session()
	s.headers = {'Authorization': 'Bearer ' + TOKEN}
	s.params = {
		"Where": '1=1',
		'outFields': 'OBJECTID, STREAM_ID, FLOW_AVG, FLOW_MIN, DATA_CONSULTA, DATA_PREVISAO',
		"f": "json"
	}
	response = requests_retry_session(session=s).get(URL_FORCAST + '/query')
	data = {}
	for feat in response.json()['features']:
		data[feat['attributes']['OBJECTID']] = [
			feat['attributes']['STREAM_ID'],
			feat['attributes']['FLOW_AVG'],
			feat['attributes']['FLOW_MIN'],
			feat['attributes']['DATA_CONSULTA'],
			feat['attributes']['DATA_PREVISAO']
		]
	return data


def get_object_ids(url) -> {}:
	"""Get objectids from a feature service."""
	s = requests.Session()
	s.headers = {'Authorization': 'Bearer ' + TOKEN}
	s.params = {
		"Where": '1=1',
		'outFields': 'OBJECTID',
		"f": "json"
	}
	response = requests_retry_session(session=s).get(f'{url}/query')
	ids = []
	for feat in response.json()['features']:
		ids.append(feat['attributes']['OBJECTID'])
	return ids


def delete_all_features(url) -> {}:
	"""Delete all stored forcasts."""
	ids = get_object_ids(url)
	if len(ids) == 0:
		return
	delete_payload = {
		'objectids': str(ids).replace('[', '').replace(']', ''),
		'f': 'json'
	}
	s = requests.Session()
	s.headers = {'Authorization': 'Bearer ' + TOKEN}
	requests_retry_session(session=s).post(f'{url}/deletefeatures', data=delete_payload)
	ids = get_object_ids(url)
	if len(ids) > 0:
		delete_all_features(url)


def insert_features(url, inserts) -> {}:
	"""Insert features."""
	json_dumps_insert = json.dumps(inserts)
	s = requests.Session()
	s.headers = {'Authorization': 'Bearer ' + TOKEN}
	s.params = {
		"features": json_dumps_insert,
		"f": "json"
	}
	response = requests_retry_session(session=s).post(f'{url}/addFeatures')
	return response.json()
