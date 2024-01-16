from datetime import datetime, timedelta
from definitions import URL_FORCAST, URL_GEOGLOW_API
from os.path import join
import requests
from utils import insert_features, requests_retry_session


def get_geoglow_forcast(reach_id) -> {}:
	"""Get forcast from geoglow api."""
	url = join(URL_GEOGLOW_API, 'ForecastStats')
	s = requests.Session()
	s.params = {'reach_id': reach_id, 'return_format': 'json'}
	response = requests_retry_session(session=s).get(url)
	return response


def get_geoglow_record_forcast(reach_id, days_before) -> {}:
	"""Get record forcast from geoglow api."""
	url = join(URL_GEOGLOW_API, 'ForecastRecords')
	end_date = (datetime.today().date() - timedelta(days=1)).strftime('%Y%m%d')
	start_date = (datetime.today().date() - timedelta(days=days_before)).strftime('%Y%m%d')
	s = requests.Session()
	s.params = {'reach_id': reach_id, 'start_date': start_date, 'end_date': end_date, 'return_format': 'json'}
	response = requests_retry_session(session=s).get(url)
	return response


def get_geoglow_historic_forcast(reach_id) -> {}:
	"""Get historic forcast from geoglow api."""
	url = join(URL_GEOGLOW_API, 'HistoricSimulation')
	s = requests.Session()
	s.params = {'reach_id': reach_id, 'return_format': 'json'}
	response = requests_retry_session(session=s).get(url)
	return response.json()


def insert_forcasts(stream_id, correction_factor,  forcast):
	"""Insert forcasts"""
	time_series = forcast.json()['time_series']
	inserts = []
	inserts_high = []
	i = 0
	# format focasts to insert
	for datetime_str in time_series['datetime']:
		datetime_previsao = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')
		datetime_consulta = datetime.strptime(forcast.json()['gendate'], '%Y-%m-%dT%H:%M:%S.%fZ')
		nova_previsao = {"attributes": {},
										 "geometry": {}}
		nova_previsao["attributes"]['STREAM_ID'] = stream_id
		nova_previsao["attributes"]['DATA_CONSULTA'] = datetime.timestamp(
				datetime_consulta) * 1000  # timestamp oracle em mileseconds
		nova_previsao["attributes"]['DATA_PREVISAO'] = datetime.timestamp(datetime_previsao) * 1000
		nova_previsao["attributes"]['FLOW_25'] = time_series['flow_25%_m^3/s'][i]
		nova_previsao["attributes"]['FLOW_75'] = time_series['flow_75%_m^3/s'][i]
		nova_previsao["attributes"]['FLOW_AVG'] = time_series['flow_avg_m^3/s'][i]
		nova_previsao["attributes"]['FLOW_MAX'] = time_series['flow_max_m^3/s'][i]
		nova_previsao["attributes"]['FLOW_MIN'] = time_series['flow_min_m^3/s'][i]
		# Correction factor
		nova_previsao["attributes"]['FLOW_25_F'] = time_series['flow_25%_m^3/s'][i] * correction_factor
		nova_previsao["attributes"]['FLOW_75_F'] = time_series['flow_75%_m^3/s'][i] * correction_factor
		nova_previsao["attributes"]['FLOW_AVG_F'] = time_series['flow_avg_m^3/s'][i] * correction_factor
		nova_previsao["attributes"]['FLOW_MAX_F'] = time_series['flow_max_m^3/s'][i] * correction_factor
		nova_previsao["attributes"]['FLOW_MIN_F'] = time_series['flow_min_m^3/s'][i] * correction_factor

		inserts.append(nova_previsao)
		i += 1
	# format high forcast to insert
	i = 0
	for datetime_str in time_series['datetime_high_res']:
		datetime_previsao = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')
		datetime_consulta = datetime.strptime(forcast.json()['gendate'], '%Y-%m-%dT%H:%M:%S.%fZ')
		nova_previsao = {"attributes": {},
										 "geometry": {}}
		nova_previsao["attributes"]['STREAM_ID'] = stream_id
		nova_previsao["attributes"]['DATA_CONSULTA'] = datetime.timestamp(datetime_consulta) * 1000
		nova_previsao["attributes"]['DATA_PREVISAO'] = datetime.timestamp(datetime_previsao) * 1000
		nova_previsao["attributes"]['FLOW_HIGH_RES'] = time_series['high_res'][i]
		# Correction factor
		nova_previsao["attributes"]['FLOW_HIGH_RES_F'] = time_series['high_res'][i] * correction_factor
		inserts_high.append(nova_previsao)
		i += 1
	# inserts
	insert_features(URL_FORCAST, inserts)
	insert_features(URL_FORCAST, inserts_high)
	return inserts, inserts_high


def insert_records_forcasts(stream_id, correction_factor, r_forcasts):
	"""Insert record forcast days before today"""
	time_series = r_forcasts.json()['time_series']
	inserts = []
	i = 0
	# format focasts to insert
	for datetime_str in time_series['datetime']:
		datetime_previsao = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')
		datetime_consulta = datetime.strptime(r_forcasts.json()['gendate'], '%Y-%m-%dT%H:%M:%S.%fZ')
		nova_previsao = {"attributes": {}, "geometry": {}}
		nova_previsao["attributes"]['STREAM_ID'] = stream_id
		nova_previsao["attributes"]['DATA_CONSULTA'] = datetime.timestamp(
			datetime_consulta) * 1000  # timestamp oracle em mileseconds
		nova_previsao["attributes"]['DATA_PREVISAO'] = datetime.timestamp(datetime_previsao) * 1000
		nova_previsao["attributes"]['FLOW_25'] = None
		nova_previsao["attributes"]['FLOW_75'] = None
		nova_previsao["attributes"]['FLOW_AVG'] = time_series['flow'][i]
		nova_previsao["attributes"]['FLOW_AVG_F'] = time_series['flow'][i] * correction_factor
		nova_previsao["attributes"]['FLOW_MAX'] = None
		nova_previsao["attributes"]['FLOW_MIN'] = None
		inserts.append(nova_previsao)
		i += 1
	# inserts
	insert_features(URL_FORCAST, inserts)
	return inserts


def get_geoglow_alerts():
	"""Get alerts from geoglow api"""
	url = join(URL_GEOGLOW_API, 'ForecastWarnings')
	s = requests.Session()
	s.params = {'region': 'south_america-geoglows', 'return_format': 'json'}
	response = requests_retry_session(session=s).get(url)
	return response.json()


def get_high_alert_level(period):
		if period == 2:
			return 'Baixo'
		elif period == 5:
			return 'Médio'
		else:
			return 'Alto'


def get_low_alert_level(variance):
	if variance <= 30:
		return 'Baixo'
	elif variance <= 70:
		return 'Médio'
	else:
		return 'Alto'
