from src.definitions import URL_FORCAST
import logging
from geoglow import get_geoglow_forcast, get_geoglow_record_forcast,  insert_records_forcasts, insert_forcasts
from multiprocessing import Pool
import tqdm
from utils import delete_all_features,  get_stream_nodes

# aplication log
logging.basicConfig(format='%(asctime)s | %(levelname)s:%(message)s', level=logging.INFO, filename='processmento.log',
                    encoding='utf-8', )
logging.info('Novo processamento')


def process_stream_node(stream_id):
    # get new forcast
    forcasts = get_geoglow_forcast(stream_id)
    correction_factor = stream_nodes[stream_id][3]
    # store forcast
    _insert_forcasts,  _insert_high_forcasts = insert_forcasts(stream_id, correction_factor, forcasts)
    # get record forcast
    r_forcasts = get_geoglow_record_forcast(stream_id, days_before=10)

    # store record forcast
    insert_records_forcasts(stream_id, correction_factor, r_forcasts)


def stream_forcasts():
    with Pool(10) as p:
        stream_ids_list = [stream_id for stream_id in stream_nodes]
        resultado = list(tqdm.tqdm(p.imap(process_stream_node, stream_ids_list), total=len(stream_ids_list)))
        p.close()
        p.join()


if __name__ == '__main__':
    # try:
    stream_nodes = get_stream_nodes()
    print('Limpando tabelas....')
    # delete geoglow forcast
    delete_all_features(URL_FORCAST)
    # delete climatempo forcast
    print('Tabelas limpas')
    # Process forcasts
    print('Processando')
    stream_forcasts()
