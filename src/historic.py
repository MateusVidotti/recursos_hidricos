from geoglow import get_geoglow_historic_forcast
import pandas as pd
from utils import get_stream_nodes

stream_nodes = get_stream_nodes()

for stream_id in stream_nodes:
    stream_historic = get_geoglow_historic_forcast(stream_id)
    df = pd.DataFrame(data=stream_historic['time_series'])
    print(df)
    df.to_excel(f'historic_geoglow_{stream_id}.xlsx')

