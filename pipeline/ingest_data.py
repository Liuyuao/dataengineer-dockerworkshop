#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd


# In[3]:


pd.__file__


# In[4]:


prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
url = f'{prefix}/yellow_tripdata_2021-01.csv.gz'
url


# In[5]:


df = pd.read_csv(url)


# In[9]:


df.head()


# In[10]:


len(df)


# In[12]:


df['VendorID']


# In[16]:


df['tpep_pickup_datetime']


# In[6]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates
)


# In[7]:


df.head()


# In[17]:


df


# In[19]:


get_ipython().system('uv add psycopg2-binary')


# In[8]:


from sqlalchemy import create_engine
engine = create_engine('postgresql+psycopg://root:root@localhost:5432/ny_taxi')


# In[9]:


print(pd.io.sql.get_schema(df, name = 'yellow_taxi_data', con=engine))


# In[13]:


df.head(0).to_sql(name = 'yellow_taxi_data', con=engine, if_exists='replace')


# In[10]:


df.head(0)


# In[12]:


df_iter = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000
)


# In[14]:


get_ipython().system('uv add tqdm')


# In[15]:


from tqdm.auto import tqdm


# In[16]:


for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[ ]:




