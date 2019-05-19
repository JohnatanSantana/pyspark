#sudo apt-get build-dep python3-psycopg2
#sudo pip3 install psycopg2-binary
import pandas as pd
from sqlalchemy import create_engine
import psycopg2 


def read_csv(file):
    return pd.read_excel(file)

# def write_postgres(df):


file = '/home/johnatan/Downloads/Educational Template.xlsx'

df = read_csv(file)
engine = create_engine('postgresql://postgres:Senha123!@#@172.28.0.3:5432/postgres')
df.to_sql('teste', engine, if_exists='append')
# write_postgres(df)









