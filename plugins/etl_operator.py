import pandas as pd
import requests
from datetime import datetime
from subprocess import Popen
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

class ANPOperator(BaseOperator):
    '''
    Operator that treats, parse and create a table with url data.

    Methods:
        execute: gets the parsed data and sends to GCS.
        download_transform:
        extract_xlsx:
        transform_data:
    '''
    def __init__(self, *args, **kwargs):
        super(ANPOperator, self).__init__(*args, **kwargs)

    def execute(self, **kwargs):
        """
        Executes the tasks: download_transform() and transform_data().
        """
        self.download_transform()
        self.transform_data()
        self._store_data()

    def download_transform(self):
        """
        Downloads an Excel file from the ANP (Brazilian Petroleum Agency) website and converts it to xlsx format.
        """
        url = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos/de/vdpb/vendas-combustiveis-m3.xls'
        response = requests.get(url)
        
        if response.status_code == 200:
            with open('vendas-combustiveis-m3.xls', 'wb') as file:
                file.write(response.content)
        else:
            print('URL download error.')

        # Converts downloaded xls file to xlsx using libreoffice
        conversion_command = [
            'libreoffice',
            '--headless',
            '--convert-to',
            'xlsx', 
            '--outdir',
            './raw_data',
            'vendas-combustiveis-m3.xls'
        ]
        p_xlsx = Popen(conversion_command)
        p_xlsx.communicate()

    def extract_xlsx(self):
        """
        Checks which are the pivot caches and extracts the hidden table from the xlsx file.
        """
        path = ('./raw_data/vendas-combustiveis-m3.xlsx')
        xl = pd.ExcelFile(path)
        tabs = xl.sheet_names

        year_tabs = {'2013': [], '2000': []}

        for tab in tabs:
            df = xl.parse(tab)
            if 'ANO' in df.columns:
                year = str(df['ANO'].iloc[0])
                if year in year_tabs:
                    year_tabs[year].append(tab)
                    print(f'Pivot cache from table Venda por UF e tipo for {year}:', year_tabs[year])

    def transform_data(self):
        """
        Reads xlsx data, processes it, and creates a final temporary CSV file in a structured format.
        """
        file_path = './raw_data/vendas-combustiveis-m3.xlsx'
        sheets_to_read = ['DPCache_m3 -1', 'DPCache_m3 1']
        dataframes = [pd.read_excel(file_path, sheet_name=sheet) for sheet in sheets_to_read]

        for df in dataframes:
            if 'TOTAL' in df.columns:
                df.drop(columns='TOTAL', inplace=True)

        mapped_months = {
            'Jan': '01', 'Fev': '02', 'Mar': '03', 'Abr': '04',
            'Mai': '05', 'Jun': '06', 'Jul': '07', 'Ago': '08',
            'Set': '09', 'Out': '10', 'Nov': '11', 'Dez': '12'
        }
        combined_dataframes = []
        for df in dataframes:
            melted_df = pd.melt(df, id_vars=["COMBUSTÍVEL", "ANO", "REGIÃO", "ESTADO", "UNIDADE"],
                                var_name="month", value_name="volume")
            melted_df['month'] = melted_df['month'].map(mapped_months)
            melted_df['year_month'] = melted_df['ANO'].astype(str) + '-' + melted_df['month'] + '-01'
            # '01' insertion is necessary due to the type DATE in 'created_at', that only accepts 'YYYY-MM-DD'
            final_df = melted_df[['year_month', 'ESTADO', 'COMBUSTÍVEL', 'UNIDADE', 'volume']]
            final_df['created_at'] = datetime.now()
            final_df.columns = ['year_month', 'uf', 'product', 'unit', 'volume', 'created_at']
            combined_dataframes.append(final_df)

        resulting_dataframe = pd.concat(combined_dataframes, ignore_index=True)
        resulting_dataframe['volume'].fillna(0, inplace=True)
        resulting_dataframe.to_csv('/tmp/processed_data.csv', index=None, header=False
        )      

    def _store_data(self):
        """
        Uses the Postgres Hook to copy data from processed_data.csv into the table
        """
        hook = PostgresHook(postgres_conn_id = 'etl_postgres')
        hook.run("TRUNCATE TABLE vendas_combustiveis")
        hook.copy_expert(
            sql = "COPY vendas_combustiveis(year_month, uf, product, unit, volume, created_at) FROM stdin WITH DELIMITER as ','",
            filename='/tmp/processed_data.csv'
        )