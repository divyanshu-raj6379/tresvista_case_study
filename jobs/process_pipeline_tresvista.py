from configs.settings import settings
import configs.connections as connections
import functions.functions_psql as psql_fn
from functions.functions_email import SendEmail
import pandas as pd
import datetime
import logger.logger as logger

class ProcessTresvista:
    def __init__(self):
        connections.create_psql_connection()
        self.path = 'BIGC Model.xlsx'
        self.current_time = datetime.datetime.now()
        self.author = ""
        self.ticker = ""

    def process_src_dataset(self):
        """
        This process pulls data from `Data extraction` sheet of BIGC Model.xlsx
        and creates staging data. 

        Args:   NONE
        """
        settings.logger.info("Pulling author and ticker data from extracted data sheet...")
        try:
            df0 = pd.read_excel(self.path,
                                sheet_name='Data extraction',
                                header=None,
                                usecols=[2,3],
                                nrows=4,
                                names=['key','value'])
            self.author = list(df0[df0['key']=='Email']['value'])[0]
            self.ticker = list(df0[df0['key']=='Bloomberg Ticker']['value'])[0]
            settings.logger.info("Author and ticker data pulled from extracted data sheet!")
        except Exception as e:
            settings.logger.error(f"Exception {e} in pulling author/ticker data!")
        settings.logger.info("Pulling line item data from extracted data sheet...")
        try:
            df = pd.read_excel(self.path, 
                                sheet_name='Data extraction',
                                header=[0,1,2],
                                skiprows=5)
            data = df[df.columns[1:3].to_list()+df.columns[4:].to_list()]
            headers = data.columns.to_list()
            headers[0] = 'Category'
            headers[1] = 'Line Item'
            data.columns = headers
            data.set_index(headers[:2],inplace=True)
            reqd_columns = [col for col in data.columns if col[2]=='FY']
            data2 = data[reqd_columns].reset_index()
            settings.logger.info("Line item data pulled from extracted data sheet!")
        except Exception as e:
            settings.logger.error(f"Exception {e} in pulling data from extracted data sheet!")
        key = data2.iloc[0,0]
        for i in range(1,len(data2)):
            if pd.isna(data2.iloc[i,0]):
                data2.iloc[i,0] = key
            else:
                key = data2.iloc[i,0]
        drop_rows = data2[data2['Line Item'].isna()].index.to_list()
        data2.drop(drop_rows, inplace=True)
        data2.reset_index(inplace=True,drop=True)
        self.create_final_dataset(data2)

    def create_final_dataset(self, data):
        """
        This process transforms and arranges data into data ready
        to be pushed to SQL DB. It finally calls method to push
        final data to DB.

        Args:-   
            data:   Dataframe containing staging data
        """
        settings.logger.info("Creating final data to push to DB...")
        final_data = pd.DataFrame()
        try:
            data_multi_index_cols = list(data.columns)[2:]
            for i in range(len(data)):
                category = data.iloc[i,0]
                line_item = data.iloc[i,1]
                ins_cols = ['author_name','ticker_name','category','line_item',
                            'cal_year','period','estimate_actual','item_value','last_refresh']
                ins_data = {col:[] for col in ins_cols}  
                try: 
                    for j in range(len(data_multi_index_cols)):
                        ins_data['author_name'].append(self.author)
                        ins_data['ticker_name'].append(self.ticker)
                        ins_data['category'].append(category)
                        ins_data['line_item'].append(line_item)
                        ins_data['cal_year'].append(data_multi_index_cols[j][1])
                        ins_data['period'].append('FY')
                        ins_data['estimate_actual'].append(data_multi_index_cols[j][0])
                        ins_data['item_value'].append(data.iloc[i,j+2])
                        ins_data['last_refresh'].append(self.current_time)
                    final_data = pd.concat([final_data,pd.DataFrame(ins_data)], \
                        axis=0, ignore_index=True)
                except Exception as e:
                    settings.logger.error(f"Exception {e} in creating data dictionary!")
            final_data = final_data.fillna('')
            final_data = final_data[final_data['item_value']!='']
            final_data.reset_index(inplace=True,drop=True)
            settings.logger.info("Final data created!")
            settings.logger.info(f"Shape of Final data: {final_data.shape}")
        except Exception as e:
            settings.logger.error(f"Exception {e} in creating final data!")
        self.push_data_to_db(final_data)

    def push_data_to_db(self, df):
        """
        This process updates final data into Tresvista DB on PSQL server.
        After that it calls method to send email to receiver.

        Args:-
            df:     Dataframe containing final data
        """
        db, tbl = 'db_tresvista', 'model_lineitem_rpt'
        create_sql = \
        f'''CREATE TABLE IF NOT EXISTS {db}.{tbl} ( 
            row_id SERIAL PRIMARY KEY,
            author_name VARCHAR(128) NOT NULL,
            ticker_name VARCHAR(128) NOT NULL,
            category VARCHAR(128) NOT NULL,
            line_item VARCHAR(128) NOT NULL,
            cal_year INT NOT NULL,
            period VARCHAR(4) NOT NULL,
            estimate_actual VARCHAR(12) NOT NULL,
            item_value NUMERIC NULL,
            last_refresh TIMESTAMP NOT NULL );
        '''
        truncate_sql = f"TRUNCATE TABLE {db}.{tbl};"
        check = psql_fn.check_exists(db, tbl)
        if not check.iloc[0,0]:
            settings.logger.info(f"{db}.{tbl} does not exist! Creating the table...")
            try:
                psql_fn.table_ddl(create_sql)
                settings.logger.info(f"Created table {db}.{tbl}!")
            except Exception as e:
                settings.logger.error(f"Exception {e} creating {db}.{tbl}!")
        else:
            settings.logger.info(f"{db}.{tbl} already exists!")
        settings.logger.info(f"Truncating table {db}.{tbl}...")
        try:
            psql_fn.table_ddl(truncate_sql)
            settings.logger.info(f"Table {db}.{tbl} truncated!")
        except Exception as e:
            settings.logger.error(f"Exception {e} in truncating {db}.{tbl}!")
        settings.logger.info(f"Inserting final data to {db}.{tbl}...")
        try:
            psql_fn.fast_insert(db, tbl, df)
            settings.logger.info(f"Final data inserted to table {db}.{tbl}!")
        except Exception as e:
            settings.logger.error(f"Exception {e} in inserting final data to {db}.{tbl}!")
        self.send_email(df)

    def send_email(self, df):
        """
        This method sends email to the receiver and also sends 2 attachments - 
        logfile, data file in .csv form
        
        Args:-
            df:     Dataframe containing final data
        """
        logfile = logger.get_logfile()
        data_filename = f"data_{('_'.join(logfile.split('_')[1:])).split('.')[:-1][0]}.csv"
        datafile = f"output/{data_filename}"
        df.to_csv(datafile, index=False)
        # receiver is hard-coded for testing only
        # receiver = self.author
        receiver = 'divyanshu.raj@praxis.ac.in'
        files = [logfile,datafile]
        se = SendEmail(receiver, files)
        settings.logger.info("Sending email to receiver...")
        se.send_email()
        settings.logger.info("Email sent to receiver with attachments!")