import requests
from airflow.models import BaseOperator 
from airflow.utils.decorators import apply_defaults

import os 
import requests

class DownloadDataOperator(BaseOperator):
    
    ui_color = '#FFE6E6'
    
    @apply_defaults
    def __init__(
        self,
        url,
        *args,
        **kwargs
        
    ):
        super(DownloadDataOperator, self).__init__(*args, **kwargs)
        self.url = url 
    
    def download_data_from_url(self):
        try:
            gz_downloaded_file = self.download_file()
        except (Exception, requests.exceptions.RequestException,
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError) as e:
            print("URL is not accessible. ", e)
            pass
    
        return gz_downloaded_file
    
    def download_file(self):
        dest_folder = 'data'
        if not os.path.exists(dest_folder):
            os.makedirs(dest_folder)
        
        filename = self.url.split('/')[-1].replace(" ", "_")
        filepath = os.path.join(dest_folder, filename)
        
        response = requests.get(self.url, stream=True)
        self.log.info("Downloading {}.".format(self.url))
        if response.ok:
            print('Saving to ', os.path.abspath(filepath))
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024 * 8):
                    if chunk:
                        f.write(chunk)
                        f.flush()
                        os.fsync(f.fileno())
        else:
            self.log.error("Download failed: status code {}\n{}".format(response.status_code, response.text))
           
        gz_downloaded_file =  os.path.join(dest_folder, filename)
        return gz_downloaded_file
        
    def execute(self, context):
        gz_downloaded_file = self.download_data_from_url()
        context['ti'].xcom_push(key='gz_downloaded_file', value=gz_downloaded_file)
        