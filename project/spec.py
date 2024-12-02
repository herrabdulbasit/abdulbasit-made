import os
import pytest
import sqlite3
from pipeline import data_pipeline, BASE_DIR

class TestDataPipeline:
    @pytest.fixture
    def pipeline(self):
        return data_pipeline()

    def test_download_datasets(self, pipeline):
        print("Running test_download_datasets")
        pipeline.set_kaggle_api_token()
        unemployment_path = data_pipeline._data_sets[data_pipeline.UNEMPLOYMENT]['file_path']
        unemployment_url = data_pipeline._data_sets[data_pipeline.UNEMPLOYMENT]['url']
        
        print("Downloading Dataset 1")
        pipeline.download_data_sets(unemployment_path, unemployment_url)
        assert os.path.isfile(unemployment_path), f"Unemployment dataset file not found at {unemployment_path}"

        crime_path = data_pipeline._data_sets[data_pipeline.CRIME_RATE]['file_path']
        crime_url = data_pipeline._data_sets[data_pipeline.CRIME_RATE]['url']
        
        print("Downloading Dataset 2")
        pipeline.download_data_sets(crime_path, crime_url)
        assert os.path.isfile(crime_path), f"Crime dataset file not found at {crime_path}"

    def test_database_creation(self, pipeline):
        print("Running test_database_creation")
        pipeline.initialize_pipeline()
        print("Initialized Pipeline")
        data_folder_path = os.path.join(BASE_DIR, os.pardir, 'data')
        data_folder_path = os.path.abspath(data_folder_path)
        db_path = os.path.join(data_folder_path, 'crime_unemployment_analysis.sqlite')
        
        assert os.path.isfile(db_path), f"Database file not found at {db_path}"
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [table[0] for table in cursor.fetchall()]
            
            assert 'crimes' in tables, "Crimes table not found in database"
            assert 'unemployments' in tables, "Unemployments table not found in database"
            assert 'unemployment_crime_merged' in tables, "Merged dataset table not found in database"
            
            conn.close()
        except sqlite3.Error as e:
            pytest.fail(f"Database connection failed: {str(e)}")

    def test_kaggle_token_creation(self, pipeline):
        print("Running test_kaggle_token_creation")
        pipeline.set_kaggle_api_token()
        assert os.path.isfile('kaggle.json'), "Kaggle API token file not created"

    @pytest.fixture(autouse=True)
    def cleanup(self):
        yield
        if os.path.exists('kaggle.json'):
            os.remove('kaggle.json')