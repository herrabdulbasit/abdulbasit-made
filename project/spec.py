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
        pipeline.setup_kaggle_api()
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

    def test_unemployment_transformation(self, pipeline):
        print("Running test_unemployment_transformation")
        pipeline.init_unemployment_df()
        pipeline.transform_unemployment_df()
        
        years = pipeline.unemp_df['Year'].unique()
        assert min(years) >= 1990, "Years before 1990 found in transformed unemployment data"
        assert max(years) <= 2010, "Years after 2010 found in transformed unemployment data"
        
        assert all(month in pipeline._valid_months for month in pipeline.unemp_df['Month'].unique()), \
            "Invalid months found in transformed data"
        
        assert pipeline.unemp_df['Rate'].dtype.kind in 'fc', "Rate column not converted to numeric"
        assert pipeline.unemp_df['Year'].dtype.kind in 'i', "Year column not converted to integer"
        
        unemployment_levels = pipeline.state_level_unemp_df['Unemployment_Level'].unique()
        assert all(level in ['Low', 'Medium', 'High'] for level in unemployment_levels), \
            "Invalid unemployment level categories found"

    def test_crime_transformation(self, pipeline):
        print("Running test_crime_transformation")
        pipeline.init_crime_rate_df()
        pipeline.transform_crime_rate_df()
        
        required_columns = ['Record ID', 'City', 'State', 'Year', 'Month', 'Crime Type']
        assert all(col in pipeline.crime_r_df.columns for col in required_columns), \
            "Missing required columns in crime data"
        
        years = pipeline.crime_r_df['Year'].unique()
        assert min(years) >= 1990, "Years before 1990 found in transformed crime data"
        assert max(years) <= 2010, "Years after 2010 found in transformed crime data"
        
        assert all(month in pipeline._valid_months for month in pipeline.crime_r_df['Month'].unique()), \
            "Invalid months found in transformed crime data"
        
        assert not pipeline.crime_r_df['Crime Type'].isnull().any(), \
            "Null values found in Crime Type column"

    def test_datasets_merge(self, pipeline):
        print("Running test_datasets_merge")
        pipeline.init_unemployment_df()
        pipeline.init_crime_rate_df()
        pipeline.transform_unemployment_df()
        pipeline.transform_crime_rate_df()
        pipeline.merge_datasets()
        
        required_columns = ['Year', 'Month', 'State', 'Average_Unemployment_Rate', 'Crime_Count']
        assert all(col in pipeline.merged_datasets.columns for col in required_columns), \
            "Missing required columns in merged dataset"
        
        duplicates = pipeline.merged_datasets.duplicated(['Year', 'Month', 'State']).sum()
        assert duplicates == 0, f"Found {duplicates} duplicate entries in merged dataset"

    @pytest.fixture(autouse=True)
    def cleanup(self):
        yield
        if os.path.exists('kaggle.json'):
            os.remove('kaggle.json')