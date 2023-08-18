import pandas as pd
import argparse
import os
import boto3
import logging
import requests
from io import StringIO
import gdown

def main():

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        # Read Station Inventory data
        # Download the Station Inventory from Google Drive
        csv_url = 'https://drive.google.com/uc?id=1HDRnj41YBWpMioLPwAFiLlK4SK8NV72C'
        csv_file_path = 'Station_Inventory_EN.csv'
        gdown.download(csv_url, csv_file_path, quiet=False)

        # Read the downloaded CSV file
        station_inventory_df = pd.read_csv(csv_file_path, skiprows=3)

        # For command line input for year and city
        parser = argparse.ArgumentParser(description='Process input for year and city')
        parser.add_argument('--year', type=int, choices=[2018], help='Valid Input years: (2018)')
        parser.add_argument('--city', type=str, choices=['Toronto'], help='Valid City input: (Toronto)')
        args = parser.parse_args()

        # Set AWS credentials using environment variables
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        
        # Connect to S3
        s3 = boto3.client('s3')
        
        # List of years to retrieve data for
        years_to_retrieve = [args.year - 2, args.year - 1, args.year]
        # Loop through each year and upload data to S3
        for year in years_to_retrieve:
            # Read Weather data for the specified year and city
            # URL to get data for Toronto City
            url = f'https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=31688&Year={year}&Month=12&Day=14&timeframe=2&submit=Download+Data'
            response = requests.get(url)
            if response.status_code == 200:
                # Read the CSV content into a DataFrame
                csv_content = response.content.decode('utf-8')
                weather_data = pd.read_csv(StringIO(csv_content))
                # Concatenate weather data for the year and merge with Station Inventory data
                weather_data['Climate ID'] = weather_data['Climate ID'].astype(str)
                station_inventory_df['Climate ID'] = station_inventory_df['Climate ID'].astype(str)
                merged_data = pd.merge(weather_data, station_inventory_df, on='Climate ID', how='inner')
                merged_data = merged_data.drop(['Data Quality', 'Max Temp Flag', 'Min Temp Flag', 'Mean Temp Flag',
                                                'Heat Deg Days Flag', 'Cool Deg Days Flag', 'Total Rain (mm)', 'Total Rain Flag',
                                                'Total Snow (cm)', 'Total Snow Flag', 'Total Precip Flag', 'Snow on Grnd (cm)',
                                                'Snow on Grnd Flag', 'Dir of Max Gust (10s deg)', 'Spd of Max Gust (km/h)',
                                                'First Year', 'Last Year', 'HLY First Year', 'HLY Last Year', 'DLY First Year',
                                                'DLY Last Year', 'MLY First Year', 'MLY Last Year', 'Longitude (x)', 'Latitude (y)',
                                                'Latitude (Decimal Degrees)', 'Longitude (Decimal Degrees)'], axis=1)
                merged_data['Mean Temp (°C)'] = merged_data['Mean Temp (°C)'].fillna(merged_data['Mean Temp (°C)'].mean())
                merged_data['Max Temp (°C)'] = merged_data['Max Temp (°C)'].fillna(merged_data['Max Temp (°C)'].mean())
                merged_data['Min Temp (°C)'] = merged_data['Min Temp (°C)'].fillna(merged_data['Min Temp (°C)'].mean())
                merged_data['Heat Deg Days (°C)'] = merged_data['Heat Deg Days (°C)'].fillna(merged_data['Heat Deg Days (°C)'].mean())
                merged_data['Cool Deg Days (°C)'] = merged_data['Cool Deg Days (°C)'].fillna(merged_data['Cool Deg Days (°C)'].mean())
                merged_data['Total Precip (mm)'] = merged_data['Total Precip (mm)'].fillna(merged_data['Total Precip (mm)'].mean())

                # Upload merged data to S3
                bucket_name = 'wavehistoricalweatherdata'
                file_name = f'{args.city}_{year}_merged_data.csv'
                s3.upload_file(file_name, bucket_name, file_name)
                logging.info(f"Data for year {year} uploaded to S3 successfully.")
            else:
                print(f"Failed to fetch data for the year: {year}")
            
    except Exception as e:
                logging.error(f"An error occurred while processing year {year}: {e}")

    # Generate Excel file with tabs/sheets for each year
    excel_file_name = f'{args.city}_full_data_all_years.xlsx'
    excel_writer = pd.ExcelWriter(excel_file_name, engine='xlsxwriter')

    for year in years_to_retrieve:
        try:
             # URL to get data for Toronto City
            url = f'https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=31688&Year={year}&Month=12&Day=14&timeframe=2&submit=Download+Data'
            response = requests.get(url)
            if response.status_code == 200:
                # Read the CSV content into a DataFrame
                csv_content = response.content.decode('utf-8')
                weather_data = pd.read_csv(StringIO(csv_content))
                # Write data for the year to the Excel file
                sheet_name = str(year)
                weather_data.to_excel(excel_writer, sheet_name=sheet_name, index=False)
                logging.info(f"Data for year {year} added to Excel file.")
            else:
                 print(f"Failed to fetch data for the year: {year}")
        except Exception as e:
                logging.error(f"An error occurred while processing year {year} for Excel file: {e}")
                
    # Save the Excel file
    excel_writer._save()
    logging.info("Excel file generated successfully.")
    
    # Querying and analyzing data
    try:
        
        # Load data from S3
        s3.download_file(bucket_name, f'{args.city}_{year}_merged_data.csv', f'{args.city}_{year}_merged_data.csv')
        s3.download_file(bucket_name, f'{args.city}_{year-1}_merged_data.csv', f'{args.city}_{year-1}_merged_data.csv')
        s3.download_file(bucket_name, f'{args.city}_{year-2}_merged_data.csv', f'{args.city}_{year-2}_merged_data.csv')

        downloaded_data_input_year = pd.read_csv(f'{args.city}_{year}_merged_data.csv')
        downloaded_data_previous_year = pd.read_csv(f'{args.city}_{year-1}_merged_data.csv')
        downloaded_data_previous_year_2 = pd.read_csv(f'{args.city}_{year-2}_merged_data.csv')

        # Performing queries
        # Max and Min temperature for year
        max_temp = downloaded_data_input_year[downloaded_data_input_year['Year'] == args.year]['Max Temp (°C)'].max()
        min_temp = downloaded_data_input_year[downloaded_data_input_year['Year'] == args.year]['Min Temp (°C)'].min()

        # Percentage difference between average daily temperature for the year vs. average of previous 2 years
        avg_temp_year = downloaded_data_input_year[downloaded_data_input_year['Year'] == args.year]['Mean Temp (°C)'].mean()
        avg_temp_prev_years = (downloaded_data_previous_year['Mean Temp (°C)'].mean() + downloaded_data_previous_year_2['Mean Temp (°C)'].mean()) / 2
        percentage_difference = ((avg_temp_year - avg_temp_prev_years) / avg_temp_prev_years) * 100


        # Difference between average temperature per month for year
        avg_temp_per_month = downloaded_data_input_year[downloaded_data_input_year['Year'] == args.year].groupby('Month')['Mean Temp (°C)'].mean()

        # Print results
        print(f"Max Temperature for {args.year}: {max_temp}")
        print(f"Min Temperature for {args.year}: {min_temp}")
        print(f"Percentage difference in average daily temperature: {percentage_difference:.2f}%")
        print("Average Temperature per Month:")
        print(avg_temp_per_month)
    except Exception as e:
                logging.error(f"An error occurred while processing queries for year {year}: {e}")

if __name__ == "__main__":
    main()
