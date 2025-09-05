import os
import glob
import pandas as pd
import numpy as np

def load_and_process_data():
    """
    Reads all CSV files in the '../data/temperatures/' folder.
    Transforms the data from wide format (one row per station) to long format (one row per station-month).
    """
    all_files = glob.glob(os.path.join("temp", "*.csv"))
    if not all_files:
        raise FileNotFoundError("No CSV files found in the 'temp' directory.")

    list_of_dfs = []
    for filename in all_files:
        df = pd.read_csv(filename)
        # Melt the dataframe: Convert columns for each month into rows
        id_vars = ['STATION_NAME', 'STN_ID', 'LAT', 'LON']
        value_vars = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        
        # Use melt to transform
        melted_df = df.melt(id_vars=id_vars, value_vars=value_vars, 
                            var_name='Month_Name', value_name='Temperature')
        
        # Map month name to number for season calculation
        month_map = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4,
            'May': 5, 'June': 6, 'July': 7, 'August': 8,
            'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        melted_df['Month'] = melted_df['Month_Name'].map(month_map)
        list_of_dfs.append(melted_df)

    # Combine all DataFrames
    combined_df = pd.concat(list_of_dfs, ignore_index=True)
    # Drop rows where Temperature is NaN
    combined_df = combined_df.dropna(subset=['Temperature'])
    return combined_df

def assign_season(month):
    """Assigns an Australian season based on the month."""
    if month in [12, 1, 2]:
        return 'Summer'
    elif month in [3, 4, 5]:
        return 'Autumn'
    elif month in [6, 7, 8]:
        return 'Winter'
    elif month in [9, 10, 11]:
        return 'Spring'

def analyze_temperatures():
    """Main function for Q2: Performs all temperature analyses."""
    try:
        df = load_and_process_data()
        print(f"Successfully loaded data for {len(df)} station-month recordings.")
    except FileNotFoundError as e:
        print(e)
        return
    except Exception as e:
        print(f"An error occurred: {e}")
        return

    # Add a 'Season' column
    df['Season'] = df['Month'].apply(assign_season)

    # --- Seasonal Average ---
    seasonal_avg = df.groupby('Season')['Temperature'].mean().round(1)
    with open('average_temp.txt', 'w') as f:
        for season, avg_temp in seasonal_avg.items():
            f.write(f"{season}: {avg_temp}°C\n")
    print("Seasonal averages saved to '../output/average_temp.txt'.")

    # --- Largest Temperature Range ---
    # Group by station and find min and max temp
    station_stats = df.groupby('STATION_NAME').agg(
        Min_Temp=('Temperature', 'min'),
        Max_Temp=('Temperature', 'max')
    ).round(1)
    station_stats['Range'] = station_stats['Max_Temp'] - station_stats['Min_Temp']

    max_range = station_stats['Range'].max()
    # Find all stations with the maximum range
    stations_with_max_range = station_stats[station_stats['Range'] == max_range]

    with open('largest_temp_range_station.txt', 'w') as f:
        for station, row in stations_with_max_range.iterrows():
            f.write(f"Station {station}: Range {max_range}°C (Max: {row['Max_Temp']}°C, Min: {row['Min_Temp']}°C)\n")
    print("Largest temperature range analysis saved to '../output/largest_temp_range_station.txt'.")

    # --- Temperature Stability (Standard Deviation) ---
    station_std = df.groupby('STATION_NAME')['Temperature'].std().round(1).sort_values()
    min_std = station_std.min()
    max_std = station_std.max()

    most_stable = station_std[station_std == min_std]
    most_variable = station_std[station_std == max_std]

    with open('temperature_stability_stations.txt', 'w') as f:
        f.write("Most Stable:\n")
        for station, std_val in most_stable.items():
            f.write(f"Station {station}: StdDev {std_val}°C\n")
        f.write("\nMost Variable:\n")
        for station, std_val in most_variable.items():
            f.write(f"Station {station}: StdDev {std_val}°C\n")
    print("Temperature stability analysis saved to '../output/temperature_stability_stations.txt'.")