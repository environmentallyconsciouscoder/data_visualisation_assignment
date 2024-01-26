import pandas as pd

earthquake_dataset = pd.read_csv("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv")

def get_location_name() -> pd.Series:
    #this is using regular expression to remove the unwanted letters, space and numbers in the location name
    return earthquake_dataset['place'].str.replace(r'\d+\s*km\s*[A-Z]*\s*of\s*(?:The\s*)?', '', regex=True)

def create_csv_for_earthquakes_locations()  -> None:
    # assign the modified 'place' Series back to the 'place' column of the earthquake_dataset DataFrame
    earthquake_dataset['place'] = get_location_name()
    #remove duplicated locations
    unique_earthquake_locations = earthquake_dataset['place'].drop_duplicates()
    #reset indexes starting from zero
    unique_earthquake_locations.reset_index()
    unique_earthquake_locations.to_csv('datasets/locations.csv', sep=',', index=False, encoding='utf-8')

def filter_earth_quake_type(cause):
    return earthquake_dataset[earthquake_dataset['type'] == cause]

def save_dataframe_to_csv(data_frame: pd.DataFrame, name: str) -> None:
    data_frame.to_csv(name, sep=',', index=False, encoding='utf-8')

def create_csv_for_earthquakes_occurences():
    # initialised an empty dataframe and give it two columns called location and count
    counts_df = pd.DataFrame(columns=['location', 'count'])
    locations = pd.read_csv("datasets/locations.csv")
    # need to get each location from the place column
    for location in locations['place']:
        try:
            # by adding the [location] you are doing the the value count for that specified location
            earthquakes_count = earthquake_dataset['place'].value_counts()[location]
            new_row = pd.DataFrame({'location': [location], 'count': [earthquakes_count]})
            # stacking them on top of each other
            # ignore_index=True will reset the index
            counts_df = pd.concat([counts_df, new_row], ignore_index=True)
        except:
            continue
    counts_df = counts_df.sort_values('count',ascending=False).head(30)
    save_dataframe_to_csv(counts_df ,'datasets/earthquake_count_and_location_data.csv')

def create_csv_for_files(columne_one, column_two, file_name) -> None:
    #axis 1 means it will merge horizontally
    coordinates = pd.concat([earthquake_dataset[columne_one], earthquake_dataset[column_two]], axis=1)
    coordinates.to_csv(file_name, sep=',', index=False, encoding='utf-8')

def create_csv_for_data_without_outlier() -> None:
    median = earthquake_dataset['mag'].median()
    higher_quantile = earthquake_dataset['mag'].quantile(0.75)
    lower_quantile = earthquake_dataset['mag'].quantile(0.25)

    earthquake_dataset.loc[earthquake_dataset['mag'] > higher_quantile, 'mag'] = median
    earthquake_dataset.loc[earthquake_dataset['mag'] < lower_quantile, 'mag'] = median
    earthquake_dataset.to_csv('datasets/earthquake_dataset_no_outliers.csv', sep=',', index=False, encoding='utf-8')

def create_csv_line_chart():
    data_set = pd.read_csv("datasets/earthquake_count_and_location_data.csv")
    median_magnitudes = []
    locations = []

    for location_name in data_set['location']:
        # Filter the earthquake dataset for the specified location
        earthquakes_at_location = earthquake_dataset[earthquake_dataset['place'] == location_name]
        # Calculate the median magnitude for the specified location
        median_magnitude = earthquakes_at_location['mag'].median()
        median_magnitudes.append(median_magnitude)
        locations.append(location_name)
    df = pd.DataFrame({'median_magnitude': median_magnitudes, 'locations': locations})
    df.to_csv('datasets/earthquake_median_per_location.csv', sep=',', index=False, encoding='utf-8')


if __name__ == "__main__":
    create_csv_for_earthquakes_locations()
    create_csv_for_earthquakes_occurences()
    create_csv_for_files('latitude', 'longitude', 'datasets/earthquakes_coordinates.csv')
    create_csv_for_files('place', 'mag', 'datasets/locations_and_magnitude.csv')
    create_csv_for_data_without_outlier()
    create_csv_line_chart()
