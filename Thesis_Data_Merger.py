import pandas as pd
import os
import re
from io import StringIO


def compute_target_period(year, quarter):
    quarter_to_month = {
        'Q1': 'Mar',
        'Q2': 'Jun',
        'Q3': 'Sep',
        'Q4': 'Dec'
    }
    
    quarter_order = ['Q1', 'Q2', 'Q3', 'Q4']
    
    # Compute the current quarter index
    current_index = quarter_order.index('Q' + quarter)
    
    # Compute the target quarter index, 3 quarters ahead
    target_index = (current_index + 3) % 4
    
    # Compute the target year
    target_year = int(year) + (current_index + 3) // 4
    
    # Compute the target period
    target_period = f"{target_year}{quarter_to_month[quarter_order[target_index]]}"
    
    return target_period

def merge_datasets_inflation(folder_path):
    files = sorted([f for f in os.listdir(folder_path) if re.match(r'\d{4}Q[1-4]\.csv', f)], reverse=True)
    inflation_data = pd.DataFrame()

    for filename in files:
        file_path = os.path.join(folder_path, filename)
        # Read the file as plain text to find the relevant section
        with open(file_path, 'r') as file:
            content = file.read()
        
        # Isolate the section for Inflation Expectations if it exists
        match = re.search(r'INFLATION EXPECTATIONS; YEAR-ON-YEAR CHANGE IN HICP.*?(?=CORE INFLATION EXPECTATIONS|GROWTH EXPECTATIONS|EXPECTED UNEMPLOYMENT RATE|$)', content, re.DOTALL)
        if not match:
            print(f"No relevant data section found in file: {filename}")
            continue

        # If a section is found, read it as a DataFrame
        data_section = match.group(0)
        df = pd.read_csv(StringIO(data_section), header=1)
        
        # Ensure only required columns are considered
        if 'TARGET_PERIOD' in df.columns and 'FCT_SOURCE' in df.columns and 'POINT' in df.columns:
            df = df[['TARGET_PERIOD', 'FCT_SOURCE', 'POINT']].dropna()
            year, quarter = re.search(r'(\d{4})Q([1-4])', filename).groups()
            target_period = compute_target_period(year, quarter)

            # Filter rows based on the target period
            filtered_rows = df[df['TARGET_PERIOD'].str.contains(f'^{target_period}$', regex=True)]

            # Ensure FCT_SOURCE is of type int to match the forecaster_ids DataFrame
            filtered_rows.loc[:, 'FCT_SOURCE'] = filtered_rows['FCT_SOURCE'].astype(int)

            # Create a DataFrame with all forecasters for the current period
            forecaster_ids = pd.DataFrame({'FCT_SOURCE': range(1, 151)})
            period_data = pd.DataFrame({'TARGET_PERIOD': [target_period] * 150})
            full_forecaster_df = pd.concat([forecaster_ids, period_data], axis=1)

            # Merge the filtered inflation data with the full forecaster DataFrame
            inflation_rows_full = pd.merge(full_forecaster_df, filtered_rows, 
                                           on=['FCT_SOURCE', 'TARGET_PERIOD'], 
                                           how='left')

            # Concatenate to the main dataframe
            inflation_data = pd.concat([inflation_data, inflation_rows_full], ignore_index=True)
        else:
            print(f"Required columns are missing in data section in file: {filename}")

    # Save the collected data if any
    if not inflation_data.empty:
        output_file = os.path.join(folder_path, 'Inflation_SPF.csv')
        inflation_data.to_csv(output_file, index=False)
        print(f"Data merged and saved to {output_file}")
    else:
        print("No data found to merge.")
        
def merge_datasets_GDP(folder_path):
    # Mapping quarters to the respective target period in the next year
    quarter_to_next_qtr = {
        'Q1': 'Q3',  # Q1 of this year targets Q3 of the same year
        'Q2': 'Q4',  # Q2 of this year targets Q4 of the same year
        'Q3': 'Q1',  # Q3 of this year targets Q1 of the next year
        'Q4': 'Q2'   # Q4 of this year targets Q2 of the next year
    }
    
    # Load files, sorted from latest to earliest
    files = sorted([f for f in os.listdir(folder_path) if re.match(r'\d{4}Q[1-4]\.csv', f)], reverse=True)
    growth_data = None

    for filename in files:
        file_path = os.path.join(folder_path, filename)
        df = pd.read_csv(file_path, header=1)
        
        # Extract year and quarter
        year, quarter = filename.split('Q')
        quarter = 'Q' + quarter[:-4]  # Remove '.csv' and append 'Q' to the quarter number
        next_year = str(int(year) + (1 if quarter in ['Q3', 'Q4'] else 0))
        target_period = next_year + quarter_to_next_qtr[quarter]

        # Create a flag to determine when to start and stop scraping rows
        include_rows = False
        temp_data = []

        for index, row in df.iterrows():
            if row['TARGET_PERIOD'] == target_period and not include_rows:
                include_rows = True  # Start including rows
            
            if include_rows:
                if row['TARGET_PERIOD'] != target_period:
                    break  # Stop including when TARGET_PERIOD changes
                temp_data.append(row)

        # Convert list to DataFrame
        if temp_data:
            temp_df = pd.DataFrame(temp_data)
            
            # Ensure FCT_SOURCE is of type int to match the forecaster_ids DataFrame
            temp_df.loc[:, 'FCT_SOURCE'] = temp_df['FCT_SOURCE'].astype(int)
            
            # Create a DataFrame with all forecasters for the current period
            forecaster_ids = pd.DataFrame({'FCT_SOURCE': range(1, 151)})
            period_data = pd.DataFrame({'TARGET_PERIOD': [target_period] * 150})
            full_forecaster_df = pd.concat([forecaster_ids, period_data], axis=1)

            # Merge the filtered GDP data with the full forecaster DataFrame
            temp_df_full = pd.merge(full_forecaster_df, temp_df, 
                                    on=['FCT_SOURCE', 'TARGET_PERIOD'], 
                                    how='left')

            growth_data = pd.concat([growth_data, temp_df_full]) if growth_data is not None else temp_df_full

    # Save the concatenated dataframe if it is not empty
    if growth_data is not None:
        output_file = os.path.join(folder_path, 'GDP_SPF.csv')
        growth_data.to_csv(output_file, index=False)
        print(f"Data merged and saved to {output_file}")
    else:
        print("No data found to merge.")





import os
import re
import pandas as pd
import numpy as np

def compute_target_period_unemployment(year, quarter):
    # Define the mapping from quarters to months in the next year
    quarter_to_month = {
        'Q1': 'Nov',  # Q1 of this year targets November of the same year
        'Q2': 'Feb',  # Q2 of this year targets February of the next year
        'Q3': 'May',  # Q3 of this year targets May of the next year
        'Q4': 'Aug'   # Q4 of this year targets August of the next year
    }

    # Compute the target year (Q1 targets the same year, others target the next year)
    if quarter == 'Q1':
        target_year = year
    else:
        target_year = str(int(year) + 1)
    
    # Combine the target year with the corresponding month from the mapping
    target_period = f"{target_year}{quarter_to_month[quarter]}"
    
    return target_period

def merge_datasets_unemployment(folder_path):
    files = sorted([f for f in os.listdir(folder_path) if re.match(r'\d{4}Q[1-4]\.csv', f)], reverse=True)
    unemployment_data = pd.DataFrame()

    for filename in files:
        file_path = os.path.join(folder_path, filename)
        df = pd.read_csv(file_path, header=1)
        
        # Extract year and quarter from the filename
        year, quarter = filename.split('Q')
        quarter = 'Q' + quarter[0]  # Isolate the quarter number and prepend 'Q'

        # Compute the target period using the helper function
        target_period = compute_target_period_unemployment(year, quarter)

        # Fill empty cells with NaN
        df.replace("", np.nan, inplace=True)

        # Ensure TARGET_PERIOD does not contain NaN before filtering
        df = df.dropna(subset=['TARGET_PERIOD'])

        # Filter rows based on the target period
        unemployment_rows = df[df['TARGET_PERIOD'].str.contains(f'^{target_period}$', regex=True)]

        # Ensure FCT_SOURCE is of type int to match the forecaster_ids DataFrame
        unemployment_rows.loc[:, 'FCT_SOURCE'] = unemployment_rows['FCT_SOURCE'].astype(int)

        # Create a DataFrame with all forecasters for the current period
        forecaster_ids = pd.DataFrame({'FCT_SOURCE': range(1, 151)})
        period_data = pd.DataFrame({'TARGET_PERIOD': [target_period] * 150})
        full_forecaster_df = pd.concat([forecaster_ids, period_data], axis=1)

        # Merge the filtered unemployment data with the full forecaster DataFrame
        unemployment_rows_full = pd.merge(full_forecaster_df, unemployment_rows, 
                                          on=['FCT_SOURCE', 'TARGET_PERIOD'], 
                                          how='left')

        # Concatenate to the main dataframe
        unemployment_data = pd.concat([unemployment_data, unemployment_rows_full], ignore_index=True)

    # Save the concatenated dataframe if it is not empty
    if not unemployment_data.empty:
        output_file = os.path.join(folder_path, 'UNEMPLOYMENT_SPF.csv')
        unemployment_data.to_csv(output_file, index=False)
        print(f"Data merged and saved to {output_file}")

# Example usage
# merge_datasets_unemployment('/path/to/folder')





def merge_inflation_datasets_ECB_Eurostat(true_inflation_path, spf_inflation_path, output_path):
    # Load the datasets
    true_inflation = pd.read_csv(true_inflation_path, delimiter=';', header=1)
    spf_inflation = pd.read_csv(spf_inflation_path)

    # Filter true_inflation to include only the months of interest
    months_of_interest = ['Dec', 'Mar', 'Jun', 'Sep']
    true_inflation_filtered = true_inflation[true_inflation['TIME PERIOD'].str.contains('|'.join(months_of_interest))]

    # Merge datasets on the "TARGET_PERIOD" and "TIME PERIOD"
    merged_data = pd.merge(spf_inflation, true_inflation_filtered, left_on='TARGET_PERIOD', right_on='TIME PERIOD', how='inner')

    # Select only the specified columns
    columns_to_keep = ['TARGET_PERIOD', 'FCT_SOURCE', 'POINT', 'TIME PERIOD', 'HICP - Overall index (ICP.M.U2.N.000000.4.ANR)']
    merged_data = merged_data[columns_to_keep]

    # Rename and drop columns as specified
    merged_data = merged_data.rename(columns={"HICP - Overall index (ICP.M.U2.N.000000.4.ANR)": "Observed_HICP_Inflation"})
    merged_data = merged_data.drop(columns=["TIME PERIOD"])

    # Save the merged data to a CSV file
    merged_data.to_csv(output_path, index=False)
    print(f"Data merged and saved to {output_path}")




def merge_GDP_datasets_ECB_Eurostat(growth_path, gdp_path, output_path):
    # Load the datasets
    growth_df = pd.read_csv(growth_path)
    gdp_df = pd.read_csv(gdp_path, delimiter=';')  # Using semicolon based on your data structure info

    # Merge datasets on the "TARGET_PERIOD" from growth data and "TIME PERIOD" from GDP data
    merged_data = pd.merge(growth_df, gdp_df, left_on='TARGET_PERIOD', right_on='TIME PERIOD', how='inner')

    # Define columns to keep, based on your provided headers
    columns_to_keep = [
        'TARGET_PERIOD', 'FCT_SOURCE', 'POINT', 'TIME PERIOD', 
        'Gross domestic product at market prices (MNA.Q.Y.I9.W2.S1.S1.B.B1GQ._Z._Z._Z.EUR.LR.GY)'
    ]
    
    # Select and rename columns as specified
    merged_data = merged_data[columns_to_keep]
    merged_data = merged_data.rename(columns={
        "Gross domestic product at market prices (MNA.Q.Y.I9.W2.S1.S1.B.B1GQ._Z._Z._Z.EUR.LR.GY)": "Observed_GDP_Growth"
    })
    merged_data = merged_data.drop(columns=["TIME PERIOD"])

    # Save the merged data to a CSV file
    merged_data.to_csv(output_path, index=False)
    print(f"Data merged and saved to {output_path}")
    
    
    

def merge_unemployment_datasets_SPF_Eurostat(SPF_unemployment, ECB_Unemployment, Output_File):
    # Load the datasets
    spf_unemployment = pd.read_csv(SPF_unemployment)
    ecb_unemployment = pd.read_csv(ECB_Unemployment)

    # Filter ECB unemployment to include only the months of interest
    months_of_interest = ['Nov', 'Feb', 'May', 'Aug']
    ecb_unemployment_filtered = ecb_unemployment[ecb_unemployment['TIME PERIOD'].str.contains('|'.join(months_of_interest))]

    # Merge datasets on the "TARGET_PERIOD" and "TIME PERIOD"
    merged_data = pd.merge(spf_unemployment, ecb_unemployment_filtered, left_on='TARGET_PERIOD', right_on='TIME PERIOD', how='inner')

    # Select only the specified columns
    columns_to_keep = [
        'TARGET_PERIOD', 'FCT_SOURCE', 'POINT', 'TIME PERIOD', 
        '(LFSI.M.I9.S.UNEHRT.TOTAL0.15_74.T)'
    ]
    merged_data = merged_data[columns_to_keep]

    # Optionally rename the unemployment rate column for clarity
    merged_data = merged_data.rename(columns={
        "(LFSI.M.I9.S.UNEHRT.TOTAL0.15_74.T)": "Observed_Unemployment_Rate"
    })
    
    # Drop the 'TIME PERIOD' column
    merged_data = merged_data.drop(columns=["TIME PERIOD"])

    # Save the merged data to a CSV file
    merged_data.to_csv(Output_File, index=False)
    print(f"Data merged and saved to {Output_File}")




if __name__ == "__main__":
    merge_datasets_inflation("/Users/emre/Desktop/Thesis_Code/SPF_individual_forecasts")
    merge_datasets_GDP('/Users/emre/Desktop/Thesis_Code/SPF_individual_forecasts')
    merge_datasets_unemployment('/Users/emre/Desktop/Thesis_Code/SPF_individual_forecasts')
    merge_inflation_datasets_ECB_Eurostat('/Users/emre/Desktop/Thesis_Code/HICP_Inflation_Monthly.csv', '/Users/emre/Desktop/Thesis_Code/SPF_individual_forecasts/Inflation_SPF.csv', '/Users/emre/Desktop/Thesis_Code/SPF_ECB_Inflation_MERGED.csv')
    merge_GDP_datasets_ECB_Eurostat("/Users/emre/Desktop/Thesis_Code/SPF_individual_forecasts/GDP_SPF.csv", "/Users/emre/Desktop/Thesis_Code/GDP_EuroStat.csv" , "/Users/emre/Desktop/Thesis_Code/SPF_ECB_GDP_MERGED.csv")
    merge_unemployment_datasets_SPF_Eurostat("/Users/emre/Desktop/Thesis_Code/SPF_individual_forecasts/UNEMPLOYMENT_SPF.csv", "/Users/emre/Desktop/Thesis_Code/ECB_Unemployment.csv","/Users/emre/Desktop/Thesis_Code/SPF_ECB_Unemployment_MERGED.csv")