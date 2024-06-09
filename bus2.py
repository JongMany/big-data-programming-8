import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from geopy.geocoders import Nominatim

geo_local = Nominatim(user_agent= 'South Korea', timeout=None)
df1 = pd.read_excel('./data/서울시버스노선별정류소정보(20240507).xlsx')
df2 = pd.read_excel('./저상버스.xlsx')

# Renaming columns to ensure they match for merging
df1.rename(columns={'노선명': 'Route Name'}, inplace=True)
df2.rename(columns={'노선번호': 'Route Name'}, inplace=True)

# Converting all values in 'Route Name' to string type
df1['Route Name'] = df1['Route Name'].astype(str)
df2['Route Name'] = df2['Route Name'].astype(str)

# Function to normalize the route names
def normalize_route(route_name):
    return ''.join([char for char in route_name if char.isdigit() or char.isalpha() and char.isascii()])

# Apply the normalization function to the 'Route Name' column in both dataframes
df2['Route Name'] = df2['Route Name'].apply(normalize_route)

merged_df = pd.merge(df1, df2, on='Route Name', how='left')

# Fill missing values with defaults
merged_df['인가대수'].fillna(0, inplace=True)
merged_df['저상대수'].fillna(0, inplace=True)
merged_df['보유율'].fillna(0, inplace=True)

# 캐시 저장소
cache = {}

def geocoding_reverse(lat, lng):
    if (lat, lng) in cache:
        return cache[(lat, lng)]
    try:
        address = geo_local.reverse([lat, lng], exactly_one=True, language='ko')
        detail_address = address.address  # 상세주소
        zip_code = address.raw['address']['postcode']  # 우편번호
        #         x_y = [detail_address, zip_code]
        return [i.strip() for i in detail_address.split(',')]
    #         return x_y
    except:
        return None

i = 0
def make_gu(lat, lng):
    m = geocoding_reverse(lat, lng)
    global i
    i += 1
    print(i, m)
    if m is None:
        return None
    try:
        idx = m.index('서울')
        if idx == -1:
            return None
        return m[idx - 1]
    except:
        return None

def process_row(row):
    return make_gu(row['Y좌표'], row['X좌표'])

print("m")
# Applying make_gu to find the district for each coordinate using ThreadPoolExecutor
# Using a larger thread pool for faster processing
max_workers = min(32, (len(merged_df) // 2) + 1)
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_index = {executor.submit(process_row, row): index for index, row in merged_df.iterrows()}
    for future in as_completed(future_to_index):
        index = future_to_index[future]
        try:
            result = future.result()
            merged_df.at[index, 'District'] = result
        except Exception as exc:
            print(f'Generated an exception: {exc}')

merged_df.to_excel('./merged_with_districts2.xlsx', index=False)
