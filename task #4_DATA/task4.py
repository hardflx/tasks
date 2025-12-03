import os
import pandas as pd
import re
from dateutil import parser
from itertools import combinations
import networkx as nx
import yaml

EXCHANGE_RATE_EUR_TO_USD = 1.2

def clean_timestamp(value):
    if pd.isna(value):
        return None
    v = str(value).strip()
    v = re.sub(r'\bA\.M\.\b', 'AM', v, flags=re.IGNORECASE)
    v = re.sub(r'\bP\.M\.\b', 'PM', v, flags=re.IGNORECASE)
    v = v.replace(';', ' ')
    v = re.sub(r'(\d{2}:\d{2}:\d{2}),', r'\1 ', v)
    v = v.replace(',', ' ')
    v = re.sub(r'\s+', ' ', v).strip()
    try:
        return parser.parse(v, dayfirst=True)
    except:
        return None

def clean_unit_price(value):
    if pd.isna(value):
        return None
    v = str(value).strip()
    lower = v.lower()
    is_euro = ('€' in v) or ('eur' in lower)
    is_usd = ('$' in v) or ('usd' in lower)
    if not (is_euro or is_usd):
        is_usd = True
    v = re.sub(r'(eur|usd|€|\$)', '', v, flags=re.IGNORECASE)
    v = v.replace('¢', '.')
    if v.count(',') == 1 and v.count('.') == 0:
        v = v.replace(',', '.')
    else:
        v = v.replace(',', '')
    v = re.sub(r'[^0-9.]+', '', v)
    if v.count('.') > 1:
        parts = v.split('.')
        v = parts[0] + '.' + ''.join(parts[1:])
    try:
        price = float(v)
    except:
        return None
    if is_euro:
        price *= EXCHANGE_RATE_EUR_TO_USD
    return float(f'{price:.2f}')

def process_users_orders(orders, users):
    orders["paid_price"] = orders["quantity"] * orders["unit_price"]
    orders_table = orders[['user_id','paid_price']].rename(columns={'user_id':'id'})
    users_table = users[['id','name','address','phone','email']]
    users_orders = orders_table.merge(users_table, on='id', how='left')
    
    fields = ['name','address','phone','email']
    users_orders[fields] = users_orders[fields].fillna('').astype(str)
    
    for combo in combinations(fields, 3):
        col_name = '_'.join(combo)
        def make_key(row):
            parts = [row[f] for f in combo if row[f].strip() != '']
            return '-'.join(parts)
        users_orders[col_name] = users_orders.apply(make_key, axis=1)
    
    key_columns = [col for col in users_orders.columns if '_' in col]
    melted = users_orders.melt(
        id_vars=['id','paid_price','name','address','phone','email'],
        value_vars=key_columns,
        value_name='key'
    )
    melted = melted[melted['key'] != '']
    
    G = nx.Graph()
    for ids in melted.groupby('key')['id']:
        id_list = list(ids[1])
        for i in range(len(id_list)):
            for j in range(i+1, len(id_list)):
                G.add_edge(id_list[i], id_list[j])
    
    components = list(nx.connected_components(G))
    
    final_rows = []
    for comp in components:
        comp_df = users_orders[users_orders['id'].isin(comp)]
        final_rows.append({
            'id': ','.join(sorted(set(comp_df['id'].astype(str)))),
            'name': ','.join(sorted(set(comp_df['name'].astype(str)))),
            'address': ','.join(sorted(set(comp_df['address'].astype(str)))),
            'phone': ','.join(sorted(set(comp_df['phone'].astype(str)))),
            'email': ','.join(sorted(set(comp_df['email'].astype(str)))),
            'paid_price': comp_df['paid_price'].sum()
        })
    
    result = pd.DataFrame(final_rows)
    result_sorted = result.sort_values(by='paid_price', ascending=False)
    unique_users_number = len(result_sorted)
    best_buyer = result_sorted.iloc[0].to_dict() if not result_sorted.empty else None
    return result_sorted, unique_users_number, best_buyer

def process_books_orders(orders, books_file):
    with open(books_file,'r',encoding='utf-8') as f:
        books_data = yaml.safe_load(f)
    books_table = pd.DataFrame(books_data).rename(columns={
        ':id':'id', ':title':'title', ':author':'author', ':genre':'genre',
        ':publisher':'publisher', ':year':'year'
    })
    books_table = books_table[['id','title','author','genre','publisher','year']]

    books_orders = orders[['book_id','quantity']].rename(columns={'book_id':'id'})
    books_orders_final = books_orders.merge(books_table, on='id', how='left')
    
    unique_authors = books_orders_final['author'].nunique()
    author_sales = books_orders_final.groupby('author')['quantity'].sum().reset_index()
    author_sales = author_sales.rename(columns={'quantity':'total_quantity'})
    author_sales_sorted = author_sales.sort_values(by='total_quantity', ascending=False)
    most_popular_author = author_sales_sorted.iloc[0]['author'] if not author_sales_sorted.empty else None
    
    return unique_authors, most_popular_author


def top_5_days(orders):
    if 'timestamp' not in orders.columns or orders['timestamp'].isna().all():
        return []
    orders['date'] = pd.to_datetime(orders['timestamp'], errors='coerce').dt.date
    top_days = orders.groupby('date')['paid_price'].sum().sort_values(ascending=False).head(5)
    return [str(d) for d in top_days.index]


def daily_revenue(orders):
    if 'timestamp' not in orders.columns or orders['timestamp'].isna().all():
        return pd.DataFrame(columns=['date','daily_revenue'])
    orders['date'] = pd.to_datetime(orders['timestamp'], errors='coerce').dt.date
    daily_rev = orders.groupby('date')['paid_price'].sum().reset_index()
    daily_rev = daily_rev.rename(columns={'paid_price':'daily_revenue'})
    daily_rev = daily_rev.sort_values(by='date')
    return daily_rev


def process_all_folders(base_path):
    for folder in os.listdir(base_path):
        folder_path = os.path.join(base_path, folder)
        if not os.path.isdir(folder_path):
            continue
        
        orders_path = os.path.join(folder_path,'orders.parquet')
        orders = pd.read_parquet(orders_path, engine='pyarrow')
        orders["timestamp"] = orders["timestamp"].apply(clean_timestamp)
        orders["unit_price"] = orders["unit_price"].apply(clean_unit_price)
        orders["paid_price"] = orders["quantity"] * orders["unit_price"]
        

        users_path = os.path.join(folder_path,'users.csv')
        users = pd.read_csv(users_path)
        

        users_result, unique_users_number, best_buyer = process_users_orders(orders, users)
        

        books_path = os.path.join(folder_path,'books.yaml')
        unique_authors, most_popular_author = process_books_orders(orders, books_path)
        

        top_days = top_5_days(orders)
        

        daily_rev_df = daily_revenue(orders)
        daily_csv = os.path.join(folder_path,'daily_revenue.csv')
        daily_rev_df.to_csv(daily_csv, index=False)
        print(f"daily_revenue saved in {folder_path}")
        
        summary = pd.DataFrame([{
            'top_5_days': ','.join(top_days),
            'unique_users_number': unique_users_number,
            'unique_authors': unique_authors,
            'most_popular_author': most_popular_author,
            'best_buyer': f"{best_buyer['name']} (Ids: {best_buyer['id']})" if best_buyer else None
        }])
        summary_csv = os.path.join(folder_path,'summary.csv')
        summary.to_csv(summary_csv, index=False)
        print(f"summary saved in {folder_path}")

base_path = r'C:/Users/hardf/Desktop/tasks/task #4_DATA/task_data'
process_all_folders(base_path)