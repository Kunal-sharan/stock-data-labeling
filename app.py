import streamlit as st
from streamlit_gsheets import GSheetsConnection
from sqlalchemy import create_engine, text
import plotly.graph_objects as go
import random
import pandas as pd
import time
import os
from dotenv import load_dotenv
load_dotenv()
avien_key = st.secrets["AVIEN_KEY"]
# st.write(avien_key)
@st.cache_data
def get_all_users():
    conn_users = st.connection('mysql', type='sql')
    df_users = conn_users.query("select username,password from users;", ttl=1)  
    return df_users  

st.set_page_config(layout="wide")

def get_user_id(user_name, password):
    conn_users = st.connection('mysql', type='sql')
    df_users = conn_users.query(f"select id from users where username = '{user_name}' and password = '{password}';", ttl=1)
    user_id = df_users['id'].tolist()[0]
    return user_id

def cache_entries(user_id):
    conn_sql = st.connection('mysql', type='sql')
    df = conn_sql.query(f'select stock_id from stock_data_label where user_id = {user_id};', ttl=1)
    cache_set = set(df['stock_id'].tolist())
    return cache_set

def plot_stock_chart(df, title="Stock Price Movement"):
    fig = go.Figure(data=[go.Candlestick(
        x=df["Date"],
        open=df["Open"],
        high=df["High"],
        low=df["Low"],
        close=df["Close"],
        name="Stock Price"
    )])
    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False,
        template="plotly_dark"
    )
    st.plotly_chart(fig)

# MySQL Database Connection
def get_connection():
    # return create_engine("mysql+pymysql://root:isagi11@localhost/mydb")
    return create_engine(f"mysql://avnadmin:{avien_key}@mysql-1409-kunalsharan1409-54fa.d.aivencloud.com:10676/defaultdb")

cache = set()
stock_entry = []
user_data = get_all_users()

@st.fragment
def insert_into_database(start_time, end_time, user_id, stock_end_id, df_main):
    trend = st.selectbox("What trend you see here?", ("Up trend", "Down trend", "Neutral/No trend"))
    submit = st.button("Submit")
    try:
        if trend and submit:
            st.write(start_time, end_time, trend)
            engine = get_connection()
            check_arr = cache_entries(user_id)
            # st.write(check_arr)
            # st.write(stock_entry)
            if stock_entry[-1] in check_arr:
                st.warning("Data Already Inserted. Click on 'Next Graph' to continue.")
                return
            with engine.connect() as conn:
                query = text("INSERT INTO stock_data_label (user_id, stock_id, startDate, endDate, trend, stock_id_end) VALUES (:user_id, :stock_id, :start_time, :end_time, :trend, :stock_end_id)")
                conn.execute(query, {"start_time": start_time, "end_time": end_time, "trend": trend, "stock_id": stock_entry[-1], "user_id": user_id, "stock_end_id": stock_end_id})
                conn.commit()
                st.success("Data Inserted")     
    except Exception as e:
        st.error(f"Error: {e}")

@st.fragment
def carousel_graph(df, user_id):
    # Initialize graph_index in session state if not set
    if "graph_index" not in st.session_state:
        st.session_state.graph_index = random.randint(0, len(df) - 30)
    
    # Use the current index to display a graph segment
    current_index = st.session_state.graph_index
    stock_entry.append(current_index)
    new_df = df.iloc[current_index:current_index+30]
    last_stock_id = current_index + len(new_df) - 1
    plot_stock_chart(new_df)
    start = new_df.iloc[0]['Date']
    end = new_df.iloc[-1]['Date']
    
    insert_into_database(start, end, user_id, last_stock_id, df)
    if st.button("Next Graph"):
        # Update index with a new random segment ensuring it doesn't overlap with previously used ones
        new_index = random.randint(0, len(df) - 30)
        # Ensure new_index is different than current (or you can implement more advanced checks)
        while new_index == current_index:
            new_index = random.randint(0, len(df) - 30)
        st.session_state.graph_index = new_index
        st.rerun()

def merge_intervals(arr):
  a = []
  arr.sort()
  first = arr[0][0]
  second = arr[0][1]
  i = 1
  
  while i < len(arr):
    if arr[i][0] <= second:
      if arr[i][1] > second:
        second = arr[i][1]
    else:
      a.append([first,second])
      first = arr[i][0]
      second = arr[i][1]
    i+=1
    
  a.append([first,second])
  return a


@st.fragment
def update_database(user_id, df):
    if st.button(f"Show the data base for user_id : {user_id}"):
        conn_sql = st.connection('mysql', type='sql')
        df_updated_db = conn_sql.query(f'select * from stock_data_label where user_id = {user_id};', ttl=1)
        stock_overlapping_intervals = []
        for i in range(len(df_updated_db)):
            stock_overlapping_intervals.append([df_updated_db.iloc[i]["stock_id"], df_updated_db.iloc[i]["stock_id_end"]])
        if len(stock_overlapping_intervals) == 0:
            st.warning("No data available")
            return
        m_intervals = merge_intervals(stock_overlapping_intervals)
        st.write(m_intervals)
        c = 0
        for intervals in m_intervals:
            st.write(intervals)
            c+=intervals[1]-intervals[0]+1
            st.write(c)
        progress_text = f"{c} out of {len(df)} labelled: {len(df)-c} left"
        my_bar = st.progress(0, text=progress_text)
        completetion = (c/len(df)) * 100
        directory_name = f"{user_id}_labels"
        try:
            os.mkdir(directory_name)
            print(f"Directory '{directory_name}' created successfully.")
        except FileExistsError:
            print(f"Directory '{directory_name}' already exists.")
        except PermissionError:
            print(f"Permission denied: Unable to create '{directory_name}'.")
        except Exception as e:
            print(f"An error occurred: {e}")
        df_updated_db.to_csv(f"{user_id}_labels/{user_id}_3IINFOLTD.NS_label.csv")
        for percent_complete in range(int(completetion + 1)):
            time.sleep(0.02)
            my_bar.progress(percent_complete + 1, text=progress_text)
        n_df = pd.read_csv(f"{user_id}_labels/{user_id}_3IINFOLTD.NS_label.csv")
        st.write(n_df)

if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

def login(username, password):
    for i in range(len(user_data)):
        if user_data['username'][i] == username and user_data['password'][i] == password:
            return True
    return False

def logout():
    st.session_state['logged_in'] = False
    st.rerun()

if not st.session_state['logged_in']:
    st.title("Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    st.session_state['password'] = password
    st.session_state['username'] = username
    if st.button("Login"):
        if login(username, password):
            st.session_state['logged_in'] = True
            st.success("Logged in successfully!")
            st.rerun()
        else:
            st.error("Invalid username or password")
else:
    st.title("Welcome!")
    st.write("You are logged in.")
    st.write(st.session_state.password, st.session_state.username)
    p = st.session_state.password
    u = st.session_state.username
    df = pd.read_csv("Data_to_label/3IINFOLTD.NS.csv")
    st.write(df)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].dt.date
    conn_user_table = st.connection("mysql", type='sql')
    user_id = get_user_id(user_name=u, password=p)
    cache = cache_entries(user_id)
    if len(cache) == 0:
        cache = set()
    
    # Display the carousel graph (with navigation controls below)
    carousel_graph(df, user_id)
    
    # Update database and show data
    update_database(user_id, df)
    
    if st.button("Logout"):
        logout()
