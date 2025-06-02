import requests
import subprocess
import pandas as pd
import numpy as np
import os
import json
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import streamlit as st
import plotly.express as px

#Converting data
def convert_to_crore(number):
    crore = number/10000000
    return round(crore,2)

def convert_to_lakh(number):
    lakh = number/100000
    return round(lakh,2)

def convert_to_thousand(number):
    thousand = number/1000
    return round(thousand,2)

conn = pymysql.connect(host = 'localhost',user = 'root', password = 'B@1998', db= 'phonepe_pulse')
cursor = conn.cursor()

#creating streamlit dashboard
st.set_page_config(layout = 'wide')

st.header(':violet[Phonepe Pulse Data Visualization]')
st.write('**Note**:- This data is between **2018** to **2024(Quarter 2)** in **INDIA**')

option = st.radio('**Select your option**',('All India', 'State wise', 'Top Ten Categories', 'District-wise Analysis'), horizontal = True)

if option == 'All India':

    tab1, tab2 = st.tabs(['Transaction','User'])

    with tab1:
        col1,col2,col3 = st.columns(3)
        with col1:
            in_tr_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023','2024'), key = 'in_tr_yr')
        with col2:
            in_tr_qtr= st.selectbox('**Select Quarter**',('1','2','3','4'), key = 'in_tr_qtr')
        with col3:
            in_tr_tr_typ = st.selectbox('**Select Transaction Type**', ('Recharge & Bill Payments','Peer-to-Peer Payments', 'Merchant Payments', 'Financial Services', 'Others'), key = 'in_tr_tr_typ')
        #Transaction Analysis bar chart query
        cursor.execute(f"SELECT State, Transaction_amount FROM aggregated_transaction WHERE Year ='{in_tr_yr}' AND Quarter= '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
        in_tr_tab_qry_rslt = cursor.fetchall()
        df_in_tr_tab_qry_rslt = pd.DataFrame(np.array(in_tr_tab_qry_rslt), columns = ['State', 'Transaction_amount'])
        df_in_tr_tab_qry_rslt1 = df_in_tr_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_tr_tab_qry_rslt)+1)))

        #Transaction Analysis table query
        cursor.execute(f"SELECT State, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter= '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
        in_tr_anly_tab_qry_rslt = cursor.fetchall()
        df_in_tr_anly_tab_qry_rslt = pd.DataFrame(np.array( in_tr_anly_tab_qry_rslt), columns = ['State', 'Transaction_count', 'Transaction_amount'])
        df_in_tr_anly_tab_qry_rslt1 = df_in_tr_anly_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_tr_anly_tab_qry_rslt)+1)))

        #Total Transaction Amount table query
        cursor.execute(f" SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter= '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
        in_tr_amt_qry_rslt = cursor.fetchall()
        df_in_tr_amt_qry_rslt = pd.DataFrame(np.array(in_tr_amt_qry_rslt), columns = ['Total', 'Average'])
        df_in_tr_amt_qry_rslt1 = df_in_tr_amt_qry_rslt.set_index(['Average'])

        # Total Transaction Count Table Query
        cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE Year = '{in_tr_yr}' AND Quarter= '{in_tr_qtr}' AND Transaction_type = '{in_tr_tr_typ}';")
        in_tr_cnt_qry_rslt = cursor.fetchall()
        df_in_tr_cnt_qry_rslt = pd.DataFrame(np.array(in_tr_cnt_qry_rslt), columns = ['Total', 'Average'])
        df_in_tr_cnt_qry_rslt1 = df_in_tr_cnt_qry_rslt.set_index(['Average'])
        
        
        #Output
        tab1_1, tab1_2, tab1_3 = st.tabs(['Geo Visualization', 'Bar Chart', 'Total Calculation'])

        with tab1_1:
            #Geovisualization dashboard for transaction
            #dropping state column from transaction table query result
            df_in_tr_tab_qry_rslt.drop(columns = ['State'], inplace = True)

            #cloning geo data
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data1 = json.loads(response.content)

            #extracting state names and sorting them in alphabetical order
            state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
            state_names_tra.sort()

            #Creating data frame with the state names column
            df_state_names_tra = pd.DataFrame({'State': state_names_tra})

            #Combining gio state name with df_in_tr_tab_qry_rslt
            df_state_names_tra['Transaction_amount'] = df_in_tr_tab_qry_rslt
            

            #convert dataframe to csv
            df_state_names_tra.to_csv('State_transaction.csv', index = False)

            #read csv
            df_tra = pd.read_csv('State_transaction.csv')                      

            fig_tra = px.choropleth( df_tra, geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson", 
                                    featureidkey = 'properties.ST_NM', locations = 'State', color = 'Transaction_amount', color_continuous_scale = 'thermal', title = 'Transaction Analysis')
            fig_tra.update_geos(fitbounds = 'locations', visible = False)
            fig_tra.update_layout(title_font = dict(size = 33), title_font_color = '#6739b7', height = 800)
            st.plotly_chart(fig_tra, use_container_width = True)

        with tab1_2:
            # All India Transaction Analysis Bar Chart
            df_in_tr_tab_qry_rslt1['State'] = df_in_tr_tab_qry_rslt1['State'].astype(str)
            df_in_tr_tab_qry_rslt1['Transaction_amount'] =df_in_tr_tab_qry_rslt1['Transaction_amount'].astype(float).apply(convert_to_crore)
            df_in_tr_tab_qry_rslt1_fig = px.bar(df_in_tr_tab_qry_rslt1, x= 'State', y= 'Transaction_amount', color= 'Transaction_amount', color_continuous_scale= 'thermal', title= 'Transaction Analysis Chart', height= 700)
            df_in_tr_tab_qry_rslt1_fig.update_layout(title_font = dict(size = 33), title_font_color = '#6739b7')
            st.plotly_chart(df_in_tr_tab_qry_rslt1_fig, use_container_width = True)

        with tab1_3:
            # All India total transaction calculation table
            st.header(':violet[Total Calculation]')

            col4, col5 = st.columns(2)
            with col4:
                st.subheader('Transaction Analysis')
                st.dataframe(df_in_tr_anly_tab_qry_rslt1)
            with col5:
                st.subheader('Transaction Amount')
                st.dataframe(df_in_tr_amt_qry_rslt1)
                st.subheader('Transaction Count')
                st.dataframe(df_in_tr_cnt_qry_rslt1)
    # user Analysis
    with tab2:
        col_1, col_2 = st.columns(2)
        with col_1:
            in_us_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022'), key='in_us_yr')
        with col_2:
            in_us_qtr = st.selectbox('**Select Quarter**', ('1','2','3','4'), key = 'in_us_qtr')
            #if in_us_yr == '2022' and in_us_qtr == '2' or '3' or '4':
                #with col5:
                    #st.header('No New Users')
        tab2_1, tab2_2, tab2_3, tab2_4 = st.tabs(['Geo Visualization', 'Bar Chart', 'Total Calculation', 'Brandwise Users'])


        with tab2_1:
            try:
                cursor.execute(f"SELECT State, SUM(Count) FROM aggregated_user WHERE Year= '{in_us_yr}' AND Quarter= '{in_us_qtr}' GROUP BY State;")
                in_us_tab_qry_rslt = cursor.fetchall()
                df_in_us_tab_qry_rslt = pd.DataFrame(np.array(in_us_tab_qry_rslt),columns=['State','User Count'])
                df_in_us_tab_qry_rslt1 = df_in_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_us_tab_qry_rslt)+1)))
                # Output
                #Geo Visualization dashboard for user
                # Dropping state column from df_in_us_tab_qry_rslt
                df_in_us_tab_qry_rslt.drop(columns= ['State'], inplace= True)
                #Cloning geo data
                url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
                response = requests.get(url)
                data2 = json.loads(response.content)

                #Extract State names and sorting them in alphabetical order
                state_names_usr = [feature['properties']['ST_NM'] for feature in data2['features']]
                state_names_usr.sort()

                #Creating data frame with the state names column
                df_state_names_usr = pd.DataFrame({'State': state_names_usr})

                #Combining gio state name with df_in_tr_tab_qry_rslt
                df_state_names_usr['User Count'] = df_in_us_tab_qry_rslt
                

                #convert dataframe to csv
                df_state_names_usr.to_csv('State_Users.csv', index = False)

                #read csv
                df_usr = pd.read_csv('State_Users.csv')

                #Geo plot
                fig_usr = px.choropleth( df_usr, geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson", 
                                            featureidkey = 'properties.ST_NM', locations = 'State', color = 'User Count', color_continuous_scale = 'thermal', title = 'User Analysis')
                fig_usr.update_geos(fitbounds = 'locations', visible = False)
                fig_usr.update_layout(title_font = dict(size = 33), title_font_color = '#6739b7', height = 800)
                st.plotly_chart(fig_usr, use_container_width = True)
            except:
                st.error('No Registrations')

        with tab2_2:
            # user Analysis bar chart query
            try:
                cursor.execute(f"SELECT State, SUM(Count) FROM aggregated_user WHERE Year= '{in_us_yr}' AND Quarter= '{in_us_qtr}' GROUP BY State;")
                in_us_tab_qry_rslt = cursor.fetchall()
                df_in_us_tab_qry_rslt = pd.DataFrame(np.array(in_us_tab_qry_rslt),columns=['State','User Count'])
                df_in_us_tab_qry_rslt1 = df_in_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_us_tab_qry_rslt)+1)))

                #Output for all India User Analysis Bar chart
                df_in_us_tab_qry_rslt1['State'] = df_in_us_tab_qry_rslt1['State'].astype(str)
                df_in_us_tab_qry_rslt1['User Count']= df_in_us_tab_qry_rslt1['User Count'].astype(int).apply(convert_to_lakh)
                df_in_us_tab_qry_rslt1_fig = px.bar(df_in_us_tab_qry_rslt1, x= 'State', y='User Count', color= 'User Count', color_continuous_scale = 'thermal', title = 'User Analysis Chart', height = 700)
                df_in_us_tab_qry_rslt1_fig.update_layout(title_font= dict(size=33), title_font_color= '#6739b7')
                st.plotly_chart(df_in_us_tab_qry_rslt1_fig, use_container_width = True)

            except:
                st.error('No Registrations')    

        with tab2_3:
            try:
                #Total user count table query
                cursor.execute(f"SELECT SUM(Count), AVG(Count) FROM aggregated_user WHERE Year= '{in_us_yr}' AND Quarter= '{in_us_qtr}';")
                in_us_cnt_qry_rslt = cursor.fetchall()
                df_in_us_cnt_qry_rslt = pd.DataFrame(np.array(in_us_cnt_qry_rslt), columns = ['Total','Average'])
                df_in_us_cnt_qry_rslt1 = df_in_us_cnt_qry_rslt.set_index(['Average'])

                # All India Total User calculation table
                st.header(':violet[Total Calculation]')

                col3, col4 = st.columns(2)
                with col3:
                    st.subheader('User Analysis State Wise')
                    st.dataframe(df_in_us_tab_qry_rslt1)

                with col4:
                    st.subheader('Aggregated User Count')
                    st.dataframe(df_in_us_cnt_qry_rslt1)

            except:
                st.error('No Registrations')  
        
        


        with tab2_4:
            try:
                cursor.execute(f"SELECT Brands, SUM(Count) FROM aggregated_user WHERE Year= '{in_us_yr}' AND Quarter= '{in_us_qtr}' GROUP BY Brands;")
                in_us_brnd_tab_qry_rslt = cursor.fetchall()
                df_in_us_brnd_tab_qry_rslt = pd.DataFrame(np.array(in_us_brnd_tab_qry_rslt),columns=['Brands','User Count'])
                df_in_us_brnd_tab_qry_rslt1 = df_in_us_brnd_tab_qry_rslt.set_index(pd.Index(range(1, len(df_in_us_brnd_tab_qry_rslt)+1)))

                #Output for all India User Analysis Bar chart
                df_in_us_brnd_tab_qry_rslt1['Brands'] = df_in_us_brnd_tab_qry_rslt1['Brands'].astype(str)
                df_in_us_brnd_tab_qry_rslt1['User Count']= df_in_us_brnd_tab_qry_rslt1['User Count'].astype(int).apply(convert_to_lakh)
                df_in_us_brnd_tab_qry_rslt1_fig = px.bar(df_in_us_brnd_tab_qry_rslt1, x= 'Brands', y='User Count', color= 'User Count', color_continuous_scale = 'thermal', title = 'Brand User Analysis Chart', height = 700)
                df_in_us_brnd_tab_qry_rslt1_fig.update_layout(title_font= dict(size=33), title_font_color= '#6739b7')
                st.plotly_chart(df_in_us_brnd_tab_qry_rslt1_fig, use_container_width = True)

            except:
                st.error('No Registrations')


# State wise Analysis
elif option == 'State wise':
    #Select tabs
    tab3, tab4 = st.tabs(['Transaction', 'User'])
    #State wise transaction
    with tab3:
        col1, col2, col3= st.columns(3)
        with col1:
            st_transactions = st.selectbox('**Select State**',('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh','assam', 'bihar', 
            'chandigarh', 'chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 
            'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh','maharashtra', 'manipur', 
            'meghalaya', 'mizoram', 'nagaland','odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana', 
            'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'),key='st_tr_st')
        with col2:
            st_tr_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023','2024'), key='st_tr_yr')
        with col3:
            st_tr_qtr= st.selectbox('**Select Quarter**', ('1','2','3','4'), key= 'st_tr_qtr')
        #SQL query
        tab_a, tab_b = st.tabs(['Bar Chart','Total Calculation'])

        with tab_a:
            try:
                #SQL Query
                #Transaction Analysis Bar chart
                cursor.execute(f"SELECT Transaction_type, Transaction_amount FROM aggregated_transaction WHERE State='{st_transactions}' AND Year= '{st_tr_yr}' AND Quarter= '{st_tr_qtr}';")
                st_tr_tab_bar_qry_rslt= cursor.fetchall()
                df_st_tr_tab_bar_qry_rslt= pd.DataFrame(np.array(st_tr_tab_bar_qry_rslt), columns=['Transaction_type','Transaction_amount'])
                df_st_tr_tab_bar_qry_rslt1= df_st_tr_tab_bar_qry_rslt.set_index(pd.Index(range(1, len(df_st_tr_tab_bar_qry_rslt)+1)))

                #Output
                #Statewise transaction analysis bar chart
                df_st_tr_tab_bar_qry_rslt1['Transaction_type'] = df_st_tr_tab_bar_qry_rslt1['Transaction_type'].astype(str)
                df_st_tr_tab_bar_qry_rslt1['Transaction_amount'] = df_st_tr_tab_bar_qry_rslt1['Transaction_amount'].astype(float).apply(convert_to_crore)
                df_st_tr_tab_bar_qry_rslt1_fig = px.bar(df_st_tr_tab_bar_qry_rslt1 , x = 'Transaction_type', y ='Transaction_amount', color ='Transaction_amount', color_continuous_scale = 'thermal', title = 'Transaction Analysis Chart', height = 500,)
                df_st_tr_tab_bar_qry_rslt1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
                st.plotly_chart(df_st_tr_tab_bar_qry_rslt1_fig,use_container_width=True)

            except:
                st.error("No Registrations")
            
        

        with tab_b:
            try:
                #Transaction Analysis table query
                cursor.execute(f"SELECT Transaction_type, Transaction_count, Transaction_amount FROM aggregated_transaction WHERE State= '{st_transactions}' AND Year='{st_tr_yr}' AND Quarter= '{st_tr_qtr}';")
                st_tr_anly_qry_rslt = cursor.fetchall()
                df_st_tr_anly_qry_rslt= pd.DataFrame(np.array(st_tr_anly_qry_rslt), columns= ['Transaction_type','Transaction_count','Transaction_amount'])
                df_st_tr_anly_qry_rslt1 = df_st_tr_anly_qry_rslt.set_index(pd.Index(range(1, len(df_st_tr_anly_qry_rslt)+1)))

                #Transaction amount table query
                cursor.execute(f"SELECT SUM(Transaction_amount), AVG(Transaction_amount) FROM aggregated_transaction WHERE State = '{st_transactions}' AND Year = '{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
                st_tr_amt_qry_rslt = cursor.fetchall()
                df_st_tr_amt_qry_rslt = pd.DataFrame(np.array(st_tr_amt_qry_rslt), columns=['Total','Average'])
                df_st_tr_amt_qry_rslt1 = df_st_tr_amt_qry_rslt.set_index(['Average'])
                
                # Total Transaction Count table query
                cursor.execute(f"SELECT SUM(Transaction_count), AVG(Transaction_count) FROM aggregated_transaction WHERE State = '{st_transactions}' AND Year ='{st_tr_yr}' AND Quarter = '{st_tr_qtr}';")
                st_tr_cnt_qry_rslt = cursor.fetchall()
                df_st_tr_cnt_qry_rslt = pd.DataFrame(np.array(st_tr_cnt_qry_rslt), columns=['Total','Average'])
                df_st_tr_cnt_qry_rslt1 = df_st_tr_cnt_qry_rslt.set_index(['Average'])

                #Output
                # ------  /  State wise Total Transaction calculation Table  /  ---- #
                st.header(':violet[Total calculation]')

                col4, col5 = st.columns(2)
                with col4:
                    st.subheader('Transaction Analysis')
                    st.dataframe(df_st_tr_anly_qry_rslt1)
                with col5:
                    st.subheader('Transaction Amount')
                    st.dataframe(df_st_tr_amt_qry_rslt1)
                    st.subheader('Transaction Count')
                    st.dataframe(df_st_tr_cnt_qry_rslt1)

            except:
                st.error("No Registrations")


        #Output
        #tab_a, tab_b = st.tabs(['Bar Chart','Total Calculation'])

        # with tab_a:
        #     #Statewise transaction analysis bar chart
        #     df_st_tr_tab_bar_qry_rslt1['Transaction_type'] = df_st_tr_tab_bar_qry_rslt1['Transaction_type'].astype(str)
        #     df_st_tr_tab_bar_qry_rslt1['Transaction_amount'] = df_st_tr_tab_bar_qry_rslt1['Transaction_amount'].astype(float).apply(convert_to_crore)
        #     df_st_tr_tab_bar_qry_rslt1_fig = px.bar(df_st_tr_tab_bar_qry_rslt1 , x = 'Transaction_type', y ='Transaction_amount', color ='Transaction_amount', color_continuous_scale = 'thermal', title = 'Transaction Analysis Chart', height = 500,)
        #     df_st_tr_tab_bar_qry_rslt1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
        #     st.plotly_chart(df_st_tr_tab_bar_qry_rslt1_fig,use_container_width=True)

        #with tab_b:
            #  # ------  /  State wise Total Transaction calculation Table  /  ---- #
            # st.header(':violet[Total calculation]')

            # col4, col5 = st.columns(2)
            # with col4:
            #     st.subheader('Transaction Analysis')
            #     st.dataframe(df_st_tr_anly_qry_rslt1)
            # with col5:
            #     st.subheader('Transaction Amount')
            #     st.dataframe(df_st_tr_amt_qry_rslt1)
            #     st.subheader('Transaction Count')
            #     st.dataframe(df_st_tr_cnt_qry_rslt1)
    # State wise user

    with tab4:
        col5, col6= st.columns(2)
        with col5:
            st_us_st= st.selectbox('**Select State**',('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh','assam', 'bihar', 
            'chandigarh', 'chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 
            'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh','maharashtra', 'manipur', 
            'meghalaya', 'mizoram', 'nagaland','odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana', 
            'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'),key='st_us_st')
        with col6:
            st_us_yr = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022'),key='st_us_yr')

        # SQL Query
        tab_c, tab_d, tab_e = st.tabs(['Bar Chart','Total Calculation','Brand Share'])

        with tab_c:
            try:
                cursor.execute(f"SELECT Quarter, SUM(Count) FROM aggregated_user WHERE State = '{st_us_st}' AND Year = '{st_us_yr}' GROUP BY Quarter;")
                st_us_tab_qry_rslt = cursor.fetchall()
                df_st_us_tab_qry_rslt = pd.DataFrame(np.array(st_us_tab_qry_rslt), columns=['Quarter', 'User Count'])
                df_st_us_tab_qry_rslt1 = df_st_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_st_us_tab_qry_rslt)+1)))

                # ---------  /  Output  /  -------- #

                # -----   /   All India User Analysis Bar chart   /   ----- #
                df_st_us_tab_qry_rslt1['Quarter'] = df_st_us_tab_qry_rslt1['Quarter'].astype(int)
                df_st_us_tab_qry_rslt1['User Count'] = df_st_us_tab_qry_rslt1['User Count'].astype(int)
                df_st_us_tab_qry_rslt1_fig = px.bar(df_st_us_tab_qry_rslt1 , x = 'Quarter', y ='User Count', color ='User Count', color_continuous_scale = 'thermal', title = 'User Analysis Chart', height = 500,)
                df_st_us_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
                st.plotly_chart(df_st_us_tab_qry_rslt1_fig,use_container_width=True)
            
            except:
                st.error("No Registrations ") 

        with tab_d:
            try:
                cursor.execute(f"SELECT Quarter, SUM(Count) FROM aggregated_user WHERE State = '{st_us_st}' AND Year = '{st_us_yr}' GROUP BY Quarter;")
                st_us_tab_qry_rslt = cursor.fetchall()
                df_st_us_tab_qry_rslt = pd.DataFrame(np.array(st_us_tab_qry_rslt), columns=['Quarter', 'User Count'])
                df_st_us_tab_qry_rslt1 = df_st_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_st_us_tab_qry_rslt)+1)))

                # Total User Count table query
                cursor.execute(f"SELECT SUM(Count), AVG(Count) FROM aggregated_user WHERE State = '{st_us_st}' AND Year = '{st_us_yr}';")
                st_us_co_qry_rslt = cursor.fetchall()
                df_st_us_co_qry_rslt = pd.DataFrame(np.array(st_us_co_qry_rslt), columns=['Total','Average'])
                df_st_us_co_qry_rslt1 = df_st_us_co_qry_rslt.set_index(['Average'])

            # ------    /   State wise User Total User calculation Table   /   -----#
                st.header(':violet[Total calculation]')

                col3, col4 = st.columns(2)
                with col3:
                    st.subheader('User Analysis')
                    st.dataframe(df_st_us_tab_qry_rslt1)
                with col4:
                    st.subheader('User Count')
                    st.dataframe(df_st_us_co_qry_rslt1)

            except:
                st.error("No Registrations")

        with tab_e:
            try:
                cursor.execute(f"SELECT Brands, SUM(Count) FROM aggregated_user WHERE State = '{st_us_st}' AND Year = '{st_us_yr}' GROUP BY Brands;")
                st_us_brnd_tab_qry_rslt = cursor.fetchall()
                df_st_us_brnd_tab_qry_rslt = pd.DataFrame(np.array(st_us_brnd_tab_qry_rslt), columns=['Brands', 'User Count'])
                df_st_us_brnd_tab_qry_rslt1 = df_st_us_brnd_tab_qry_rslt.set_index(pd.Index(range(1, len(df_st_us_brnd_tab_qry_rslt)+1)))

                # ---------  /  Output  /  -------- #

                # -----   /   All India User Analysis Bar chart   /   ----- #
                df_st_us_brnd_tab_qry_rslt1['Brands'] = df_st_us_brnd_tab_qry_rslt1['Brands'].astype(str)
                df_st_us_brnd_tab_qry_rslt1['User Count'] = df_st_us_brnd_tab_qry_rslt1['User Count'].astype(int)
                df_st_us_brnd_tab_qry_rslt1_fig = px.bar(df_st_us_brnd_tab_qry_rslt1 , x = 'Brands', y ='User Count', color ='User Count', color_continuous_scale = 'thermal', title = 'Brand Analysis Chart', height = 500,)
                df_st_us_brnd_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
                st.plotly_chart(df_st_us_brnd_tab_qry_rslt1_fig,use_container_width=True)
            
            except:
                st.error("No Registrations")
# Top categories
elif option == 'Top Ten Categories':
    #select tabs
    tab5, tab6 = st.tabs(['Transaction','User'])
    # All India Top Transactions
    with tab5:
        top_tr_yr = st.selectbox('**Select Year**',('2018','2019','2020','2021','2022','2023','2024'), key= 'top_tr_yr')

        #SQL Query
        # Top Transaction Bar chart query
        cursor.execute(f"SELECT State, SUM(Transaction_amount) As Transaction_amount FROM top_transaction WHERE Year = '{top_tr_yr}' GROUP BY State ORDER BY Transaction_amount DESC LIMIT 10;")
        top_tr_tab_qry_rslt = cursor.fetchall()
        df_top_tr_tab_qry_rslt = pd.DataFrame(np.array(top_tr_tab_qry_rslt), columns=['State', 'Top Transaction amount'])
        df_top_tr_tab_qry_rslt1 = df_top_tr_tab_qry_rslt.set_index(pd.Index(range(1, len(df_top_tr_tab_qry_rslt)+1)))

        # Top Transaction Analysis table query
        cursor.execute(f"SELECT State, SUM(Transaction_amount) as Transaction_amount, SUM(Transaction_count) as Transaction_count FROM top_transaction WHERE Year = '{top_tr_yr}' GROUP BY State ORDER BY Transaction_amount DESC LIMIT 10;")
        top_tr_anly_tab_qry_rslt = cursor.fetchall()
        df_top_tr_anly_tab_qry_rslt = pd.DataFrame(np.array(top_tr_anly_tab_qry_rslt), columns=['State', 'Top Transaction amount','Total Transaction count'])
        df_top_tr_anly_tab_qry_rslt1 = df_top_tr_anly_tab_qry_rslt.set_index(pd.Index(range(1, len(df_top_tr_anly_tab_qry_rslt)+1)))

        #Output
        tab5_1, tab5_2= st.tabs(['Bar Chart', 'Top 10'])

        with tab5_1:
            #All India Transaction Analysis Bar chart
            df_top_tr_tab_qry_rslt1['State'] = df_top_tr_tab_qry_rslt1['State'].astype(str)
            df_top_tr_tab_qry_rslt1['Top Transaction amount'] = df_top_tr_tab_qry_rslt1['Top Transaction amount'].astype(float).apply(convert_to_crore)
            df_top_tr_tab_qry_rslt1_fig = px.bar(df_top_tr_tab_qry_rslt1 , x = 'State', y ='Top Transaction amount', color ='Top Transaction amount', color_continuous_scale = 'thermal', title = 'Top Transaction Analysis Chart', height = 600,)
            df_top_tr_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
            st.plotly_chart(df_top_tr_tab_qry_rslt1_fig,use_container_width=True)

        with tab5_2:
            # All India Total Transaction calculation
            st.subheader('Top Transaction Analysis')
            st.dataframe(df_top_tr_anly_tab_qry_rslt1)

    # All India Top User
    with tab6:
        top_us_yr= st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022','2023','2024'),key='top_us_yr')

        #SQL query
        # Top users analysis bar chart query
        cursor.execute(f"SELECT State, SUM(Registered_Users) AS Top_user FROM top_user WHERE Year='{top_us_yr}' GROUP BY State ORDER BY Top_user DESC LIMIT 10;")
        top_us_tab_qry_rslt = cursor.fetchall()
        df_top_us_tab_qry_rslt = pd.DataFrame(np.array(top_us_tab_qry_rslt), columns=['State', 'Total User count'])
        df_top_us_tab_qry_rslt1 = df_top_us_tab_qry_rslt.set_index(pd.Index(range(1, len(df_top_us_tab_qry_rslt)+1)))

        # Output
        tab6_1, tab6_2 = st.tabs(['Bar Chart','Top 10'])

        with tab6_1:
            # Analysis of all India Bar chart
            df_top_us_tab_qry_rslt1['State'] = df_top_us_tab_qry_rslt1['State'].astype(str)
            df_top_us_tab_qry_rslt1['Total User count'] = df_top_us_tab_qry_rslt1['Total User count'].astype(float).apply(convert_to_lakh)
            df_top_us_tab_qry_rslt1_fig = px.bar(df_top_us_tab_qry_rslt1 , x = 'State', y ='Total User count', color ='Total User count', color_continuous_scale = 'thermal', title = 'Top User Analysis Chart', height = 600,)
            df_top_us_tab_qry_rslt1_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
            st.plotly_chart(df_top_us_tab_qry_rslt1_fig,use_container_width=True)

        with tab6_2:
            # All India Total transaction calculation table
            st.header(':violet[Total calculation]')
            st.subheader('Total User Analysis')
            st.dataframe(df_top_us_tab_qry_rslt1)

# District wise Analysis
else:
    #select tab
    tab7, tab8 = st.tabs(['Transaction','User'])

    # map transaction
    with tab7:
        col1, col2, col3 = st.columns(3)
        with col1:
            map_tr_st = st.selectbox('**Select State**',('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh','assam', 'bihar', 
            'chandigarh', 'chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 
            'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh','maharashtra', 'manipur', 
            'meghalaya', 'mizoram', 'nagaland','odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana', 
            'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'),key='map_tr_st')
        with col2:
            map_tr_yr= st.selectbox('**Select Year**',('2018','2019','2020','2021','2022','2023','2024'), key='map_tr_yr')
        with col3:
            map_tr_qtr = st.selectbox('**Select Quarter**',('1','2','3','4'), key='map_tr_qtr')
            #if map_tr_yr == '2024' and (map_tr_qtr == '3' or map_tr_qtr == '4'):
                #st.header('Data available till 2024 quarter 2')

        #SQL Query
        tab7_1, tab7_2 = st.tabs(['Bar Chart', 'Total Calculation'])

        with tab7_1:
            try:
                cursor.execute(f"SELECT District, Amount FROM map_transaction WHERE State='{map_tr_st}' AND Year='{map_tr_yr}' AND Quarter='{map_tr_qtr}';")
                map_tr_bar_qry_rslt = cursor.fetchall()
                if not map_tr_bar_qry_rslt:
                    st.warning("No data found for the selected inputs.")
                    raise ValueError("Empty result set")
                df_map_tr_bar_qry_rslt= pd.DataFrame(np.array(map_tr_bar_qry_rslt), columns=['District','Amount'])
                df_map_tr_bar_qry_rslt1= df_map_tr_bar_qry_rslt.set_index(pd.Index(range(1,len(df_map_tr_bar_qry_rslt)+1)))

                # output

                df_map_tr_bar_qry_rslt1['District']= df_map_tr_bar_qry_rslt1['District'].astype(str)
                df_map_tr_bar_qry_rslt1['Amount']= df_map_tr_bar_qry_rslt1['Amount'].astype(float).apply(convert_to_lakh)
                df_map_tr_bar_qry_rslt1_fig = px.bar(df_map_tr_bar_qry_rslt1, x= 'District', y='Amount', color='Amount', color_continuous_scale= 'thermal', title='District Wise Transaction Analysis Chart', height= 700)
                df_map_tr_bar_qry_rslt1_fig.update_layout(title_font= dict(size=33),title_font_color='#6739b7')
                st.plotly_chart(df_map_tr_bar_qry_rslt1_fig, use_container_width= True)
            except:
                st.error('Data Available till 2024 Quarter 2')

        # with tab7_1:
        #     try:
        #         st.write("Running SQL query...")
        #         cursor.execute(f"SELECT District, Amount FROM map_transaction WHERE State='{map_tr_st}' AND Year='{map_tr_yr}' AND Quarter='{map_tr_qtr}';")
        #         map_tr_bar_qry_rslt = cursor.fetchall()
        #         st.write("Query result:", map_tr_bar_qry_rslt)

        #         st.write("Creating DataFrame...")
        #         df_map_tr_bar_qry_rslt = pd.DataFrame(np.array(map_tr_bar_qry_rslt), columns=['District','Amount'])
        #         st.write("DataFrame created:", df_map_tr_bar_qry_rslt)

        #         df_map_tr_bar_qry_rslt1 = df_map_tr_bar_qry_rslt.set_index(pd.Index(range(1, len(df_map_tr_bar_qry_rslt)+1)))

        #         st.write("Converting types...")
        #         df_map_tr_bar_qry_rslt1['District'] = df_map_tr_bar_qry_rslt1['District'].astype(str)
        #         df_map_tr_bar_qry_rslt1['Amount'] = df_map_tr_bar_qry_rslt1['Amount'].astype(float).apply(convert_to_lakh)
        #         st.write("Converted DataFrame:", df_map_tr_bar_qry_rslt1)

        #         st.write("Generating chart...")
        #         df_map_tr_bar_qry_rslt1_fig = px.bar(
        #             df_map_tr_bar_qry_rslt1,
        #             x='District',
        #             y='Amount',
        #             color='Amount',
        #             color_continuous_scale='thermal',
        #             title='District Wise Analysis Chart',
        #             height=700
        #         )
        #         df_map_tr_bar_qry_rslt1_fig.update_layout(title_font=dict(size=33), title_font_color='#6739b7')
        #         st.plotly_chart(df_map_tr_bar_qry_rslt1_fig, use_container_width=True)

        #     except Exception as e:
        #         st.error(f"Error occurred: {e}")

        with tab7_2:
            try:

                #Map Transaction table query
                cursor.execute(f"SELECT District, Amount, Count FROM map_transaction WHERE State='{map_tr_st}' AND Year='{map_tr_yr}' AND Quarter='{map_tr_qtr}';")
                map_tr_tabl_qry_rslt= cursor.fetchall()
                df_map_tr_tabl_qry_rslt= pd.DataFrame(np.array(map_tr_tabl_qry_rslt), columns=['District','Amount','Count'])
                df_map_tr_tabl_qry_rslt1= df_map_tr_tabl_qry_rslt.set_index(pd.Index(range(1,len(df_map_tr_tabl_qry_rslt)+1)))

                # Output
                st.subheader('District Transaction Analysis Chart')
                st.dataframe(df_map_tr_tabl_qry_rslt1)
            except:
                st.error('Data Available till 2024 Quarter 2')

    # Map User Analysis
    with tab8:
        col1, col2, col3 = st.columns(3)
        with col1:
            map_usr_st = st.selectbox('**Select State**',('andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh','assam', 'bihar', 
            'chandigarh', 'chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 
            'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh','maharashtra', 'manipur', 
            'meghalaya', 'mizoram', 'nagaland','odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim', 'tamil-nadu', 'telangana', 
            'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'),key='map_usr_st')
        with col2:
            map_usr_yr= st.selectbox('**Select Year**',('2018','2019','2020','2021','2022','2023','2024'), key='map_usr_yr')
        with col3:
            map_usr_qtr = st.selectbox('**Select Quarter**',('1','2','3','4'), key='map_usr_qtr')
            #if map_tr_yr == '2024' and (map_tr_qtr == '3' or map_tr_qtr == '4'):
                #st.header('Data available till 2024 quarter 2')

        #SQL Query
        tab8_1, tab8_2 = st.tabs(['Bar Chart', 'Total Calculation'])

        with tab8_1:
            try:
                cursor.execute(f"SELECT District, Registered_user FROM map_user WHERE State='{map_usr_st}' AND Year='{map_usr_yr}' AND Quarter='{map_usr_qtr}';")
                map_usr_bar_qry_rslt = cursor.fetchall()
                # if not map_tr_bar_qry_rslt:
                #     st.warning("No data found for the selected inputs.")
                #     raise ValueError("Empty result set")
                df_map_usr_bar_qry_rslt= pd.DataFrame(np.array(map_usr_bar_qry_rslt), columns=['District','Registered_user'])
                df_map_usr_bar_qry_rslt1= df_map_usr_bar_qry_rslt.set_index(pd.Index(range(1,len(df_map_usr_bar_qry_rslt)+1)))

                # output

                df_map_usr_bar_qry_rslt1['District']= df_map_usr_bar_qry_rslt1['District'].astype(str)
                df_map_usr_bar_qry_rslt1['Registered_user']= df_map_usr_bar_qry_rslt1['Registered_user'].astype(float).apply(convert_to_thousand)
                df_map_usr_bar_qry_rslt1_fig = px.bar(df_map_usr_bar_qry_rslt1, x= 'District', y='Registered_user', color='Registered_user', color_continuous_scale= 'thermal', title='District Wise User Analysis Chart', height= 700)
                df_map_usr_bar_qry_rslt1_fig.update_layout(title_font= dict(size=33),title_font_color='#6739b7')
                st.plotly_chart(df_map_usr_bar_qry_rslt1_fig, use_container_width= True)
            except:
                st.error('Data Available till 2024 Quarter 2')

        with tab8_2:
            try:

                #Map Transaction table query
                cursor.execute(f"SELECT District, Registered_user, App_opens FROM map_user WHERE State='{map_usr_st}' AND Year='{map_usr_yr}' AND Quarter='{map_usr_qtr}';")
                map_usr_tabl_qry_rslt= cursor.fetchall()
                df_map_usr_tabl_qry_rslt= pd.DataFrame(np.array(map_usr_tabl_qry_rslt), columns=['District','Registered_user','App_opens'])
                df_map_usr_tabl_qry_rslt1= df_map_usr_tabl_qry_rslt.set_index(pd.Index(range(1,len(df_map_usr_tabl_qry_rslt)+1)))

                # Output
                st.subheader('District User Analysis Chart')
                st.dataframe(df_map_usr_tabl_qry_rslt1)
            except:
                st.error('Data Available till 2024 Quarter 2')






