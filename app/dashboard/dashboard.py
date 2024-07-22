import streamlit as st
import plotly.express as px
from streamlit_folium import folium_static
import folium
import pandas as pd
import numpy as np
import random
import streamlit_option_menu
from streamlit_option_menu import option_menu

# Database Connection Details
POSTGRES_CONNECTION = {
    "dialect": "postgresql",
    "host": "postgres", 
    "port": "5432",
    "username": "myuser",
    "password": "mypassword",
    "database": "mydatabase"
}

# Fetch data function
def fetch_data(sql, params=None):
    conn = st.connection("postgres", type="sql", **POSTGRES_CONNECTION)
    try:
        if params:
            data = conn.query(sql, params)
        else:
            data = conn.query(sql)
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Failed to fetch data: {e}")
        return pd.DataFrame()


# # Sidebar for navigation
def setup_sidebar():
    with st.sidebar:
        selected = option_menu(
            menu_title="MENU",
            options=["Home", "Measures", "Hospitals"],
            icons=["house", "bar-chart", "hospital"],
            menu_icon="cast",
            default_index=0,
        )

    return selected

    
# Display functions for different sections

# Home Page
# def display_home_page():
#     st.title("Welcome to Healthcare Resource Allocation")
#     st.write("""
#         This application is designed to facilitate healthcare resource allocation through data visualization and chatbot interface.
#         Use the sidebar to navigate to different sections:
#     """)

#     st.markdown("### General Plots")
#     df = pd.DataFrame({'x': [1, 2, 3, 4, 5], 'y': [10, 20, 30, 40, 50]})
#     fig = px.line(df, x='x', y='y', title='Example Plot')
#     st.plotly_chart(fig)

def generate_fake_data():
    data = {
        "Total Hospitals": {
            "current": random.randint(100, 150),
            "previous": random.randint(-5, 5)
        },
        "Total Surgeries": {
            "current": random.randint(5000, 7000),
            "previous": random.randint(-500, 500)
        },
        "Total Emergencies": {
            "current": random.randint(8000, 12000),
            "previous": random.randint(-1000, 1000)
        }
    }
    return data

def display_home_page():
    st.title("Welcome to Healthcare Resource Allocation")

    # Display a healthcare-related image
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        st.write("")
    with col2:
        st.image("/app/images/symbol.png", width=50, use_column_width=True)  # Replace with the path to your image file
    with col3:
        st.write("")
    st.write("""
        This application is designed to facilitate healthcare resource allocation through data.
        Use the sidebar to navigate to different sections:
    """)

    st.markdown("### Key Metrics")

    # Generate fake data for metrics
    metrics_data = generate_fake_data()

    m1, m2, m3, m4, m5 = st.columns((1, 1, 1, 1, 1))
    
    m1.write('')
    m2.metric(label='Total Hospitals in Australia',
              value=metrics_data["Total Hospitals"]["current"],
              delta=f"{metrics_data['Total Hospitals']['previous']} Compared to last month",
              delta_color='inverse')
    m3.metric(label='Total Surgeries',
              value=metrics_data['Total Surgeries']['current'],
              delta=f"{metrics_data['Total Surgeries']['previous']} Compared to last month",
              delta_color='inverse')
    m4.metric(label='Total Emergencies',
              value=metrics_data['Total Emergencies']['current'],
              delta=f"{metrics_data['Total Emergencies']['previous']} Compared to last month")
    m1.write('')

    with st.expander('About', expanded=True):
        st.write('''
            - Data: [Australian Institute of Health and Welfare](https://www.aihw.gov.au), [Australian Bureau of Statistics](https://www.abs.gov.au/statistics/people/population)
            ''')    

    st.markdown("### General Plots")
    df = pd.DataFrame({'x': [1, 2, 3, 4, 5], 'y': [10, 20, 30, 40, 50]})
    fig = px.line(df, x='x', y='y', title='Example Plot')
    st.plotly_chart(fig)


# Display Measures

def display_measures():
    st.title("Measures")
    st.markdown("### Explore Healthcare Metrics by State and Hospital")
    st.markdown("""
    This section of the dashboard allows you to explore detailed metrics for various healthcare measures across different states in Australia.""")
    sql_query_codes = '''   
    SELECT 
        ds.*, 
        m.measurename,
        rm.reportedmeasurename
    FROM 
        datasets ds
    LEFT JOIN 
        measurements m ON ds.measurecode = m.measurecode
    LEFT JOIN 
        reported_measurements rm ON ds.reportedmeasurecode = rm.reportedmeasurecode
    WHERE 
        ds.stored = TRUE;
    '''
                        
    df_measures = fetch_data(sql_query_codes)
    selected_measure = st.selectbox("Select Measure", np.sort(df_measures['measurename'].unique()))
    df_reported_measures = df_measures[df_measures['measurename'] == selected_measure]
    selected_reported_measure = st.selectbox("Select Reported Measure", np.sort(df_reported_measures['reportedmeasurename'].unique()))
    
    # Fetch the list of states
    sql_query_states = '''
    SELECT DISTINCT 
        state 
    FROM 
        hospitals 
    WHERE 
        open_closed = 'Open';
    '''
    df_states = fetch_data(sql_query_states)
    state_list = df_states['state'].unique()
    
    selected_state = st.selectbox("Select State", np.sort(state_list))
    
    safe_measure = selected_measure.replace("'", "''")  # rudimentary SQL injection protection
    safe_reported_measure = selected_reported_measure.replace("'", "''")  # rudimentary SQL injection protection
    safe_state = selected_state.replace("'", "''")  # rudimentary SQL injection protection

    sql_query_state = f'''
    SELECT 
        info.value,
        ds.reportingstartdate,
        info.reportingunitcode,
        h.name as hospital_name,
        h.latitude,
        h.longitude
    FROM 
        datasets ds
    JOIN 
        measurements m ON ds.measurecode = m.measurecode
    JOIN 
        reported_measurements rm ON ds.reportedmeasurecode = rm.reportedmeasurecode
    JOIN 
        info ON ds.datasetid = info.datasetid
    JOIN 
        hospitals h ON info.reportingunitcode = h.code
    WHERE 
        m.measurename = '{safe_measure}' AND 
        rm.reportedmeasurename = '{safe_reported_measure}' AND
        ds.stored = TRUE AND 
        h.state = '{safe_state}'
    ORDER BY 
        ds.reportingstartdate ASC;
    '''
    
    df_value = fetch_data(sql_query_state)

    if df_value.empty:
        st.write("No data found for the selected state. Displaying national data.")
        
        sql_query_national = f'''
        SELECT 
            info.value,
            ds.reportingstartdate,
            info.reportingunitcode,
            h.name as hospital_name,
            h.latitude,
            h.longitude
        FROM 
            datasets ds
        JOIN 
            measurements m ON ds.measurecode = m.measurecode
        JOIN 
            reported_measurements rm ON ds.reportedmeasurecode = rm.reportedmeasurecode
        JOIN 
            info ON ds.datasetid = info.datasetid
        JOIN 
            hospitals h ON info.reportingunitcode = h.code
        WHERE 
            m.measurename = '{safe_measure}' AND 
            rm.reportedmeasurename = '{safe_reported_measure}' AND
            ds.stored = TRUE AND 
            info.reportingunitcode = 'NAT'
        ORDER BY 
            ds.reportingstartdate ASC;
        '''

        df_value = fetch_data(sql_query_national)

    # Filter out NaN values
    df_value = df_value.dropna()

    if not df_value.empty:
        # Aggregate data by reporting date
        df_value_aggregated = df_value.groupby('reportingstartdate').agg({'value': 'mean'}).reset_index()

        # Plotting using Plotly Express
        fig = px.line(df_value_aggregated, x='reportingstartdate', y='value', title=f'{selected_measure} - {selected_reported_measure} Over Time (Averaged)')
        st.plotly_chart(fig)
        
        # Plotting the map of hospitals
        fig_map = px.scatter_mapbox(
            df_value, lat='latitude', lon='longitude', hover_name='hospital_name',
            hover_data={'latitude': False, 'longitude': False, 'value': True},
            title=f'Hospitals in {selected_state} Reporting {selected_measure}',
            mapbox_style="open-street-map", zoom=5,
            color_discrete_sequence=["darkblue"]
        )
        st.plotly_chart(fig_map)

        # Center the data table and include hospital names
        st.markdown("<div style='text-align: center;'>", unsafe_allow_html=True)
        st.dataframe(df_value[['reportingstartdate', 'value', 'hospital_name']])
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.write("No data found for the selected measure and reported measure.")
    
    if st.button("Return to Home"):
        st.session_state['page'] = 'home'
    


# hospitals 
        
def display_hospitals():
    """Display hospitals on a map, as a pie chart, and in a table."""
    st.title("Hospitals")

    if st.button("Return to Home"):
        st.session_state['page'] = 'home'
    
    # Fetch hospital data from the database
    df = fetch_data('SELECT Latitude, Longitude, Name, Type, Sector, Open_Closed, State FROM hospitals')
    df['latitude'] = pd.to_numeric(df['latitude'])
    df['longitude'] = pd.to_numeric(df['longitude'])
    df.dropna(subset=['latitude', 'longitude'], inplace=True)

    hospital_map = folium.Map(location=[-25, 135], zoom_start=5)
    for _, row in df.iterrows():
        folium.Marker(
            [row['latitude'], row['longitude']],
            popup=row['name']
        ).add_to(hospital_map)
    folium_static(hospital_map)

    state_sector_counts = df.groupby(['state', 'sector']).size().reset_index(name='Number of Hospitals')

    private_hospitals = state_sector_counts[state_sector_counts['sector'] == 'Private']
    public_hospitals = state_sector_counts[state_sector_counts['sector'] == 'Public']

    state_counts = pd.merge(private_hospitals, public_hospitals, on='state', suffixes=('_private', '_public'), how='outer').fillna(0)

    #pie chart for the total private and public hospitals in Australia
    total_private_hospitals = private_hospitals['Number of Hospitals'].sum() if not private_hospitals.empty else 0
    total_public_hospitals = public_hospitals['Number of Hospitals'].sum() if not public_hospitals.empty else 0

    fig_pie = px.pie(names=['Private', 'Public'], values=[total_private_hospitals, total_public_hospitals], 
                 title="Total Private and Public Hospitals in Australia")
    st.plotly_chart(fig_pie)

    #histogram for the number of private and public hospitals per state
    fig_hist = px.bar(state_counts, x='state', y=['Number of Hospitals_private', 'Number of Hospitals_public'], barmode='group', 
                 title="Number of Private and Public Hospitals per State", labels={'value': 'Number of Hospitals', 'variable': 'Hospital Type'})
    fig_hist.update_layout(xaxis_title="State", yaxis_title="Count")
    st.plotly_chart(fig_hist)

    # hospitals based on selected state, open/closed status, and sector
    selected_state = st.selectbox("Select State", df['state'].unique())
    selected_status = st.selectbox("Select Open/Closed", df['open_closed'].unique())
    selected_sector = st.selectbox("Select Sector", df['sector'].unique())
    
    st.markdown(f"### Hospitals in {selected_state} - {selected_status} - {selected_sector}")
    
    filtered_df = df[(df['state'] == selected_state) & (df['open_closed'] == selected_status) & (df['sector'] == selected_sector)]
    
    if not filtered_df.empty:
        st.table(filtered_df)
    else:
        st.write("No hospitals found with the selected criteria.")
# Main Function
def main():
    if 'page' not in st.session_state:
        st.session_state['page'] = 'home'

    page = setup_sidebar()  # Setup sidebar and store the selected page

    if page == 'Home':
        display_home_page()
    elif page == 'Measures':
        display_measures()
    elif page == 'Hospitals':
        display_hospitals()


if __name__ == '__main__':
    main()