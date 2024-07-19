import streamlit as st
import plotly.express as px
from streamlit_folium import folium_static
import folium
import pandas as pd

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
def fetch_data(sql):
    conn = st.connection("postgres", type="sql", **POSTGRES_CONNECTION)
    try:
        data = conn.query(sql)
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Failed to fetch data: {e}")
        return pd.DataFrame()


# Home Page
def display_home_page():
    st.title("Welcome to Healthcare Resource Allocation")
    st.write("""
        This application is designed to facilitate healthcare resource allocation through data visualization and chatbot interface.
        
        Use the buttons below to navigate to different sections:
    """)
    
    if st.button('Measures'):
        st.session_state['page'] = 'measures'
    if st.button('Hospitals'):
        st.session_state['page'] = 'hospitals'
    if st.button('Chat'):
        st.session_state['page'] = 'chat'
    if st.button('Predictions'):
        st.session_state['page'] = 'predictions'
    
    st.markdown("### General Plots")
    # Example plot
    df = pd.DataFrame({
        'x': [1, 2, 3, 4, 5],
        'y': [10, 20, 30, 40, 50]
    })
    fig = px.line(df, x='x', y='y', title='Example Plot')
    st.plotly_chart(fig)

# Display Measures
def display_measures():
    st.title("Measures")
    if st.button("Return to Home"):
        st.session_state['page'] = 'home'
    
    sql_query = '''
        SELECT 
            ds."DataSetId", 
            ds."DatasetName", 
            COUNT(v."Value") AS "Occurrences" 
        FROM 
            "values" v 
        JOIN 
            "datasets" ds ON v."DatasetId" = ds."DataSetId" 
        GROUP BY 
            ds."DataSetId", 
            ds."DatasetName"
    '''
    df = fetch_data(sql_query)
    
    if not df.empty:
        fig = px.bar(df, x='Occurrences', y='DatasetName', orientation='h',
                     title='Occurrences by Dataset Name', labels={'DatasetName': 'Dataset Name'})
        st.plotly_chart(fig)
    else:
        st.write("No measures found.")
# hospitals 
        
def display_hospitals():
    """Display hospitals on a map, as a pie chart, and in a table."""
    st.title("Hospitals")

    if st.button("Return to Home"):
        st.session_state['page'] = 'home'
    
    # Fetch hospital data from the database
    df = fetch_data('SELECT Latitude, Longitude, Name, Type, Sector, Open_Closed, State FROM hospitals')
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
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
        
    if st.session_state['page'] == 'home':
        display_home_page()
    elif st.session_state['page'] == 'measures':
        display_measures()
    elif st.session_state['page'] == 'hospitals':
        display_hospitals()
    elif st.session_state['page'] == 'predictions':
        display_predictions()
    elif st.session_state['page'] == 'chat':
        display_chatbot()

if __name__ == '__main__':
    main()