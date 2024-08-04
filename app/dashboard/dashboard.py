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
            options=["Home", "Measures", "Hospitals", "Budget", "Contact us"],
            icons=["house", "bar-chart", "hospital", "activity", "envelope"],
            menu_icon="cast",
            default_index=0,
        )
        # st.markdown(
        #     """
        #     <div style="display: flex; justify-content: center; align-items: center;">
        #         <img src="/app/images/logo.png" width="150">
        #     </div>
        #     """,
        #     unsafe_allow_html=True
        # )

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


def display_home_page():
    st.title("Welcome to Healthcare Resource Allocation")

    # Display a healthcare-related image
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        st.write("")
    with col2:
        st.image("/app/images/logo.png", width=50, use_column_width=True)  
    with col3:
        st.write("")
    st.write("""
        This application is designed to facilitate healthcare resource allocation through data.
        Use the sidebar to navigate to different sections:
    """)

    st.markdown("### Key Metrics")

    # Generate fake data for metrics
    # metrics_data = generate_fake_data()

    # m1, m2, m3, m4, m5 = st.columns((1, 1, 1, 1, 1))

    # m1.write('')
    # m2.metric(label='Total Hospitals in Australia',
    #           value=metrics_data["Total Hospitals"]["current"],
    #           delta=f"{metrics_data['Total Hospitals']['previous']} Compared to last month",
    #           delta_color='inverse')
    # m3.metric(label='Total Surgeries',
    #           value=metrics_data['Total Surgeries']['current'],
    #           delta=f"{metrics_data['Total Surgeries']['previous']} Compared to last month",
    #           delta_color='inverse')
    # m4.metric(label='Total Emergencies',
    #           value=metrics_data['Total Emergencies']['current'],
    #           delta=f"{metrics_data['Total Emergencies']['previous']} Compared to last month")
    # m1.write('')

    with st.expander('About', expanded=True):
        st.write('''
            - Data: [Australian Institute of Health and Welfare](https://www.aihw.gov.au), [Australian Bureau of Statistics](https://www.abs.gov.au/statistics/people/population)
            ''')

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


# def display_hospitals():
#     """Display hospitals on a map, as a pie chart, and in a table."""
#     st.title("Hospitals")

#     if st.button("Return to Home"):
#         st.session_state['page'] = 'home'

#     # Fetch hospital data from the database
#     hospital_df = fetch_data('SELECT Latitude, Longitude, Name, Type, Sector, Open_Closed, State FROM hospitals')
#     hospital_df['latitude'] = pd.to_numeric(hospital_df['latitude'])
#     hospital_df['longitude'] = pd.to_numeric(hospital_df['longitude'])
#     hospital_df.dropna(subset=['latitude', 'longitude'], inplace=True)

#     hospital_map = folium.Map(location=[-25, 135], zoom_start=5)
#     for _, row in hospital_df.iterrows():
#         folium.Marker(
#             [row['latitude'], row['longitude']],
#             popup=row['name']
#         ).add_to(hospital_map)
#     folium_static(hospital_map)

#     state_sector_counts = hospital_df.groupby(['state', 'sector']).size().reset_index(name='Number of Hospitals')

#     private_hospitals = state_sector_counts[state_sector_counts['sector'] == 'Private']
#     public_hospitals = state_sector_counts[state_sector_counts['sector'] == 'Public']

#     state_counts = pd.merge(private_hospitals, public_hospitals, on='state', suffixes=('_private', '_public'), how='outer').fillna(0)

#     # Histogram for the number of private and public hospitals per state
#     fig_hist = px.bar(state_counts, x='state', y=['Number of Hospitals_private', 'Number of Hospitals_public'], barmode='group',
#                       title="Number of Private and Public Hospitals per State", labels={'value': 'Number of Hospitals', 'variable': 'Hospital Type'})
#     fig_hist.update_layout(xaxis_title="State", yaxis_title="Count")
#     st.plotly_chart(fig_hist)


#     # Load the Excel file
#     excel_file = '/app/Additional_data/AdmittedPatients.xlsx'

#     # Load Table 2.1
#     df_2_1 = pd.read_excel(excel_file, sheet_name='Table 2.1', skiprows=2)
#     df_2_1 = df_2_1.rename(columns={
#         'Unnamed: 0': 'Hospital Type',
#         '2018–19': '2018-19',
#         '2019–20': '2019-20',
#         '2020–21': '2020-21',
#         '2021–22(a)': '2021-22',
#         '2022–23': '2022-23'
#     }).drop(columns=['Average since 2018–19', 'Since 2021–22'])
#     df_2_1['Hospital Type'] = df_2_1['Hospital Type'].ffill()
#     df_2_1 = df_2_1[df_2_1['Hospital Type'].isin(['Total public hospitals', 'Total private hospitals'])]
#     df_melted_2_1 = df_2_1.melt(id_vars='Hospital Type', var_name='Year', value_name='Separations')
#     df_melted_2_1 = df_melted_2_1[~df_melted_2_1['Year'].str.contains('Unnamed')]
#     df_melted_2_1['Year'] = pd.to_datetime(df_melted_2_1['Year'].apply(lambda x: x.split('-')[0]), format='%Y')
#     df_melted_2_1['Separations'] = pd.to_numeric(df_melted_2_1['Separations'])

#     # Plot the separations over time for private and public hospitals
#     fig_line = px.line(df_melted_2_1, x='Year', y='Separations', color='Hospital Type',
#                        title="Separations Over Time for Private and Public Hospitals",
#                        labels={'Year': 'Year', 'Separations': 'Number of Separations', 'Hospital Type': 'Hospital Type'})
#     st.plotly_chart(fig_line)

#     # Hospitals based on selected state and open/closed status
#     selected_state_hospital = st.selectbox("Select State", hospital_df['state'].unique())
#     selected_status = st.selectbox("Select Open/Closed", hospital_df['open_closed'].unique())

#     st.markdown(f"### Hospitals in {selected_state_hospital}")

#     filtered_df = hospital_df[(hospital_df['state'] == selected_state_hospital) & (hospital_df['open_closed'] == selected_status)]

#     col1, col2 = st.columns(2)

#     with col1:
#         if not filtered_df.empty:
#             fig_map = px.scatter_mapbox(
#                 filtered_df,
#                 lat="latitude",
#                 lon="longitude",
#                 hover_name="name",
#                 color="sector",
#                 zoom=4,
#                 height=500,
#                 color_discrete_map={'Public': 'blue', 'Private': 'light blue'}
#             )
#             fig_map.update_layout(mapbox_style="open-street-map")
#             fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
#             st.plotly_chart(fig_map)
#         else:
#             st.write("No hospitals found with the selected criteria.")

#     with col2:
#         sector_counts = filtered_df['sector'].value_counts().reset_index()
#         sector_counts.columns = ['Sector', 'Count']

#         fig_pie = px.pie(sector_counts, names='Sector', values='Count', title=f"Total number of hospitals in {selected_state_hospital}")
#         st.plotly_chart(fig_pie)


def clean_year_column(year):
    if isinstance(year, str):
        return year.split('–')[0]
    return year

def display_hospitals():
    """Display hospitals on a map, as a pie chart, and in a table."""
    st.title("Hospitals")

    if st.button("Return to Home"):
        st.session_state['page'] = 'home'

    # Fetch hospital data from the database
    hospital_df = fetch_data('SELECT Latitude, Longitude, Name, Type, Sector, Open_Closed, State FROM hospitals')
    hospital_df['latitude'] = pd.to_numeric(hospital_df['latitude'])
    hospital_df['longitude'] = pd.to_numeric(hospital_df['longitude'])
    hospital_df.dropna(subset=['latitude', 'longitude'], inplace=True)

    hospital_map = folium.Map(location=[-25, 135], zoom_start=5)
    for _, row in hospital_df.iterrows():
        folium.Marker(
            [row['latitude'], row['longitude']],
            popup=row['name']
        ).add_to(hospital_map)
    folium_static(hospital_map)

    state_sector_counts = hospital_df.groupby(['state', 'sector']).size().reset_index(name='Number of Hospitals')

    private_hospitals = state_sector_counts[state_sector_counts['sector'] == 'Private']
    public_hospitals = state_sector_counts[state_sector_counts['sector'] == 'Public']

    state_counts = pd.merge(private_hospitals, public_hospitals, on='state', suffixes=('_private', '_public'), how='outer').fillna(0)

    # Histogram for the number of private and public hospitals per state
    fig_hist = px.bar(state_counts, x='state', y=['Number of Hospitals_private', 'Number of Hospitals_public'], barmode='group',
                      title="Number of Private and Public Hospitals per State", labels={'value': 'Number of Hospitals', 'variable': 'Hospital Type'})
    fig_hist.update_layout(xaxis_title="State", yaxis_title="Count")
    st.plotly_chart(fig_hist)

    # Load the Excel file
    excel_file = '/app/Additional_data/AdmittedPatients.xlsx'

    # Load Table 2.1
    df_2_1 = pd.read_excel(excel_file, sheet_name='Table 2.1', skiprows=2)
    df_2_1 = df_2_1.rename(columns={
        'Unnamed: 0': 'Hospital Type',
        '2018–19': '2018-19',
        '2019–20': '2019-20',
        '2020–21': '2020-21',
        '2021–22(a)': '2021-22',
        '2022–23': '2022-23'
    }).drop(columns=['Average since 2018–19', 'Since 2021–22'])
    df_2_1['Hospital Type'] = df_2_1['Hospital Type'].ffill()
    df_2_1 = df_2_1[df_2_1['Hospital Type'].isin(['Total public hospitals', 'Total private hospitals'])]
    df_melted_2_1 = df_2_1.melt(id_vars='Hospital Type', var_name='Year', value_name='Separations')
    df_melted_2_1 = df_melted_2_1[~df_melted_2_1['Year'].str.contains('Unnamed')]
    df_melted_2_1['Year'] = df_melted_2_1['Year'].apply(clean_year_column)
    df_melted_2_1['Year'] = pd.to_datetime(df_melted_2_1['Year'], format='%Y', errors='coerce')
    df_melted_2_1['Separations'] = pd.to_numeric(df_melted_2_1['Separations'], errors='coerce')

    # Plot the separations over time for private and public hospitals
    fig_line = px.line(df_melted_2_1, x='Year', y='Separations', color='Hospital Type',
                       title="Separations Over Time for Private and Public Hospitals",
                       labels={'Year': 'Year', 'Separations': 'Number of Separations', 'Hospital Type': 'Hospital Type'})
    st.plotly_chart(fig_line)

    # # Load Table 2.2
    # df_2_2 = pd.read_excel(excel_file, sheet_name='Table 2.2', skiprows=2)
    # df_2_2 = df_2_2.rename(columns={
    #     'Unnamed: 0': 'State',
    #     '2018–19': '2018-19',
    #     '2019–20': '2019-20',
    #     '2020–21': '2020-21',
    #     '2021–22(a)': '2021-22',
    #     '2022–23': '2022-23',
    #     'Average since 2018–19': 'Average',
    #     'Since 2021–22': 'Change'
    # })
    # df_2_2 = df_2_2.replace('n.p.', pd.NA)
    # df_2_2 = df_2_2[df_2_2['State'].notna() & df_2_2['State'].str.contains('All hospitals')]
    # df_2_2 = df_2_2.melt(id_vars='State', var_name='Year', value_name='Separations')
    # df_2_2 = df_2_2[~df_2_2['Year'].isin(['Average', 'Change'])]
    # df_2_2['Year'] = df_2_2['Year'].apply(clean_year_column)
    # df_2_2['Year'] = pd.to_datetime(df_2_2['Year'], format='%Y', errors='coerce')
    # df_2_2['Separations'] = pd.to_numeric(df_2_2['Separations'], errors='coerce')

    # # Plot the separations over time for each state
    # fig_separations = px.line(df_2_2, x='Year', y='Separations', color='State',
    #                           title="Separations Over Time by State",
    #                           labels={'Year': 'Year', 'Separations': 'Number of Separations', 'State': 'State'})
    # st.plotly_chart(fig_separations)

    # # Hospitals based on selected state and open/closed status
    selected_state_hospital = st.selectbox("Select State", hospital_df['state'].unique())
    selected_status = st.selectbox("Select Open/Closed", hospital_df['open_closed'].unique())

    st.markdown(f"### Hospitals in {selected_state_hospital}")

    filtered_df = hospital_df[(hospital_df['state'] == selected_state_hospital) & (hospital_df['open_closed'] == selected_status)]

    col1, col2 = st.columns(2)

    with col1:
        if not filtered_df.empty:
            fig_map = px.scatter_mapbox(
                filtered_df,
                lat="latitude",
                lon="longitude",
                hover_name="name",
                color="sector",
                zoom=4,
                height=500,
                color_discrete_map={'Public': 'blue', 'Private': 'light blue'}
            )
            fig_map.update_layout(mapbox_style="open-street-map")
            fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_map)
        else:
            st.write("No hospitals found with the selected criteria.")

    with col2:
        sector_counts = filtered_df['sector'].value_counts().reset_index()
        sector_counts.columns = ['Sector', 'Count']

        fig_pie = px.pie(sector_counts, names='Sector', values='Count', title=f"Total number of hospitals in {selected_state_hospital}")
        st.plotly_chart(fig_pie)

    df_2_9 = pd.read_excel(excel_file, sheet_name='Table 2.9', skiprows=2)
    df_2_9 = df_2_9.rename(columns={
        'Unnamed: 0': 'Category',
        '2018–19': '2018-19',
        '2019–20': '2019-20',
        '2020–21': '2020-21',
        '2021–22(b)': '2021-22',
        '2022–23': '2022-23',
        'Average since 2018–19': 'Average since 2018-19',
        'Since 2021–22': 'Since 2021-22'
    })

    total_public_hospitals = df_2_9[df_2_9['Category'] == 'Total public hospitals']
    total_private_hospitals = df_2_9[df_2_9['Category'] == 'Total private hospitals']

    # Melt the dataframes to long format, excluding columns 'Average since 2018-19' and 'Since 2021-22'
    total_public_hospitals_melted = total_public_hospitals.melt(
        id_vars='Category',
        value_vars=['2018-19', '2019-20', '2020-21', '2021-22', '2022-23'],
        var_name='Year',
        value_name='Average Length of Stay'
    )

    total_private_hospitals_melted = total_private_hospitals.melt(
        id_vars='Category',
        value_vars=['2018-19', '2019-20', '2020-21', '2021-22', '2022-23'],
        var_name='Year',
        value_name='Average Length of Stay'
    )

    # Plot for total public hospitals
    fig_total_public = px.line(
        total_public_hospitals_melted, x='Year', y='Average Length of Stay', color='Category',
        title="Average Length of Stay for Total Public Hospitals",
        labels={'Year': 'Year', 'Average Length of Stay': 'Average Length of Stay (days)'}
    )

    # Plot for total private hospitals
    fig_total_private = px.line(
        total_private_hospitals_melted, x='Year', y='Average Length of Stay', color='Category',
        title="Average Length of Stay for Total Private Hospitals",
        labels={'Year': 'Year', 'Average Length of Stay': 'Average Length of Stay (days)'}
    )

    # Display the plots side by side
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig_total_public)
    with col2:
        st.plotly_chart(fig_total_private)



def display_contactus():
    st.title("Contact Us")

    st.write("""
    We are here to assist you with any questions, concerns, or feedback you may have. Please feel free to reach out to us via email.
    """)

    # Display contact information
    st.write("**Sonia Borsi**")
    st.write("[Sonia.borsi@studenti.unitn.it](mailto:sonia.borsi@studenti.unitn.it) | [Linkedin](https://www.linkedin.com/in/sonia-borsi-824998260/)")
    st.write("**Filippo Costamagna**")
    st.write("[Filippo.costamagna](mailto:filippo.costamagna@studenti.unitn.it) | [Linkedin](https://www.linkedin.com/in/filippo-costamagna-a439b3303/)")

    st.write("""
    We look forward to hearing from you and will respond as soon as possible.
    """)


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
    elif page == 'Contact us':
        display_contactus()


if __name__ == '__main__':
    main()