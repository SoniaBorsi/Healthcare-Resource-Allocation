# pages/hospitals.py
from dash import dcc, html, Input, Output, State, callback
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from db_utils import fetch_data
import dash
from components.navbar import navbar

# Mapbox token (optional, replace with your own if available)
# Get a free token from https://www.mapbox.com/
# If you don't have a token, you can use 'open-street-map' as the map style
mapbox_token = None  # Replace with your Mapbox token or leave as None

# Load hospital data
sql_query = 'SELECT latitude, longitude, name, type, sector, open_closed, state FROM hospitals'
hospital_df = fetch_data(sql_query)
hospital_df['latitude'] = pd.to_numeric(hospital_df['latitude'], errors='coerce')
hospital_df['longitude'] = pd.to_numeric(hospital_df['longitude'], errors='coerce')
hospital_df.dropna(subset=['latitude', 'longitude'], inplace=True)

# Load the Excel data
excel_file = 'data/Admitted Patients.xlsx'  # Adjust the path if necessary
xls = pd.ExcelFile(excel_file)

# Load and clean the data for Table 2.9
table_2_9_cleaned = pd.read_excel(xls, sheet_name='Table 2.9', header=2)
table_2_9_cleaned = table_2_9_cleaned.drop(columns=['Average since 2018–19', 'Since 2021–22'])

# Retain only the rows related to public and private hospitals
public_hospitals = table_2_9_cleaned.iloc[[1, 2, 3]].copy()
private_hospitals = table_2_9_cleaned.iloc[[4, 5, 6]].copy()

# Convert the data to long format for easier plotting
public_hospitals_long = public_hospitals.melt(id_vars=['Unnamed: 0'], var_name='Year', value_name='Average Length of Stay')
private_hospitals_long = private_hospitals.melt(id_vars=['Unnamed: 0'], var_name='Year', value_name='Average Length of Stay')

# Rename the 'Unnamed: 0' column to 'Hospital Type'
public_hospitals_long = public_hospitals_long.rename(columns={'Unnamed: 0': 'hospital_type'})
private_hospitals_long = private_hospitals_long.rename(columns={'Unnamed: 0': 'hospital_type'})

# Population data for each state
population_data = {
    "New South Wales": 7317500,
    "Victoria": 5640900,
    "Queensland": 4599400,
    "Western Australia": 2366900,
    "South Australia": 1659800,
    "Tasmania": 511000,
    "Australian Capital Territory": 366900,
    "Northern Territory": 231200
}

# Dropdown options
state_options = [{'label': state, 'value': state} for state in sorted(hospital_df['state'].unique())]
status_options = [{'label': status, 'value': status} for status in sorted(hospital_df['open_closed'].unique())]

layout = html.Div([
    navbar,
    html.Div([
        html.H1("Hospitals", className='text-center'),
        html.Hr(),
        html.Div([
            html.H2("Explore Australian Hospitals"),
            html.P("""
                This section provides a comprehensive analysis of hospitals across different states. You can visualize the locations of hospitals on an interactive map, explore the distribution of private and public hospitals, and filter hospitals by state and operational status.
            """),
            html.Br(),
        ]),
        # Map of all hospitals
        html.Div([
            html.H3("Hospital Locations in Australia"),
            dcc.Graph(id='hospital-map'),
        ]),
        # Bar chart for number of private and public hospitals per state
        html.Div([
            html.H3("Number of Private and Public Hospitals per State"),
            dcc.Graph(id='state-sector-bar'),
        ]),
        # Average length of stay plots
        html.Div([
            html.H3("Average Length of Stay in Australian Hospitals"),
            dcc.Graph(id='average-length-of-stay-public'),
            dcc.Graph(id='average-length-of-stay-private'),
        ]),
        # Filters for state and open/closed status
        html.Div([
            html.H3("Hospital Filters"),
            dbc.Row([
                dbc.Col([
                    html.Label("Select State"),
                    dcc.Dropdown(
                        id='state-dropdown',
                        options=state_options,
                        placeholder='Select a state',
                        value=state_options[0]['value'] if state_options else None
                    ),
                ], md=6),
                dbc.Col([
                    html.Label("Select Open/Closed Status"),
                    dcc.Dropdown(
                        id='status-dropdown',
                        options=status_options,
                        placeholder='Select status',
                        value=status_options[0]['value'] if status_options else None
                    ),
                ], md=6),
            ]),
            html.Br(),
            html.Div(id='hospital-stats'),
            html.Br(),
            # Map and pie chart based on filtered data
            dbc.Row([
                dbc.Col(dcc.Graph(id='filtered-hospital-map'), md=6),
                dbc.Col(dcc.Graph(id='sector-pie-chart'), md=6),
            ]),
        ]),
    ], className='container')
])

# Callback to update the hospital map
@dash.callback(
    Output('hospital-map', 'figure'),
    Input('url', 'pathname')
)
def update_hospital_map(_):
    fig_map = px.scatter_mapbox(
        hospital_df,
        lat="latitude",
        lon="longitude",
        hover_name="name",
        color="sector",
        zoom=4,
        height=600,
        color_discrete_map={'Public': 'blue', 'Private': 'lightblue'}
    )
    map_style = "open-street-map" if not mapbox_token else "mapbox://styles/mapbox/streets-v11"
    fig_map.update_layout(
        mapbox_style=map_style,
        mapbox_accesstoken=mapbox_token,
        margin={"r":0,"t":0,"l":0,"b":0}
    )
    return fig_map

# Callback to update the bar chart
@dash.callback(
    Output('state-sector-bar', 'figure'),
    Input('url', 'pathname')
)
def update_state_sector_bar(_):
    state_sector_counts = hospital_df.groupby(['state', 'sector']).size().reset_index(name='Number of Hospitals')
    state_sector_pivot = state_sector_counts.pivot(index='state', columns='sector', values='Number of Hospitals').fillna(0).reset_index()
    fig_hist = px.bar(
        state_sector_pivot,
        x='state',
        y=['Private', 'Public'],
        barmode='group',
        title="Number of Private and Public Hospitals per State",
        labels={'value': 'Number of Hospitals', 'variable': 'Hospital Type'}
    )
    fig_hist.update_layout(xaxis_title="State", yaxis_title="Count")
    return fig_hist

# Callback to update average length of stay plots
@dash.callback(
    Output('average-length-of-stay-public', 'figure'),
    Input('url', 'pathname')
)
def update_avg_length_stay_public(_):
    fig_public = px.bar(
        public_hospitals_long,
        x='Year',
        y='Average Length of Stay',
        color='hospital_type',
        title='Average Length of Stay for Public Hospitals'
    )
    fig_public.update_layout(xaxis_title="Year", yaxis_title="Average Length of Stay (Days)")
    return fig_public

@dash.callback(
    Output('average-length-of-stay-private', 'figure'),
    Input('url', 'pathname')
)
def update_avg_length_stay_private(_):
    fig_private = px.bar(
        private_hospitals_long,
        x='Year',
        y='Average Length of Stay',
        color='hospital_type',
        title='Average Length of Stay for Private Hospitals'
    )
    fig_private.update_layout(xaxis_title="Year", yaxis_title="Average Length of Stay (Days)")
    return fig_private

# Callback to update filtered hospitals and statistics
@dash.callback(
    [
        Output('hospital-stats', 'children'),
        Output('filtered-hospital-map', 'figure'),
        Output('sector-pie-chart', 'figure')
    ],
    [
        Input('state-dropdown', 'value'),
        Input('status-dropdown', 'value')
    ]
)
def update_filtered_hospitals(selected_state_hospital, selected_status):
    if selected_state_hospital and selected_status:
        filtered_df = hospital_df[
            (hospital_df['state'] == selected_state_hospital) &
            (hospital_df['open_closed'] == selected_status)
        ]
        
        population = population_data.get(selected_state_hospital, "N/A")
        total_hospitals = len(filtered_df)
        ratio = (total_hospitals / population) * 1000 if population != "N/A" else "N/A"

        # Create metrics
        metrics = html.Div([
            html.H3(f"Hospitals in {selected_state_hospital}"),
            dbc.Row([
                dbc.Col(dbc.Card([
                    dbc.CardHeader("Population"),
                    dbc.CardBody(html.H4(f"{population:,}" if population != "N/A" else population))
                ]), md=4),
                dbc.Col(dbc.Card([
                    dbc.CardHeader("Total Number of Hospitals"),
                    dbc.CardBody(html.H4(total_hospitals))
                ]), md=4),
                dbc.Col(dbc.Card([
                    dbc.CardHeader("Hospitals per 1,000 Population"),
                    dbc.CardBody(html.H4(f"{ratio:.2f}" if ratio != "N/A" else ratio))
                ]), md=4),
            ]),
        ])

        # Update the map
        if not filtered_df.empty:
            fig_map = px.scatter_mapbox(
                filtered_df,
                lat="latitude",
                lon="longitude",
                hover_name="name",
                color="sector",
                zoom=6,
                height=500,
                color_discrete_map={'Public': 'blue', 'Private': 'lightblue'}
            )
            map_style = "open-street-map" if not mapbox_token else "mapbox://styles/mapbox/streets-v11"
            fig_map.update_layout(
                mapbox_style=map_style,
                mapbox_accesstoken=mapbox_token,
                margin={"r":0,"t":0,"l":0,"b":0}
            )
        else:
            fig_map = px.scatter_mapbox()
            fig_map.update_layout(
                mapbox_style="open-street-map",
                margin={"r":0,"t":0,"l":0,"b":0},
                annotations=[dict(text="No hospitals found with the selected criteria.", showarrow=False,
                                  xref="paper", yref="paper", x=0.5, y=0.5, font=dict(size=20))]
            )
        
        # Update the pie chart
        if not filtered_df.empty:
            sector_counts = filtered_df['sector'].value_counts().reset_index()
            sector_counts.columns = ['sector', 'count']

            fig_pie = px.pie(
                sector_counts,
                names='sector',
                values='count',
                title=f"Total number of hospitals in {selected_state_hospital}"
            )
        else:
            fig_pie = px.pie()
            fig_pie.update_layout(
                annotations=[dict(text="No data available.", showarrow=False,
                                  xref="paper", yref="paper", x=0.5, y=0.5, font=dict(size=20))]
            )
        
        return metrics, fig_map, fig_pie

    else:
        # Return empty figures and a message prompting the user to select options
        metrics = html.Div([
            html.P("Please select a state and status to see the hospital statistics.")
        ])
        fig_map = px.scatter_mapbox()
        fig_pie = px.pie()
        return metrics, fig_map, fig_pie
