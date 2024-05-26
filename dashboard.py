import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import psycopg2
import pandas as pd
import json

def create_dashboard():
    def connect_to_db():
        conn = psycopg2.connect(
            dbname="hospitals",
            user="myuser",
            password="mypassword",
            host="localhost",
            port="5432"
        )
        return conn

    # Fetch hospital data from PostgreSQL database
    def fetch_hospital_data(conn):
        cursor = conn.cursor()
        cursor.execute("SELECT \"Name\", \"Latitude\", \"Longitude\", \"Sector\", \"Open/Closed\" FROM hospital_map")
        data = cursor.fetchall()
        cursor.close()
        conn.close()
        return data

    # Load GeoJSON data for Australian states
    with open('states.geojson') as f:
        states_geojson = json.load(f)

    # Create a Dash app
    app = dash.Dash(__name__)

    # Fetch hospital data
    hospital_data = fetch_hospital_data(connect_to_db())
    df = pd.DataFrame(hospital_data, columns=["Hospital Name", "Latitude", "Longitude", "Sector", "Open/Closed"])

    # Create map figure with GeoJSON and hospitals
    fig = px.scatter_mapbox(df, 
                            lat="Latitude", 
                            lon="Longitude", 
                            hover_name="Hospital Name",
                            hover_data={"Latitude": False, "Longitude": False},
                            zoom=3,
                            opacity=0.5,
                            mapbox_style="carto-positron",
                            center={"lat": -25, "lon": 133}
                        )

    fig.update_layout(mapbox={'center': {'lat': -25, 'lon': 133}})

    # Add Australian states from GeoJSON
    fig.update_layout(mapbox={'style': 'carto-positron'},
                    margin={'l':0, 'r':0, 't':0, 'b':0},
                    showlegend=False,
                    mapbox_zoom=3,
                    mapbox_center = {"lat": -25, "lon": 133},
                    )

    fig.update_geos(fitbounds="locations", visible=False)

    # Create a DataFrame with state names and a constant value (1 in this case)
    state_df = pd.DataFrame({
        'State': [feature['properties']['STATE_NAME'] for feature in states_geojson['features']],
        'Value': 1
    })

    # Add GeoJSON layers for states
    for feature in states_geojson['features']:
        fig.add_trace(
            px.choropleth_mapbox(
                geojson=feature, 
                locations=state_df['State'],
                color=state_df['Value'],
                color_continuous_scale=[[0, 'rgba(0,0,0,0)'], [1, 'rgba(0,0,0,0)']],
                range_color=[0, 1],
                opacity=0.1,
            ).data[0]
        )

    # Update hover info settings for all traces
    fig.update_traces(hoverinfo='skip')

    # Define app layout
    app.layout = html.Div([
        dcc.Graph(id="hospital-map", figure=fig),
        html.Div(id='hospital-info', style={'marginTop': 20})
    ])

    @app.callback(
        Output('hospital-info', 'children'),
        Input('hospital-map', 'clickData')
    )
    def display_hospital_info(clickData):
        if clickData is None:
            return "Click on a hospital to see more information."
        
        point = clickData['points'][0]
        hospital_name = point['hovertext']
        hospital_row = df[df['Hospital Name'] == hospital_name].iloc[0]
        sector = hospital_row['Sector']
        open_closed = hospital_row['Open/Closed']

        return html.Div([
            html.H4(hospital_name),
            html.P(f"Sector: {sector}"),
            html.P(f"Status: {open_closed}")
        ])

    return app

# This part is necessary to run the server only when this script is executed directly
if __name__ == "__main__":
    app = create_dashboard()
    app.run_server(debug=True)

