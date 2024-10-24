# app.py

import time
import logging
from db_utils import check_data_ready

# Configure logging (you can move this configuration to a separate module or the entry point)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def wait_for_data():
    tables_to_check = ['hospitals', 'datasets', 'measurements', 'info']
    max_attempts = 30
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        all_ready = True
        for table in tables_to_check:
            if not check_data_ready(table):
                all_ready = False
                logger.info(f"Data not ready in table '{table}'. Waiting...")
                break
        if all_ready:
            logger.info("All required data is available.")
            return
        time.sleep(5)
    logger.error("Data not available after maximum attempts.")
    raise Exception("Data not available after maximum attempts.")

# Wait for data readiness before initializing the app
#wait_for_data()

# Continue with app initialization
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc

# Initialize the app and load the CYBORG dark theme
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])

# For pages or multipage apps, you might also want to add a title and suppress callbacks exceptions:
app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.CYBORG],  # Apply dark theme
    suppress_callback_exceptions=True  # Needed if you're using multiple pages
)
server = app.server

# Import page layouts
from pages import home, measures, hospitals, contact_us

# Define the layout with a Location component for page routing
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])

# Callback to render the appropriate page content
@app.callback(Output('page-content', 'children'),
              Input('url', 'pathname'))
def display_page(pathname):
    if pathname == '/':
        return home.layout
    elif pathname == '/measures':
        return measures.layout
    elif pathname == '/hospitals':
        return hospitals.layout
    elif pathname == '/budget':
        return budget.layout
    elif pathname == '/contact_us':
        return contact_us.layout
    else:
        return html.Div([
            html.H1("404: Not found", className='text-danger'),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognized.")
        ])

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
