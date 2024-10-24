# pages/home.py
from dash import html, dcc, callback
import dash_bootstrap_components as dbc
from db_utils import fetch_data
import dash
from components.navbar import navbar
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd

# Define layout for homepage
layout = html.Div([
    navbar,
    html.Div([
        html.H1("Welcome to Australian Healthcare Resource Allocation App", className='text-center'),
        html.Br(),
        html.Div([
            html.Img(src='/assets/logo.png', style={'width': '50%'}),
        ], style={'textAlign': 'center'}),
        html.Br(),
        html.P("""
            This application is designed to facilitate healthcare resource allocation through data-driven insights. 
            Navigate through the various sections using the menu to explore different metrics and tools available to you.
        """),
        html.H3("Overview"),
        html.P("""
            Efficient allocation of healthcare resources is critical to improving patient outcomes and optimizing costs. 
            This application provides a comprehensive suite of tools for analyzing healthcare data, forecasting trends, 
            and making informed decisions about resource distribution.
        """),
        html.H3("Getting Started"),
        html.Ul([
            html.Li("Use the menu to select a section (e.g., Measures, Hospitals, etc.)."),
            html.Li("Choose the relevant options and filters to explore the data."),
            html.Li("View visualizations and insights to assist in decision-making."),
        ]),
        html.H3("Data Ingestion Progress"),
        html.Div(id='data-ingestion-progress'),
        
        # Interval component for periodic updates
        dcc.Interval(
            id='interval-component',   # ID of the interval component
            interval=1*1000,          # Update every second
            n_intervals=0              # Start at zero intervals
        )
    ], className='container')
])

# Callback to update data ingestion progress periodically
@dash.callback(
    Output('data-ingestion-progress', 'children'),
    [Input('interval-component', 'n_intervals')]  # Triggered by the interval component
)
def update_progress(n_intervals):
    # Query to get total datasets and stored datasets count, as well as record counts
    progress_query = """
    SELECT 
        COUNT(*) AS total_datasets, 
        SUM(CASE WHEN stored = TRUE THEN 1 ELSE 0 END) AS stored_datasets,
        SUM(totalrecords) AS total_records, 
        SUM(processedrecords) AS stored_records
    FROM datasets;
    """
    datasets_df = fetch_data(progress_query)

    if datasets_df.empty:
        return html.P("No data available.")
    else:
        total_datasets = datasets_df.iloc[0]['total_datasets']
        stored_datasets = datasets_df.iloc[0]['stored_datasets']
        total_records = datasets_df.iloc[0]['total_records']
        stored_records = datasets_df.iloc[0]['stored_records']

        # Calculate percentage of stored datasets
        if total_datasets == 0:
            percentage_stored_datasets = 0
        else:
            percentage_stored_datasets = (stored_datasets / total_datasets) * 100

        # Progress bar for dataset storage progress
        datasets_progress_bar = dbc.Progress(
            value=min(int(percentage_stored_datasets), 100),
            label=f"{percentage_stored_datasets:.2f}%",
            striped=True,
            animated=True,
            color="success" if percentage_stored_datasets == 100 else "info"
        )

        # Display dataset progress bar and records processed count
        return html.Div([
            html.H4("Overall Dataset Storage Progress"),
            html.P(f"Stored Datasets: {stored_datasets} / {total_datasets}"),
            datasets_progress_bar,
            html.Hr(),
            html.H4("Processed Records Information"),
            html.P(f"Processed Records: {stored_records} ")
        ])