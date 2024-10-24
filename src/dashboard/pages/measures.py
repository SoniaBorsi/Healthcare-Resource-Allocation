# pages/measures.py
from dash import dcc, html, Input, Output, State, callback
import dash
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objs as go
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from db_utils import fetch_data
from components.navbar import navbar

# Layout of the Measures page
layout = html.Div([
    navbar,
    html.Div([
        html.H1("Measures", className='text-center'),
        html.Hr(),
        html.P("""
            This section of the dashboard allows you to explore detailed metrics for various healthcare measures across different states in Australia.
        """),
        dbc.Row([
            dbc.Col([
                html.Label("Select Measure"),
                dcc.Dropdown(id='measure-dropdown', options=[], placeholder='Select a Measure')
                                # style={
                                #     'width': '100%',
                                #     'fontSize': '12px',  # Adjust the font size
                                #     'lineHeight': '17px'  # Adjust line height if necessary
                                # }),
            ], md=4),
            dbc.Col([
                html.Label("Select Reported Measure"),
                dcc.Dropdown(id='reported-measure-dropdown', options=[], placeholder='Select a Reported Measure'),
            ], md=4),
            dbc.Col([
                html.Label("Select State"),
                dcc.Dropdown(id='state-dropdown', options=[], placeholder='Select a State'),
            ], md=4),
        ]),
        html.Br(),
        html.Div(id='measure-content'),
    ], className='container')
])

# Callback to populate the Measure dropdown
@dash.callback(
    Output('measure-dropdown', 'options'),
    Input('url', 'pathname')
)
def update_measure_dropdown(_):
    sql_query_codes = '''
    SELECT DISTINCT m.measurename
    FROM datasets ds
    LEFT JOIN measurements m ON ds.measurecode = m.measurecode
    WHERE ds.stored = TRUE;
    '''
    df_measures = fetch_data(sql_query_codes)
    options = [{'label': name, 'value': name} for name in np.sort(df_measures['measurename'].unique())]
    return options

# Callback to populate the Reported Measure dropdown based on selected Measure
@dash.callback(
    Output('reported-measure-dropdown', 'options'),
    Input('measure-dropdown', 'value')
)
def update_reported_measure_dropdown(selected_measure):
    if selected_measure:
        sql_query = '''
        SELECT DISTINCT rm.reportedmeasurename
        FROM datasets ds
        LEFT JOIN measurements m ON ds.measurecode = m.measurecode
        LEFT JOIN reported_measurements rm ON ds.reportedmeasurecode = rm.reportedmeasurecode
        WHERE ds.stored = TRUE AND m.measurename = %s;
        '''
        df_reported_measures = fetch_data(sql_query, params=(selected_measure,))
        options = [{'label': name, 'value': name} for name in np.sort(df_reported_measures['reportedmeasurename'].unique())]
        return options
    else:
        return []

# Callback to populate the State dropdown
@dash.callback(
    Output('state-dropdown', 'options'),
    Input('reported-measure-dropdown', 'value')
)
def update_state_dropdown(_):
    sql_query_states = '''
    SELECT DISTINCT state
    FROM hospitals
    WHERE open_closed = 'Open';
    '''
    df_states = fetch_data(sql_query_states)
    options = [{'label': state, 'value': state} for state in np.sort(df_states['state'].unique())]
    return options

# Main callback to update the content based on selected options
@dash.callback(
    Output('measure-content', 'children'),
    [
        Input('measure-dropdown', 'value'),
        Input('reported-measure-dropdown', 'value'),
        Input('state-dropdown', 'value')
    ]
)
def update_measure_content(selected_measure, selected_reported_measure, selected_state):
    if not all([selected_measure, selected_reported_measure, selected_state]):
        return html.Div([
            html.P("Please select a Measure, Reported Measure, and State to see the data.")
        ])

    # Fetch the data
    safe_measure = selected_measure.replace("'", "''")  # rudimentary SQL injection protection
    safe_reported_measure = selected_reported_measure.replace("'", "''")
    safe_state = selected_state.replace("'", "''")

    sql_query_state = f'''
    SELECT
        info.value,
        info.caveats,
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
        m.measurename = %s AND
        rm.reportedmeasurename = %s AND
        ds.stored = TRUE AND
        h.state = %s
    ORDER BY
        ds.reportingstartdate ASC;
    '''

    df_value = fetch_data(sql_query_state, params=(safe_measure, safe_reported_measure, safe_state))

    if df_value.empty:
        # If no data for the selected state, fetch national data
        sql_query_national = f'''
        SELECT
            info.value,
            info.caveats,
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
            m.measurename = %s AND
            rm.reportedmeasurename = %s AND
            ds.stored = TRUE AND
            info.reportingunitcode = 'NAT'
        ORDER BY
            ds.reportingstartdate ASC;
        '''
        df_value = fetch_data(sql_query_national, params=(safe_measure, safe_reported_measure))
        data_message = f"No data found for the selected state ({selected_state}). Displaying national data."
    else:
        data_message = None

    # Filter out NaN values
    df_value['value'] = df_value.apply(
    lambda row: row['caveats'] if pd.isna(row['value']) else row['value'], axis=1)

    # Ensure the caveats column is available, otherwise provide a default empty string
    df_value['caveats'] = df_value['caveats'].fillna('No caveat provided')

    if not df_value.empty:
        # Aggregate data by reporting date
        df_value['reportingstartdate'] = pd.to_datetime(df_value['reportingstartdate'])
        df_value_aggregated = df_value.groupby('reportingstartdate').agg({'value': 'mean'}).reset_index()

        content = []

        if data_message:
            content.append(html.P(data_message, style={'color': 'red'}))

        # Time Series Plot
        content.append(html.H3("Time Series of Selected Measure"))
        content.append(html.P("This plot shows the time series of the selected measure over time, along with the national average for comparison."))

        fig = px.line(df_value_aggregated, x='reportingstartdate', y='value', title=f'{selected_measure} - {selected_reported_measure} Over Time')
        # Fetch and plot national average
        sql_query_national_avg = '''
        SELECT
            info.value,
            ds.reportingstartdate
        FROM
            datasets ds
        JOIN
            measurements m ON ds.measurecode = m.measurecode
        JOIN
            reported_measurements rm ON ds.reportedmeasurecode = rm.reportedmeasurecode
        JOIN
            info ON ds.datasetid = info.datasetid
        WHERE
            m.measurename = %s AND
            rm.reportedmeasurename = %s AND
            ds.stored = TRUE AND
            info.reportingunitcode = 'NAT'
        ORDER BY
            ds.reportingstartdate ASC;
        '''
        df_national_avg = fetch_data(sql_query_national_avg, params=(safe_measure, safe_reported_measure))
        if not df_national_avg.empty:
            df_national_avg['reportingstartdate'] = pd.to_datetime(df_national_avg['reportingstartdate'])
            df_national_avg = df_national_avg.groupby('reportingstartdate').agg({'value': 'mean'}).reset_index()
            fig.add_trace(go.Scatter(x=df_national_avg['reportingstartdate'], y=df_national_avg['value'], mode='lines', name='National Average'))
        content.append(dcc.Graph(figure=fig))

        # Map of Hospitals
        content.append(html.H3("Hospital Locations"))
        fig_map = px.scatter_mapbox(
            df_value,
            lat='latitude',
            lon='longitude',
            hover_name='hospital_name',
            hover_data={'latitude': False, 'longitude': False, 'value': True},
            title=f'Hospitals in {selected_state} Reporting {selected_measure}',
            mapbox_style="open-street-map",
            zoom=5,
            color_discrete_sequence=["darkblue"]
        )
        fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=600)
        content.append(dcc.Graph(figure=fig_map))

        # Plot Selection
        content.append(html.H3("Choose Plots to Display"))
        plot_options = [
            {'label': 'Time Series Decomposition', 'value': 'ts_decomposition'},
            {'label': 'Forecasting', 'value': 'forecasting'},
            {'label': 'Distribution of Values', 'value': 'distribution'},
            {'label': 'Value Distribution by Hospital', 'value': 'boxplot'},
            {'label': 'Heatmap of Values Over Time', 'value': 'heatmap'},
            {'label': 'Ranking of Hospitals', 'value': 'ranking'},
            {'label': 'Correlation Analysis', 'value': 'correlation'}
        ]
        content.append(
            dcc.Checklist(
                id='plot-options-checklist',
                options=plot_options,
                value=[],
                labelStyle={'display': 'inline-block', 'margin-right': '10px'}
            )
        )
        content.append(html.Div(id='additional-plots'))
        return content
    else:
        return html.Div([
            html.P("No data found for the selected measure and reported measure.")
        ])

# Callback to update additional plots based on selected options
@dash.callback(
    Output('additional-plots', 'children'),
    [
        Input('plot-options-checklist', 'value'),
        State('measure-dropdown', 'value'),
        State('reported-measure-dropdown', 'value'),
        State('state-dropdown', 'value')
    ]
)
def update_additional_plots(selected_plots, selected_measure, selected_reported_measure, selected_state):
    content = []
    if not all([selected_measure, selected_reported_measure, selected_state]):
        return content

    safe_measure = selected_measure.replace("'", "''")
    safe_reported_measure = selected_reported_measure.replace("'", "''")
    safe_state = selected_state.replace("'", "''")

    # Fetch data again (could optimize by caching)
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
        m.measurename = %s AND
        rm.reportedmeasurename = %s AND
        ds.stored = TRUE AND
        h.state = %s
    ORDER BY
        ds.reportingstartdate ASC;
    '''
    df_value = fetch_data(sql_query_state, params=(safe_measure, safe_reported_measure, safe_state))
    df_value = df_value.dropna(subset=['value'])
    if df_value.empty:
        return content

    df_value['reportingstartdate'] = pd.to_datetime(df_value['reportingstartdate'])
    df_value_aggregated = df_value.groupby('reportingstartdate').agg({'value': 'mean'}).reset_index()

    # Time Series Decomposition
    if 'ts_decomposition' in selected_plots:
        content.append(html.H3("Time Series Decomposition"))
        content.append(html.P("This section decomposes the time series into trend, seasonal, and residual components to analyze the underlying patterns in the data."))
        if len(df_value_aggregated) >= 24:
            decomposition = seasonal_decompose(df_value_aggregated.set_index('reportingstartdate')['value'], model='additive', period=12)
            fig_trend = px.line(decomposition.trend.dropna(), title='Trend Component')
            fig_seasonal = px.line(decomposition.seasonal.dropna(), title='Seasonal Component')
            fig_residual = px.line(decomposition.resid.dropna(), title='Residual Component')
            content.append(dcc.Graph(figure=fig_trend))
            content.append(dcc.Graph(figure=fig_seasonal))
            content.append(dcc.Graph(figure=fig_residual))
        else:
            content.append(html.P("Not enough data for time series decomposition. At least 24 observations are required."))

    # Forecasting
    if 'forecasting' in selected_plots:
        content.append(html.H3("Forecasting"))
        content.append(html.P("This section provides a forecast of the selected measure for the upcoming months based on historical data."))
        if len(df_value_aggregated) >= 12:
            forecast_horizon = 12  # You can make this adjustable if desired
            model = ExponentialSmoothing(df_value_aggregated['value'], trend='add', seasonal=None).fit()
            forecast = model.forecast(forecast_horizon)
            forecast_index = pd.date_range(df_value_aggregated['reportingstartdate'].iloc[-1], periods=forecast_horizon+1, freq='M')[1:]
            fig_forecast = px.line(df_value_aggregated, x='reportingstartdate', y='value', title='Forecasting')
            fig_forecast.add_trace(go.Scatter(x=forecast_index, y=forecast, mode='lines', name='Forecast'))
            content.append(dcc.Graph(figure=fig_forecast))
        else:
            content.append(html.P("Not enough data for forecasting. At least 12 observations are required."))

    # Distribution of Values
    if 'distribution' in selected_plots:
        content.append(html.H3("Distribution of Values"))
        content.append(html.P("This histogram shows the distribution of values for the selected measure across hospitals in the selected state."))
        fig_hist = px.histogram(df_value, x='value', nbins=20, title=f'Distribution of {selected_measure} - {selected_reported_measure}')
        content.append(dcc.Graph(figure=fig_hist))

    # Value Distribution by Hospital
    if 'boxplot' in selected_plots:
        content.append(html.H3("Value Distribution by Hospital"))
        content.append(html.P("This box plot shows the distribution of the selected measure across different hospitals in the selected state."))
        fig_box = px.box(df_value, x='hospital_name', y='value', title=f'{selected_measure} - {selected_reported_measure} Distribution by Hospital')
        content.append(dcc.Graph(figure=fig_box))

    # Heatmap of Values Over Time
    if 'heatmap' in selected_plots:
        content.append(html.H3("Heatmap of Values Over Time"))
        content.append(html.P("This heatmap shows the variation of the selected measure across different hospitals over time."))
        df_heatmap = df_value.pivot_table(values='value', index='hospital_name', columns='reportingstartdate', aggfunc='mean')
        fig_heatmap = go.Figure(data=go.Heatmap(
            z=df_heatmap.values,
            x=df_heatmap.columns,
            y=df_heatmap.index,
            colorscale='Viridis'
        ))
        fig_heatmap.update_layout(title=f'Heatmap of {selected_measure} - {selected_reported_measure} Over Time by Hospital', xaxis_nticks=36)
        content.append(dcc.Graph(figure=fig_heatmap))

    # Ranking of Hospitals
    if 'ranking' in selected_plots:
        content.append(html.H3("Ranking of Hospitals"))
        content.append(html.P("This bar chart ranks hospitals based on the average value of the selected measure."))
        df_ranked = df_value.groupby('hospital_name').agg({'value': 'mean'}).reset_index().sort_values(by='value', ascending=False)
        fig_bar = px.bar(df_ranked, x='hospital_name', y='value', title=f'Ranking of Hospitals by {selected_measure} - {selected_reported_measure}')
        content.append(dcc.Graph(figure=fig_bar))

    # Correlation Analysis
    if 'correlation' in selected_plots:
        content.append(html.H3("Correlation Analysis"))
        content.append(html.P("This scatter plot shows the correlation between the selected measure and another measure of your choice."))
        # Fetch list of measures for correlation
        sql_query_measures = '''
        SELECT DISTINCT measurename
        FROM measurements;
        '''
        df_measures = fetch_data(sql_query_measures)
        measure_options = [{'label': name, 'value': name} for name in np.sort(df_measures['measurename'].unique())]
        content.append(html.Label("Select Another Metric for Correlation"))
        content.append(
            dcc.Dropdown(
                id='correlation-metric-dropdown',
                options=measure_options,
                placeholder='Select a Measure for Correlation'
            )
        )
        content.append(html.Div(id='correlation-plot'))

    return content

# Callback for Correlation Analysis
@dash.callback(
    Output('correlation-plot', 'children'),
    [
        Input('correlation-metric-dropdown', 'value'),
        State('measure-dropdown', 'value'),
        State('reported-measure-dropdown', 'value'),
        State('state-dropdown', 'value')
    ]
)
def update_correlation_plot(another_metric, selected_measure, selected_reported_measure, selected_state):
    if another_metric:
        safe_measure = selected_measure.replace("'", "''")
        safe_another_metric = another_metric.replace("'", "''")
        safe_state = selected_state.replace("'", "''")

        sql_query_another_metric = f'''
        SELECT
            info.value,
            ds.reportingstartdate,
            info.reportingunitcode,
            h.name as hospital_name
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
            m.measurename = %s AND
            ds.stored = TRUE AND
            h.state = %s
        ORDER BY
            ds.reportingstartdate ASC;
        '''
        df_another_metric = fetch_data(sql_query_another_metric, params=(safe_another_metric, safe_state))
        if not df_another_metric.empty:
            df_another_metric['reportingstartdate'] = pd.to_datetime(df_another_metric['reportingstartdate'])
            df_another_metric_agg = df_another_metric.groupby('reportingstartdate').agg({'value': 'mean'}).reset_index()

            # Fetch data for selected measure
            sql_query_selected_metric = f'''
            SELECT
                info.value,
                ds.reportingstartdate
            FROM
                datasets ds
            JOIN
                measurements m ON ds.measurecode = m.measurecode
            JOIN
                reported_measurements rm ON ds.reportedmeasurecode = rm.reportedmeasurecode
            JOIN
                info ON ds.datasetid = info.datasetid
            WHERE
                m.measurename = %s AND
                ds.stored = TRUE AND
                h.state = %s
            ORDER BY
                ds.reportingstartdate ASC;
            '''
            df_selected_metric = fetch_data(sql_query_selected_metric, params=(safe_measure, safe_state))
            if not df_selected_metric.empty:
                df_selected_metric['reportingstartdate'] = pd.to_datetime(df_selected_metric['reportingstartdate'])
                df_selected_metric_agg = df_selected_metric.groupby('reportingstartdate').agg({'value': 'mean'}).reset_index()

                df_correlation = pd.merge(df_selected_metric_agg, df_another_metric_agg, on='reportingstartdate', suffixes=(f'_{selected_measure}', f'_{another_metric}'))
                fig_scatter = px.scatter(
                    df_correlation,
                    x=f'value_{selected_measure}',
                    y=f'value_{another_metric}',
                    title=f'Correlation Between {selected_measure} and {another_metric}'
                )
                return dcc.Graph(figure=fig_scatter)
            else:
                return html.P("No data available for the selected measure.")
        else:
            return html.P("No data available for the selected metric for correlation.")
    else:
        return html.P("Please select another metric for correlation.")

