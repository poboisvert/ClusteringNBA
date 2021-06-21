
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px

import pandas as pd
from pymongo import MongoClient


# Init Mongo DB
MONGO_DETAILS = "mongodb://localhost:27017"

client = MongoClient(MONGO_DETAILS)

# Database Name
db = client.players

# Collection Name - ML
collection_conn = db['Timeseries_Dataset']
collection_cursor = collection_conn.find()
df = pd.DataFrame(list(collection_cursor))
# print(df)

# Init Dash and styling
app = dash.Dash(__name__,
                external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])
# Markdown
markdown_text = html.Label('Multi-Select Dropdown'),

markdown_text = '''
Please select a cluster
'''


# Dash init layout
colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}
app.layout = html.Div([
    # Generic HTML
    html.H1(children='PCA Analysis - Scatter'),
    # Div name
    html.Div(id='display-value'),
    # Dropdown
    # html.Div(children='''Sentence test'''),
    # Markdown
    dcc.Markdown(children=markdown_text),
    # Markdown

    # html.Label('Dropdown'),
    dcc.Dropdown(
        id='dropdown-select',
        options=[
            {'label': 'Regressions', 'value': 'Regressions'},
            {'label': 'Improvements', 'value': 'Improvements'},
            {'label': 'Total', 'value': 'Total'},
        ],
        value='Timesseries options for analysis'
    ),
    # Generate Graph
    dcc.Graph(id='indicator-graphic'),
])


@app.callback(
    Output('indicator-graphic', 'figure'),
    Input('dropdown-select', 'value'))
# def display_value(value):
#    return 'You have selected "{}"'.format(value)
def update_figure(value):

    fig = px.scatter(x=df[value],
                     y=df['Change'])

    return fig

# https://dash.plotly.com/layout


if __name__ == '__main__':
    app.run_server(debug=True)
