
import base64
import datetime
import io

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.express as px
import dash_table

import pandas as pd

# Mongo DB
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
    html.H1(children='PCA Analysis & Time Series'),
    # Div name
    html.Div(id='display-value'),
    # Upload - Module
    dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Drag and Drop or ',
            html.A('Select Files')
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px'
        },
        # Allow multiple files to be uploaded
        multiple=True
    ),
    html.Div(id='output-data-upload'),    
    # Styling line
    html.Hr(),
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
        value='Total'
    ),
    # Generate Graph
    dcc.Graph(id='indicator-graphic'),
])

# Upload parse component
def parse_contents(contents, filename, date):
    content_type, content_string = contents.split(',')

    decoded = base64.b64decode(content_string)
    try:
        if 'csv' in filename:
            # Assume that the user uploaded a CSV file
            df = pd.read_csv(
                io.StringIO(decoded.decode('utf-8')))
        elif 'xls' in filename:
            # Assume that the user uploaded an excel file
            df = pd.read_excel(io.BytesIO(decoded))
    except Exception as e:
        print(e)
        return html.Div([
            'There was an error processing this file.'
        ])

    return html.Div([
        html.H5(filename),
        html.H6(datetime.datetime.fromtimestamp(date)),

        dash_table.DataTable(
            data=df.to_dict('records'),
            columns=[{'name': i, 'id': i} for i in df.columns]
        ),

        html.Hr(),  # horizontal line

        # For debugging, display the raw contents provided by the web browser
        html.Div('Raw Content'),
        html.Pre(contents[0:200] + '...', style={
            'whiteSpace': 'pre-wrap',
            'wordBreak': 'break-all'
        })
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

@app.callback(Output('output-data-upload', 'children'),
              Input('upload-data', 'contents'),
              State('upload-data', 'filename'),
              State('upload-data', 'last_modified'))

def update_output(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = [
            parse_contents(c, n, d) for c, n, d in
            zip(list_of_contents, list_of_names, list_of_dates)]
        return children
# https://dash.plotly.com/layout


if __name__ == '__main__':
    app.run_server(debug=True)
