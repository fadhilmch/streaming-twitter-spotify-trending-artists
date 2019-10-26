import dash
from dash.dependencies import Output, Input
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import datetime
import pandas as pd
from cassandraConnect import CassandraConnect


cassandra = CassandraConnect('twitter_keyspace')
minutes = 36000 # number of minutes back to show

app_color = {
    "graph_bg": "rgb(221, 236, 255)",
    "graph_line": "rgb(8, 70, 151)",
    "graph_font":"rgb(2, 29, 65)"
}

chart_colors = [
    '#664DFF',
    '#893BFF',
    '#3CC5E8',
    '#2C93E8',
    '#0BEBDD',
    '#0073FF',
    '#00BDFF',
    '#A5E82C',
    '#FFBD42',
    '#FFCA30'
]

app = dash.Dash(__name__)
app.layout = html.Div(
    [   #html.H2('ID2221 Project'),
        dcc.Graph(
            id='live-graph',
            animate=False,
            figure=go.Figure(
                layout=go.Layout(
                    plot_bgcolor=app_color["graph_bg"],
                    paper_bgcolor=app_color["graph_bg"],
                )
            )
        ),
        dcc.Interval(
            id='graph-update',
            interval=60*1000, # update once every second
            n_intervals=0
        ),
    ]
)

@app.callback(
    Output('live-graph', 'figure'),
    [Input('graph-update', 'n_intervals')])
def update_graph_bar(interval):
    try:
        rows = cassandra.get_data(minutes)

        # Create dataframe
        df = []
        time_now = datetime.datetime.now().replace(microsecond=0).isoformat()
        for row in rows:
            df.append({'date': time_now, 'artist': row.artist, 'total_count': row.total_count})
        df = pd.DataFrame(df)

        # Parse datetime
        df.date = pd.to_datetime(df.date, format='%Y/%m/%d %H:%M:%S', errors='ignore')

        sorted_df = df.sort_values('total_count', ascending=False).head(10)

        # Take x, y from last 10 minutes, update
        X = sorted_df.total_count.values
        Y = sorted_df.artist.values
        print(X)
        print(Y)
        # Define bars
        data = go.Bar(
            x=X,
            y=Y,
            name='Top 10 Artist',
            orientation='h',
            marker=dict(color=chart_colors[::-1]),
            #opacity=0.6
        )

        layout = go.Layout(

            title='Twitter mentions of Spotify',
            xaxis=dict(
                tickfont=dict(
                    size=12,
                    color='rgb(107, 107, 107)'
                ),
                autorange= True,
                title= 'Number of tweets'
            ),
            plot_bgcolor=app_color["graph_bg"],
            paper_bgcolor=app_color["graph_bg"],
            font={"color": app_color["graph_font"]},
            autosize=True,
            margin=go.layout.Margin(
                l=100,
                r=25,
                b=75,
                t=25,
                pad=4
            ),
        )

        return {'data': [data], "layout": layout}

    except Exception as e:
        with open('errors.txt','a') as f:
            f.write(str(e))
            f.write('\n')


if __name__ == '__main__':
    app.run_server(debug=True)
