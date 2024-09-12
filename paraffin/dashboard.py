import logging

import dash
import plotly.graph_objs as go
from dash import dcc, html
from dash.dependencies import Input, Output

from paraffin.cli import finished, graph, positions, submitted

log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

# Initialize the Dash app
app = dash.Dash(__name__)

# Layout of the Dash app
app.layout = html.Div(
    [
        html.H1("DVC Pipeline Dashboard"),
        dcc.Graph(id="live-update-graph"),
        dcc.Interval(
            id="interval-component",
            interval=1 * 1000,  # Update every 1 second
            n_intervals=0,
        ),
    ]
)


# Callback to update graph figure
@app.callback(
    Output("live-update-graph", "figure"), Input("interval-component", "n_intervals")
)
def update_graph(n):
    if graph is not None:
        # Get node positions using spring layout

        # Extract edge trace for Plotly
        edge_trace = []
        for edge in graph.edges():
            x0, y0 = positions[edge[0]]
            x1, y1 = positions[edge[1]]
            edge_trace.append(
                go.Scatter(
                    x=[x0, x1, None],
                    y=[y0, y1, None],
                    mode="lines",
                    line={"width": 1, "color": "gray"},
                    hoverinfo="none",
                )
            )

        # Extract node trace for Plotly
        node_trace = []
        for node in graph.nodes():
            x, y = positions[node]
            color = "blue"
            if node.addressing in finished:
                color = "green"
            elif node.addressing in submitted:
                color = "yellow"
            node_trace.append(
                go.Scatter(
                    x=[x],
                    y=[y],
                    text=[node],
                    mode="markers+text",
                    textposition="top center",
                    marker={"size": 10, "color": color},
                    hoverinfo="text",
                )
            )

        # Create the figure
        figure = {
            "data": edge_trace + node_trace,
            "layout": go.Layout(
                showlegend=False,
                margin={"b": 0, "l": 0, "r": 0, "t": 0},
                xaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
                yaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
                height=600,
            ),
        }
        return figure

    # Return an empty figure if the graph is None
    return {"data": [], "layout": go.Layout()}


if __name__ == "__main__":
    app.run_server(debug=True)
