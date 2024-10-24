# components/navbar.py
from dash import html, dcc
import dash_bootstrap_components as dbc

navbar = dbc.NavbarSimple(
    brand="Australian Healthcare Resource Allocation App",
    brand_href="/",
    color="primary",
    dark=True,
    children=[
        dbc.NavItem(dcc.Link("Home", href="/", className="nav-link")),
        dbc.NavItem(dcc.Link("Measures", href="/measures", className="nav-link")),
        dbc.NavItem(dcc.Link("Hospitals", href="/hospitals", className="nav-link")),
        dbc.NavItem(dcc.Link("Budget", href="/budget", className="nav-link")),
        dbc.NavItem(dcc.Link("Contact Us", href="/contact_us", className="nav-link")),
    ],
)
