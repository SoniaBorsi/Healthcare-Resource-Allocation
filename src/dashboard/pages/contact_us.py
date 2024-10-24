# pages/contact_us.py
from dash import html
from components.navbar import navbar

layout = html.Div([
    navbar,
    html.Div([
        html.H1("Contact Us", className='text-center'),
        html.Hr(),
        html.P("""
        We are here to assist you with any questions, concerns, or feedback you may have. Please feel free to reach out to us via email.
        """),
        html.H3("Sonia Borsi"),
        html.P([
            html.A("Email", href="mailto:sonia.borsi@studenti.unitn.it"),
            " | ",
            html.A("LinkedIn", href="https://www.linkedin.com/in/sonia-borsi-824998260/")
        ]),
        html.H3("Filippo Costamagna"),
        html.P([
            html.A("Email", href="mailto:filippo.costamagna@studenti.unitn.it"),
            " | ",
            html.A("LinkedIn", href="https://www.linkedin.com/in/filippo-costamagna-a439b3303/")
        ]),
        html.P("""
        We look forward to hearing from you and will respond as soon as possible.
        """),
        html.H4("Learn more"),
        html.A(html.Button("Visit our repository", className='btn btn-success'),
               href="https://github.com/SoniaBorsi/Healthcare-Resource-Allocation.git", target="_blank")
    ], className='container')
])
